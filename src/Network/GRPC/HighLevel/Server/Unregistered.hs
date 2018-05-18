{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DuplicateRecordFields #-}

module Network.GRPC.HighLevel.Server.Unregistered where

import           Control.Arrow (left)
import           Control.Concurrent.Async                  (async, wait, concurrently_, Async)
import Control.Exception.Safe hiding (Handler)
import           Control.Monad
import           Data.Foldable                             (find)
import           Network.GRPC.HighLevel.Server
import           Network.GRPC.LowLevel
import           Network.GRPC.LowLevel.GRPC (grpcDebug)
import qualified Network.GRPC.LowLevel.Call.Unregistered   as U
import qualified Network.GRPC.LowLevel.Server.Unregistered as U
import Network.GRPC.LowLevel.Server
import           Proto3.Suite.Class
import qualified Network.GRPC.Unsafe                            as C
import qualified Network.GRPC.Unsafe.Metadata                   as C
import qualified Network.GRPC.Unsafe.Time                       as C
import           Foreign.Storable                               (peek)
import           Network.GRPC.LowLevel.CompletionQueue.Internal (newTag, CompletionQueue(..), next)
import           Network.GRPC.LowLevel.Op (runOpsAsync, resultFromOpContext, destroyOpArrayAndContexts, readAndDestroy)
import Data.Maybe (catMaybes)
import qualified Foreign.Marshal.Alloc as C

data CallStateException = ImpossiblePayload String | NotFound C.Event | GRPCException GRPCIOError | UnknownHandler MethodName
  deriving (Show, Typeable)

-- If an exception happens in a handler, don't kill the server.
-- Cleaning up resources in exceptional cases
-- Shutting down the server by cancelling in progress async requests
-- Shutting down the server at all

instance Exception CallStateException

runCallState :: AsyncServer -> CallState -> [Handler 'Normal] -> IO (Async ())
runCallState server callState allHandlers = case callState of
  Listen -> do
    tag' <- newTag (serverCallQueue server)
    grpcDebug ("Creating tag" ++ show tag')
    callPtr <- C.malloc
    metadataPtr <- C.metadataArrayCreate
    metadata  <- peek metadataPtr
    callDetails <- C.createCallDetails
    callError <- C.grpcServerRequestCall (unsafeServer (server :: AsyncServer)) callPtr callDetails metadata (unsafeCQ . serverOpsQueue $ server) (unsafeCQ . serverCallQueue $ server) tag'

    let cleanup = do
          C.metadataArrayDestroy metadataPtr
          C.destroyCallDetails callDetails
          C.free callPtr
        go = case callError of
               C.CallOk -> do
                 let state = StartRequest callPtr callDetails metadata tag' cleanup
                 insertCall server tag' state
               other -> throw (GRPCIOCallError other)

    async (go `onException` cleanup)
  (StartRequest callPtr callDetails metadata tag cleanup) -> do
    _ <- wait <$> runCallState server Listen allHandlers

    serverCall <- U.ServerCall
      <$> peek callPtr
      <*> pure (serverOpsQueue server)
      <*> C.getAllMetadataArray metadata
      <*> (C.timeSpec <$> C.callDetailsGetDeadline callDetails)
      <*> (MethodName <$> C.callDetailsGetMethod   callDetails)
      <*> (Host       <$> C.callDetailsGetHost     callDetails)

    let
      cleanup' = cleanup >> U.destroyServerCall serverCall
      operations = [ OpSendInitialMetadata (U.metadata serverCall), OpRecvMessage ]
      sendOps = runOpsAsync (U.unsafeSC serverCall) (U.callCQ serverCall) tag operations $ \(array, contexts) -> do
        let state = ReceivePayload serverCall tag array contexts cleanup'
        replaceCall server tag state `onException` destroyOpArrayAndContexts array contexts
    async (sendOps `onException` cleanup')

  (ReceivePayload serverCall tag array contexts cleanup) -> do
    payload <- readAndDestroy array contexts
    grpcDebug "ReceivePayload: Received MessageResult"
    normalHandler <- maybe (throw $ UnknownHandler (U.callMethod serverCall)) pure (findHandler serverCall allHandlers)
      `onException` cleanup
    async (callHandler normalHandler payload `onException` cleanup)
    where
      findHandler sc = find ((== U.callMethod sc) . handlerMethodName)
      f normalHandler string =
        case normalHandler of
          (UnaryHandler _ handler) -> convertServerHandler handler (const string <$> U.convertCall serverCall)
      callHandler normalHandler = \case
        [OpRecvMessageResult (Just body)] -> do
          grpcDebug ("ReceivePayload: Received payload:" ++ show body)

          (rsp, trailMeta, st, ds) <- f normalHandler body

          let operations = [ OpRecvCloseOnServer , OpSendMessage rsp, OpSendStatusFromServer trailMeta st ds ]
          runOpsAsync (U.unsafeSC serverCall) (U.callCQ serverCall) tag operations $ \(array', contexts') -> do
            let state = AcknowledgeResponse tag array' contexts' cleanup
            replaceCall server tag state `onException` destroyOpArrayAndContexts array' contexts'
        rest -> throw (ImpossiblePayload $ "ReceivePayload: Impossible payload result: " ++ show rest)
  (AcknowledgeResponse tag array contexts cleanup) -> do
    destroyOpArrayAndContexts array contexts -- Safe to teardown after calling 'resultFromOpContext'.
    deleteCall server tag `finally` cleanup
    async (pure ())

asyncDispatchLoop :: AsyncServer
             -> (String -> IO ())
             -> MetadataMap
             -> [Handler 'Normal]
             -> [Handler 'ClientStreaming]
             -> [Handler 'ServerStreaming]
             -> [Handler 'BiDiStreaming]
             -> IO ()
asyncDispatchLoop s logger md hN _ _ _ = do
  wait <$> runCallState s Listen hN
  loop (serverCallQueue s) (Just 1) `concurrently_` loop (serverOpsQueue s) Nothing
  where
    loop queue timeout = forever $ do
      eitherEvent <- next queue timeout
      case eitherEvent of
        Right event -> do
          clientCallData <- lookupCall s (C.eventTag event)
          case clientCallData of
            Just callData -> runCallState s callData hN
            Nothing -> async (throw $ NotFound event)
        Left err -> async (throw err)

asyncServerLoop :: ServerOptions -> IO ()
asyncServerLoop ServerOptions{..} = do
  -- We run the loop in a new thread so that we can kill the serverLoop thread.
  -- Without this fork, we block on a foreign call, which can't be interrupted.
  tid <- async $ withGRPC $ \grpc ->
    withAsyncServer grpc config $ \server -> do
      asyncDispatchLoop server
                   optLogger
                   optInitialMetadata
                   optNormalHandlers
                   optClientStreamHandlers
                   optServerStreamHandlers
                   optBiDiStreamHandlers
  wait tid
  where
    config = ServerConfig
      { host                             = optServerHost
      , port                             = optServerPort
      , methodsToRegisterNormal          = []
      , methodsToRegisterClientStreaming = []
      , methodsToRegisterServerStreaming = []
      , methodsToRegisterBiDiStreaming   = []
      , serverArgs                       =
          [CompressionAlgArg GrpcCompressDeflate | optUseCompression]
          ++
          [ UserAgentPrefix optUserAgentPrefix
          , UserAgentSuffix optUserAgentSuffix
          ]
      , sslConfig = optSSLConfig
      }

dispatchLoop :: Server
             -> (String -> IO ())
             -> MetadataMap
             -> [Handler 'Normal]
             -> [Handler 'ClientStreaming]
             -> [Handler 'ServerStreaming]
             -> [Handler 'BiDiStreaming]
             -> IO ()
dispatchLoop s logger md hN hC hS hB =
  forever $ U.withServerCallAsync s $ \sc ->
    case findHandler sc allHandlers of
      Just (AnyHandler ah) -> case ah of
        UnaryHandler _ h        -> unaryHandler sc h
        ClientStreamHandler _ h -> csHandler sc h
        ServerStreamHandler _ h -> ssHandler sc h
        BiDiStreamHandler _ h   -> bdHandler sc h
      Nothing                   -> unknownHandler sc
  where
    allHandlers = map AnyHandler hN ++ map AnyHandler hC
                  ++ map AnyHandler hS ++ map AnyHandler hB

    findHandler sc = find ((== U.callMethod sc) . anyHandlerMethodName)

    unaryHandler :: (Message a, Message b) => U.ServerCall -> ServerHandler a b -> IO ()
    unaryHandler sc h =
      handleError $
        U.serverHandleNormalCall' s sc md $ \_sc' bs ->
          convertServerHandler h (const bs <$> U.convertCall sc)
    csHandler :: (Message a, Message b) => U.ServerCall -> ServerReaderHandler a b -> IO ()
    csHandler sc = handleError . U.serverReader s sc md . convertServerReaderHandler

    ssHandler :: (Message a, Message b) => U.ServerCall -> ServerWriterHandler a b -> IO ()
    ssHandler sc = handleError . U.serverWriter s sc md . convertServerWriterHandler

    bdHandler :: (Message a, Message b) => U.ServerCall -> ServerRWHandler a b -> IO ()
    bdHandler sc = handleError . U.serverRW s sc md . convertServerRWHandler

    unknownHandler :: U.ServerCall -> IO ()
    unknownHandler sc = void $ U.serverHandleNormalCall' s sc md $ \_ _ ->
      return (mempty, mempty, StatusNotFound, StatusDetails "unknown method")

    handleError :: IO a -> IO ()
    handleError = (handleCallError logger . left herr =<<) . try
      where herr (e :: SomeException) = GRPCIOHandlerException (show e)

serverLoop :: ServerOptions -> IO ()
serverLoop ServerOptions{..} = do
  -- We run the loop in a new thread so that we can kill the serverLoop thread.
  -- Without this fork, we block on a foreign call, which can't be interrupted.
  tid <- async $ withGRPC $ \grpc ->
    withServer grpc config $ \server -> do
      dispatchLoop server
                   optLogger
                   optInitialMetadata
                   optNormalHandlers
                   optClientStreamHandlers
                   optServerStreamHandlers
                   optBiDiStreamHandlers
  wait tid
  where
    config = ServerConfig
      { host                             = optServerHost
      , port                             = optServerPort
      , methodsToRegisterNormal          = []
      , methodsToRegisterClientStreaming = []
      , methodsToRegisterServerStreaming = []
      , methodsToRegisterBiDiStreaming   = []
      , serverArgs                       =
          [CompressionAlgArg GrpcCompressDeflate | optUseCompression]
          ++
          [ UserAgentPrefix optUserAgentPrefix
          , UserAgentSuffix optUserAgentSuffix
          ]
      , sslConfig = optSSLConfig
      }
