{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DuplicateRecordFields #-}

module Network.GRPC.HighLevel.Server.Unregistered where

import           Control.Arrow
import           Control.Concurrent.Async                  (async, wait, concurrently_, Async)
import qualified Control.Exception                         as CE
import           Control.Monad
import           Data.Foldable                             (find)
import           Network.GRPC.HighLevel.Server
import           Network.GRPC.LowLevel
import           Network.GRPC.LowLevel.GRPC (grpcDebug)
import Network.GRPC.LowLevel.Call   (mgdPtr)
import qualified Network.GRPC.LowLevel.Call.Unregistered   as U
import qualified Network.GRPC.LowLevel.Server.Unregistered as U
import Network.GRPC.LowLevel.Server
import           Proto3.Suite.Class
import qualified Network.GRPC.Unsafe                            as C
import qualified Network.GRPC.Unsafe.Metadata                   as C
import qualified Network.GRPC.Unsafe.Time                       as C
import           Control.Monad.Managed
import           Foreign.Storable                               (peek)
import           Network.GRPC.LowLevel.CompletionQueue.Internal (newTag, CompletionQueue(..), next)
import           Network.GRPC.LowLevel.Op (OpContext, runOpsAsync, resultFromOpContext, teardownOpArrayAndContexts)
import qualified Network.GRPC.Unsafe.Op                         as C
import Data.Maybe (catMaybes)
import Data.ByteString (ByteString)
import Foreign.Ptr (nullPtr)
import qualified Foreign.Marshal.Alloc as C

runCallState :: AsyncServer -> CallState -> Maybe C.Event -> [Handler 'Normal] -> IO (Async (Either GRPCIOError ()))
runCallState server callState event allHandlers = case callState of
  Listen -> do
    tag' <- newTag (serverCallQueue server)
    grpcDebug ("Creating tag" ++ show tag')
    callPtr <- C.malloc
    metadataPtr <- C.metadataArrayCreate
    metadata  <- peek metadataPtr
    callDetails <- C.createCallDetails
    callError <- C.grpcServerRequestCall (unsafeServer (server :: AsyncServer)) callPtr callDetails metadata (unsafeCQ . serverOpsQueue $ server) (unsafeCQ . serverCallQueue $ server) tag'

    case callError of
      C.CallOk -> do
        let state = StartRequest (callPtr, metadataPtr, callDetails) metadata tag'
        insertCall server tag' state
        async (pure (Right ()))
      other -> async (pure (Left (GRPCIOCallError other)))

  (StartRequest pointers@(callPtr, _, callDetails) metadata tag) -> do
    grpcDebug ("Processing tag" ++ show tag)
    nextCallAsync <- runCallState server Listen Nothing allHandlers
    nextCall <- wait nextCallAsync

    case nextCall of
      Right callState -> do
        serverCall <- U.ServerCall
          <$> peek callPtr
          <*> pure (serverOpsQueue server)
          <*> C.getAllMetadataArray metadata
          <*> (C.timeSpec <$> C.callDetailsGetDeadline callDetails)
          <*> (MethodName <$> C.callDetailsGetMethod   callDetails)
          <*> (Host       <$> C.callDetailsGetHost     callDetails)

        grpcDebug "Send initial metadata"
        let operations = [ OpSendInitialMetadata (U.metadata serverCall), OpRecvMessage ]
        value <- runOpsAsync (U.unsafeSC serverCall) (U.callCQ serverCall) tag operations $ \(array, contexts) -> do
          grpcDebug "Creating a message result"
          let state = ReceivePayload serverCall pointers tag array contexts
          replaceCall server tag state
          grpcDebug "Set a message result"
        async (pure value)
      Left other -> async (pure (Left other))
  (ReceivePayload serverCall pointers tag array contexts) -> do
    grpcDebug ("Received payload with tag" ++ show tag)
    payload <- fmap (Right . catMaybes) $ mapM resultFromOpContext contexts
    teardownOpArrayAndContexts array contexts -- Safe to teardown after calling 'resultFromOpContext'.
    grpcDebug "Received MessageResult"
    -- TODO bracket this call
    let normalHandler =
          case findHandler serverCall allHandlers of
            Just (UnaryHandler a b) -> UnaryHandler a b
    let f = \_sc' bs ->
          case normalHandler of
            (UnaryHandler _ handler) -> convertServerHandler handler (const bs <$> U.convertCall serverCall)

    async $ case payload of
      Right [OpRecvMessageResult (Just body)] -> do
        grpcDebug ("Received payload:" ++ show body)

        (rsp, trailMeta, st, ds) <- f serverCall body
        let operations = [ OpRecvCloseOnServer , OpSendMessage rsp, OpSendStatusFromServer trailMeta st ds ]
        runOpsAsync (U.unsafeSC serverCall) (U.callCQ serverCall) tag operations $ \(array, contexts) -> do
          let state = AcknowledgeResponse serverCall pointers tag array contexts
          replaceCall server tag state
          pure ()
    where
      findHandler sc = find ((== U.callMethod sc) . handlerMethodName)
  (AcknowledgeResponse serverCall (callPtr, metadataPtr, callDetails) tag array contexts) -> do
    C.metadataArrayDestroy metadataPtr
    C.destroyCallDetails callDetails
    C.free callPtr
    deleteCall server tag

    async (pure (Right ()))

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
    handleError = (handleCallError logger . left herr =<<) . CE.try
      where herr (e :: CE.SomeException) = GRPCIOHandlerException (show e)

asyncDispatchLoop :: AsyncServer
             -> (String -> IO ())
             -> MetadataMap
             -> [Handler 'Normal]
             -> [Handler 'ClientStreaming]
             -> [Handler 'ServerStreaming]
             -> [Handler 'BiDiStreaming]
             -> IO ()
asyncDispatchLoop s logger md hN hC hS hB = do
  initialCallDataAsync <- runCallState s Listen Nothing hN
  initialCallData <- wait initialCallDataAsync
  case initialCallData of

    Right callData -> do
      serverLoop `concurrently_` clientLoop
      where
        serverLoop = forever $ do
          eitherEvent <- next (serverCallQueue s) Nothing
          case eitherEvent of
            Right event -> do
              clientCallData <- lookupCall s (C.eventTag event)
              case clientCallData of
                Just callData -> runCallState s callData (Just event) hN
                Nothing -> async (error "Failed to lookup call data")
            Left err -> async (error ("Failed to fetch event" ++ show err))
        clientLoop = forever $ do
          eitherEvent <- next (serverOpsQueue s) Nothing
          case eitherEvent of
            Right event -> do
              clientCallData <- lookupCall s (C.eventTag event)
              case clientCallData of
                Just callData -> runCallState s callData (Just event) hN
                Nothing -> async (error "Failed to lookup call data")
            Left err -> async (error ("Failed to fetch event" ++ show err))

    Left err -> error ("failed to create initial call data" ++ show err)

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
