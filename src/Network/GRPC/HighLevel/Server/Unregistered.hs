{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.GRPC.HighLevel.Server.Unregistered where

import           Control.Arrow
import           Control.Concurrent.Async                  (async, wait)
import qualified Control.Exception                         as CE
import           Control.Monad
import           Data.Foldable                             (find)
import           Network.GRPC.HighLevel.Server
import           Network.GRPC.LowLevel
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
import           Network.GRPC.LowLevel.CompletionQueue.Internal (newTag, CompletionQueue(..))

data CallState = Create Server C.Tag | Process U.ServerCall | MessageResult ServerCall OpArray OpContexts | Finish

createCallData server tag = processCallData (Create server tag)

processCallData :: CallState -> Event -> IO CallState
processCallData callState event= case callState of
  (Create server tag) ->
    with allocs $ \(call, metadata, callDetails) -> do
      metadataPtr  <- peek metadata
      let callQueue = unsafeCQ (serverCallCQ server)
      callError <- C.grpcServerRequestCall (unsafeServer server) call callDetails metadataPtr callQueue (unsafeCQ . serverCQ $ server) tag
      case callError of
        C.CallOk -> do
          Process <$> (U.ServerCall
            <$> peek call
            <*> return (serverCallCQ server)
            <*> return tag
            <*> C.getAllMetadataArray metadataPtr
            <*> (C.timeSpec <$> C.callDetailsGetDeadline callDetails)
            <*> (MethodName <$> C.callDetailsGetMethod   callDetails)
            <*> (Host       <$> C.callDetailsGetHost     callDetails))
    (Process serverCall) ->
      nextTag <- newTag (callCQ serverCall)
      nextCall <- createCallData s nextTag
      insertCall s nextTag nextCall -- atomically

      runOpsAsync serverCall callQueue [ OpSendInitialMetadata initMeta , OpRecvMessage ] $ \(array, contexts) -> do
        let state = MessageResult serverCall array contexts
        replaceCall s (tag serverCall) state
        pure state
    (MessageResult serverCall array contexts) -> do
      payload <- readContexts
      -- TODO bracket this call
      teardownArrayAndContexts array contexts

      case payload of
        Right [OpRecvMessageResult (Just body)] -> do
          let operations = [ OpRecvCloseOnServer , OpSendMessage rsp, OpSendStatusFromServer trailMeta st ds ]
          runOpsAsync serverCall callQueue operations $ \(array, contexts) -> do
            let state = MessageSent serverCall array contexts
            replaceCall s (tag serverCall) state
            pure state
    (MessageSent serverCall array contexts) -> do
      teardownArrayAndContexts array contexts
    where
      allocs = (,,) <$> mgdPtr <*> managed C.withMetadataArrayPtr <*> managed C.withCallDetails

dispatchLoop :: Server
             -> (String -> IO ())
             -> MetadataMap
             -> [Handler 'Normal]
             -> [Handler 'ClientStreaming]
             -> [Handler 'ServerStreaming]
             -> [Handler 'BiDiStreaming]
             -> IO ()
dispatchLoop s logger md hN hC hS hB = do
  firstRequestTag <- newTag (serverCQ s)
  initialCallData <- createCallData s firstRequestTag -- C.grpcServerRequestCall >> returns (Process ServerCall)
  insertCall s tag processInitialCallData

  forever $ do
    event <- next' completionQueue
    let clientCallData = lookupTag (eventTag tag)
    processCallData clientCallData event

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
      handleError $ do
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
