{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.GRPC.HighLevel.Server.Unregistered where

import           Control.Concurrent.Async                  (async, wait, concurrently_)
import           Control.Monad
import           Data.Foldable                             (find)
import           Network.GRPC.HighLevel.Server
import           Network.GRPC.LowLevel
import           Network.GRPC.LowLevel.GRPC (grpcDebug)
import qualified Network.GRPC.LowLevel.Call.Unregistered   as U
import Network.GRPC.LowLevel.Server
import qualified Network.GRPC.Unsafe                            as C
import qualified Network.GRPC.Unsafe.Metadata                   as C
import qualified Network.GRPC.Unsafe.Time                       as C
import           Foreign.Storable                               (peek)
import           Network.GRPC.LowLevel.CompletionQueue.Internal (newTag, CompletionQueue(..), next)
import           Network.GRPC.LowLevel.Op (runOpsAsync, resultFromOpContext, teardownOpArrayAndContexts)
import Data.Maybe (catMaybes)
import qualified Foreign.Marshal.Alloc as C

processCallData :: Server -> CallState -> Maybe C.Event -> [Handler 'Normal] -> IO (Either GRPCIOError (CallState))
processCallData server callState _event allHandlers = case callState of
  Listen -> do
    tag' <- newTag (serverCQ server)
    grpcDebug ("Creating tag" ++ show tag')
    callPtr <- C.malloc
    metadataPtr <- C.metadataArrayCreate
    metadata  <- peek metadataPtr
    callDetails <- C.createCallDetails
    callError <- C.grpcServerRequestCall (unsafeServer server) callPtr callDetails metadata (unsafeCQ . serverCallCQ $ server) (unsafeCQ . serverCQ $ server) tag'
    -- C.free metadataPtr
    case callError of
      C.CallOk -> do
        let state = StartRequest callPtr metadata callDetails tag'
        insertCall server tag' state
        pure (Right state)
      other -> pure (Left (GRPCIOCallError other))

  (StartRequest callPtr metadata callDetails tag) -> do
    grpcDebug ("Processing tag" ++ show tag)
    nextCall <- processCallData server Listen Nothing allHandlers
    case nextCall of
      Right callState -> do
        serverCall <- U.ServerCall
          <$> peek callPtr
          <*> return (serverCallCQ server)
          <*> return tag
          <*> C.getAllMetadataArray metadata
          <*> (C.timeSpec <$> C.callDetailsGetDeadline callDetails)
          <*> (MethodName <$> C.callDetailsGetMethod   callDetails)
          <*> (Host       <$> C.callDetailsGetHost     callDetails)

        grpcDebug "Send initial metadata"
        runOpsAsync (U.unsafeSC serverCall) (U.callCQ serverCall) tag [ OpSendInitialMetadata (U.metadata serverCall), OpRecvMessage ] $ \(array, contexts) -> do
          grpcDebug "Creating a message result"
          let state = ReceivePayload serverCall tag array contexts
          replaceCall server tag state
          grpcDebug "Set a message result"
          pure state
      Left other -> pure (Left other)
  (ReceivePayload serverCall tag array contexts) -> do
    grpcDebug ("Received payload with tag" ++ show tag)
    payload <- fmap (Right . catMaybes) $ mapM resultFromOpContext contexts
    grpcDebug "Received MessageResult"
    -- TODO bracket this call
    let normalHandler =
          case findHandler serverCall allHandlers of
            Just (UnaryHandler a b) -> UnaryHandler a b
    let f = \_sc' bs ->
          case normalHandler of
            (UnaryHandler _ handler) -> convertServerHandler handler (const bs <$> U.convertCall serverCall)

    case payload of
      Right [OpRecvMessageResult (Just body)] -> do
        grpcDebug ("Received payload:" ++ show body)

        (rsp, trailMeta, st, ds) <- f serverCall body
        let operations = [ OpRecvCloseOnServer , OpSendMessage rsp, OpSendStatusFromServer trailMeta st ds ]
        runOpsAsync (U.unsafeSC serverCall) (U.callCQ serverCall) (U.callTag serverCall) operations $ \(array, contexts) -> do
          let state = AcknowledgeResponse serverCall tag array contexts
          replaceCall server (U.callTag serverCall) state
          pure state
    where
      findHandler sc = find ((== U.callMethod sc) . handlerMethodName)
  (AcknowledgeResponse _serverCall tag array contexts) -> do
    teardownOpArrayAndContexts array contexts
    deleteCall server tag
    pure (Right Finish)

dispatchLoop :: Server
             -> (String -> IO ())
             -> MetadataMap
             -> [Handler 'Normal]
             -> [Handler 'ClientStreaming]
             -> [Handler 'ServerStreaming]
             -> [Handler 'BiDiStreaming]
             -> IO ()
dispatchLoop s _logger _md hN hC hS hB = do
  initialCallData <- processCallData s Listen Nothing hN-- C.grpcServerRequestCall >> returns (Process ServerCall)
  case initialCallData of

    Right callData -> do
      serverLoop `concurrently_` clientLoop
      where
        serverLoop = forever $ do
          eitherEvent <- next (serverCQ s) Nothing
          case eitherEvent of
            Right event -> do
              clientCallData <- lookupCall s (C.eventTag event)
              case clientCallData of
                Just callData -> processCallData s callData (Just event) hN
                Nothing -> error "Failed to lookup call data"
            Left err -> error ("Failed to fetch event" ++ show err)
        clientLoop = forever $ do
          eitherEvent <- next (serverCallCQ s) Nothing
          case eitherEvent of
            Right event -> do
              clientCallData <- lookupCall s (C.eventTag event)
              case clientCallData of
                Just callData -> processCallData s callData (Just event) hN
                Nothing -> error "Failed to lookup call data"
            Left err -> error ("Failed to fetch event" ++ show err)

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
