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
import           Network.GRPC.LowLevel.CompletionQueue.Internal (newTag, CompletionQueue(..), next)
import           Network.GRPC.LowLevel.Op (OpContext, runOpsAsync, resultFromOpContext, teardownOpArrayAndContexts)
import qualified Network.GRPC.Unsafe.Op                         as C
import Data.Maybe (catMaybes)
import Data.ByteString (ByteString)

createCallData server tag = processCallData server (Create tag)

processCallData :: Server Handler -> CallState Handler -> Maybe C.Event -> [Handler 'Normal] -> IO (Either GRPCIOError (CallState Handler))
processCallData server callState event allHandlers = case callState of
  (Create tag) ->
    with allocs $ \(call, metadata, callDetails) -> do
      metadataPtr  <- peek metadata
      let callQueue = unsafeCQ (serverCallCQ server)
      callError <- C.grpcServerRequestCall (unsafeServer server) call callDetails metadataPtr callQueue (unsafeCQ . serverCQ $ server) tag
      case callError of
        C.CallOk -> Right . Process <$> (U.ServerCall
            <$> peek call
            <*> return (serverCallCQ server)
            <*> return tag
            <*> C.getAllMetadataArray metadataPtr
            <*> (C.timeSpec <$> C.callDetailsGetDeadline callDetails)
            <*> (MethodName <$> C.callDetailsGetMethod   callDetails)
            <*> (Host       <$> C.callDetailsGetHost     callDetails))
        other -> pure (Left (GRPCIOCallError other))
    where
      allocs = (,,) <$> mgdPtr <*> managed C.withMetadataArrayPtr <*> managed C.withCallDetails
  (Process serverCall) -> do
    nextTag <- newTag (U.callCQ serverCall)
    nextCall <- createCallData server nextTag Nothing allHandlers
    case nextCall of
      Right callState -> do
        insertCall server nextTag callState -- atomically

        runOpsAsync (U.unsafeSC serverCall) (U.callCQ serverCall) (U.callTag serverCall) [ OpSendInitialMetadata (U.metadata serverCall), OpRecvMessage ] $ \(array, contexts) -> do
          let state = MessageResult serverCall array contexts
          replaceCall server (U.callTag serverCall) state
          pure state
      Left other -> pure (Left other)
  (MessageResult serverCall array contexts) -> do
    payload <- fmap (Right . catMaybes) $ mapM resultFromOpContext contexts
    -- TODO bracket this call
    teardownOpArrayAndContexts array contexts

    let normalHandler =
          case findHandler serverCall allHandlers of
            Just (UnaryHandler a b) -> UnaryHandler a b
    let f = \_sc' bs ->
          case normalHandler of
            (UnaryHandler _ handler) -> convertServerHandler handler (const bs <$> U.convertCall serverCall)

    case payload of
      Right [OpRecvMessageResult (Just body)] -> do

        (rsp, trailMeta, st, ds) <- f serverCall body
        let operations = [ OpRecvCloseOnServer , OpSendMessage rsp, OpSendStatusFromServer trailMeta st ds ]
        runOpsAsync (U.unsafeSC serverCall) (U.callCQ serverCall) (U.callTag serverCall) operations $ \(array, contexts) -> do
          let state = MessageSent serverCall array contexts
          replaceCall server (U.callTag serverCall) state
          pure state
    where
      findHandler sc = find ((== U.callMethod sc) . handlerMethodName)
  (MessageSent serverCall array contexts) -> do
    teardownOpArrayAndContexts array contexts
    deleteCall server (U.callTag serverCall)
    pure (Right Finish)

dispatchLoop :: Server Handler
             -> (String -> IO ())
             -> MetadataMap
             -> [Handler 'Normal]
             -> [Handler 'ClientStreaming]
             -> [Handler 'ServerStreaming]
             -> [Handler 'BiDiStreaming]
             -> IO ()
dispatchLoop s logger md hN hC hS hB = do
  firstRequestTag <- newTag (serverCQ s)
  initialCallData <- createCallData s firstRequestTag Nothing hN-- C.grpcServerRequestCall >> returns (Process ServerCall)
  case initialCallData of
    Right callData -> do
      insertCall s firstRequestTag callData
      forever $ do
        eitherEvent <- next (serverCQ s) Nothing
        case eitherEvent of
          Right event -> do
            clientCallData <- lookupCall s (C.eventTag event)
            case clientCallData of
              Just callData -> processCallData s callData (Just event) hN
              Nothing -> error "Failed to lookup call data"
          Left err -> error ("Failed to fetch event" ++ show err)
    Left err -> error ("failed to create initial call data" ++ show err)

  where
    allHandlers = map AnyHandler hN ++ map AnyHandler hC
                  ++ map AnyHandler hS ++ map AnyHandler hB



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
