{-# LANGUAGE DataKinds, DuplicateRecordFields, GADTs, LambdaCase, OverloadedStrings, RankNTypes, RecordWildCards,
             ScopedTypeVariables #-}

module Network.GRPC.HighLevel.Server.Unregistered where

import           Control.Arrow (left)
import           Control.Concurrent (myThreadId)
import           Control.Concurrent.Async
    (Async (..), async, cancel, wait, waitAnyCancel)
import           Control.Concurrent.STM
import           Control.Exception (AsyncException (..), asyncExceptionFromException, asyncExceptionToException)
import           Control.Exception.Safe hiding (Handler)
import qualified Control.Exception.Safe as E
import           Control.Monad
import           Data.Foldable (find)
import qualified Data.HashMap.Strict as HashMap
import qualified Foreign.Marshal.Alloc as C
import           Foreign.Storable (peek)
import           Network.GRPC.HighLevel.Server
import           Network.GRPC.LowLevel
import qualified Network.GRPC.LowLevel.Call.Unregistered as U
import           Network.GRPC.LowLevel.CompletionQueue.Internal (CompletionQueue (..), newTag, next)
import           Network.GRPC.LowLevel.GRPC (grpcDebug)
import           Network.GRPC.LowLevel.Op (destroyOpArrayAndContexts, readAndDestroy, runOpsAsync)
import           Network.GRPC.LowLevel.Server
import qualified Network.GRPC.LowLevel.Server.Unregistered as U
import qualified Network.GRPC.Unsafe as C
import qualified Network.GRPC.Unsafe.Metadata as C
import qualified Network.GRPC.Unsafe.Time as C
import           Proto3.Suite.Class
import           System.IO (hPutStrLn, stderr)
import qualified System.Posix.Signals as P

-- Exceptions that may be thrown during call state execution.
data CallStateException =
  ImpossiblePayload String
  -- ^ Programmer error. An unexpected message result was returned from the client.
  | NotFound C.Event
  -- ^ Programmer error. An event was not found in the map of in-progress call states.
  | UnknownHandler MethodName
  -- ^ An unknown handler was requested by a client call.
  deriving (Show, Typeable)

instance Exception CallStateException

data ServerException = Terminated
  deriving (Show, Typeable)

instance Exception ServerException where
  fromException = asyncExceptionFromException
  toException = asyncExceptionToException

runCallState :: AsyncServer ->  [Handler 'Normal] -> CallState -> IO (Async ())
runCallState server allHandlers = \case
  Listen -> do
    tag' <- newTag (serverCallQueue server)
    grpcDebug ("Listen: register for call with tag" ++ show tag')
    -- Allocate the C data necessary for the lifetime of a call.
    callPtr <- C.malloc
    metadataPtr <- C.metadataArrayCreate
    metadata  <- peek metadataPtr
    callDetails <- C.createCallDetails

    let
      -- Make a cleanup function to carry around that deallocates the call data
      -- in 'onException' and `finally` throughout the lifetime of the call.
      cleanup = do
        C.metadataArrayDestroy metadataPtr
        C.destroyCallDetails callDetails
        C.free callPtr
      -- Listen for a call from the client with 'grpcServerRequestCall'
      --  and insert the corresponding tag and call state into the server state.
      go = do
        callError <- C.grpcServerRequestCall (unsafeServer (server :: AsyncServer)) callPtr callDetails metadata (unsafeCQ . serverOpsQueue $ server) (unsafeCQ . serverCallQueue $ server) tag'
        case callError of
          C.CallOk -> do
            let state = StartRequest callPtr callDetails metadata tag' cleanup
            grpcDebug ("Listen: inserting call with tag" ++ show tag')
            insertCall server tag' state
          other -> do
            grpcDebug $ "Listen: grpcServerRequestCall: Call Error" ++ show other
            throw (GRPCIOCallError other)
    async (go `onException` cleanup)

  StartRequest callPtr callDetails metadata tag cleanup -> do
    -- Listen for a subsequent client call since we've started serving a client request.
    wait <$> runCallState server allHandlers Listen
    grpcDebug ("StartRequest: running operations for tag" ++ show tag)

    -- A call has been allocated to 'callPtr' by gRPC.
    serverCall <- U.ServerCall
      <$> peek callPtr
      <*> pure (serverOpsQueue server)
      <*> C.getAllMetadataArray metadata
      <*> (C.timeSpec <$> C.callDetailsGetDeadline callDetails)
      <*> (MethodName <$> C.callDetailsGetMethod   callDetails)
      <*> (Host       <$> C.callDetailsGetHost     callDetails)

    let
      -- Extend the cleanup function to destroy the server call if an exception occurs.
      cleanup' = cleanup >> U.destroyServerCall serverCall
      operations = [ OpSendInitialMetadata (U.metadata serverCall), OpRecvMessage ]
      -- Send an 'OpRecvMessage' operation to receive a payload from the client.
      sendOps = runOpsAsync (U.unsafeSC serverCall) (U.callCQ serverCall) tag operations $ \array contexts -> do
        -- Store the given array and contexts in the server to read when the payload comes back.
        let state = ReceivePayload serverCall tag array contexts cleanup'
        grpcDebug ("StartRequest: replacing call with tag" ++ show tag)
        replaceCall server tag state `onException` destroyOpArrayAndContexts array contexts
    async (sendOps `onException` cleanup')

  ReceivePayload serverCall tag array contexts cleanup -> do
    grpcDebug $ "ReceivePayload: Received payload for tag" ++ show tag
    payload <- readAndDestroy array contexts
    normalHandler <- maybe (throw $ UnknownHandler (U.callMethod serverCall)) pure (findHandler serverCall allHandlers)
      `onException` cleanup
    async (callHandler normalHandler payload `onException` cleanup)
    where
      findHandler sc = find ((== U.callMethod sc) . handlerMethodName)
      f normalHandler string =
        case normalHandler of
          UnaryHandler _ handler -> convertServerHandler handler (const string <$> U.convertCall serverCall)
      callHandler normalHandler = \case
        [OpRecvMessageResult (Just body)] -> do

          grpcDebug $ "ReceivePayload: Received calling handler for tag" ++ show tag
          (rsp, trailMeta, st, ds) <- f normalHandler body

          let operations = [ OpRecvCloseOnServer , OpSendMessage rsp, OpSendStatusFromServer trailMeta st ds ]
          runOpsAsync (U.unsafeSC serverCall) (U.callCQ serverCall) tag operations $ \array' contexts' -> do
            let state = AcknowledgeResponse tag array' contexts' cleanup
            grpcDebug $ "ReceivePayload: Replacing call for tag" ++ show tag
            replaceCall server tag state `onException` destroyOpArrayAndContexts array' contexts'
        rest -> throw (ImpossiblePayload $ "ReceivePayload: Impossible payload result: " ++ show rest)

  AcknowledgeResponse tag array contexts cleanup -> do
    destroyOpArrayAndContexts array contexts -- Safe to teardown after calling 'resultFromOpContext'.
    tid <- myThreadId
    grpcDebug $ "AcknowledgeResponse: Deleting Call with tag: " ++ show tag ++ "on thread:" ++ show tid
    deleteCall server tag `finally` cleanup
    async (pure ())

asyncDispatchLoop :: AsyncServer
             -> (String -> IO ())
             -> MetadataMap
             -> [Handler 'Normal]
             -> [Handler 'ClientStreaming]
             -> [Handler 'ServerStreaming]
             -> [Handler 'BiDiStreaming]
             -> IO (Async (), Async ())
asyncDispatchLoop s logger md hN _ _ _ = do
  wait <$> runCallState s hN Listen
  -- Spin up two threads to handle events concurrently on the call queue and operations queue.
  -- Each thread has a 1 second deadline in order to yield control back to haskell
  -- so we don't block indefinitely on the FFI.
  (,) <$> loop (serverCallQueue s) 1 <*> loop (serverOpsQueue s) 1
  where
    loop queue timeout = async . forever $ do
      eitherEvent <- next queue timeout
      case eitherEvent of
        Right event -> do
          clientCallData <- lookupCall s (C.eventTag event)
          case clientCallData of
            Just callData -> do
              ready <- newTVarIO False
              asyncCall <- async $ do
                -- The ready TVar is used to block the async runCallState until the call
                -- has been added to 'outstandingCallActions'.
                atomically (check =<< readTVar ready)
                tid <- myThreadId
                asyncCall <- runCallState s hN callData
                wait asyncCall `catchAny` (\ex -> hPutStrLn stderr (show ex)) `finally` cleanup tid

              atomically $ do
                modifyTVar' (outstandingCallActions s) (HashMap.insert (asyncThreadId asyncCall) asyncCall)
                modifyTVar' ready (const True)
              where cleanup tid = atomically $ modifyTVar' (outstandingCallActions s) (HashMap.delete tid)
            Nothing -> throw $ NotFound event
        Left GRPCIOTimeout -> pure ()
        Left err -> throw err

asyncServerLoop :: ServerOptions -> IO ()
asyncServerLoop ServerOptions{..} = do
  mainId <- myThreadId
  -- Throw a Terminated async exception to the main thread on SIGTERM to shutdown gracefully.
  P.installHandler P.sigTERM (P.CatchOnce $ throwTo mainId Terminated) Nothing

  withGRPC $ \grpc -> do
    server <- startAsyncServer grpc config
    (callLoop, opsLoop) <- asyncDispatchLoop server
                 optLogger
                 optInitialMetadata
                 optNormalHandlers
                 optClientStreamHandlers
                 optServerStreamHandlers
                 optBiDiStreamHandlers

    void (waitAnyCancel [callLoop, opsLoop])
      `catchesAsync` [
          E.Handler (\UserInterrupt -> cancel callLoop)
        , E.Handler (\Terminated    -> cancel callLoop)
        ]
      `finally` stopAsyncServer server


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
          , MaxReceiveMessageLength (2 ^ 32)
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
