-- | A circuit breaker which works by probing on request.
--
-- 'ProbingBreaker' starts in "working" state. If action run with 'withBreaker'
-- throws any exception, the circuit breaker switches into "broken" state. In
-- this state all 'withBreaker' calls will fail until timeout passes and circuit
-- breaker switches to a "probing" state. In probing state one thread will run
-- to probe the resource, and it is successful then circuit is considered fixed
-- and breaker switches to working state again.
module Control.Concurrent.CircuitBreaker.Probing
  ( ProbingBreaker
  , newProbingBreaker
  , destroyProbingBreaker
  , withProbingBreaker
  , withBreaker
  , tryWithBreaker
  , withBreakerWaiting
  , tryWithBreakerWaiting
  , getStatusSTM
  , getStatus
  , CircuitStatus(..)
  , BrokenCircuit(..)
  ) where

import           Control.Concurrent.AlarmClock
import           Control.Concurrent.MVar
import           Control.Concurrent.STM
import           Control.Exception
import           Control.Monad
import           System.Clock (Clock (Monotonic), TimeSpec (..), getTime)

import           Control.Concurrent.CircuitBreaker.Core

-- | A circuit breaker which probes resource.
data ProbingBreaker = ProbingBreaker
  { cbTimeout      :: TimeSpec
  , cbMaxErrors    :: Int
  , cbErrorTimeout :: TimeSpec
  , cbStatus       :: TVar CircuitStatus
  , cbErrorState   :: TVar ErrorsState
  , cbProbeLock    :: MVar ()
  , cbAlarm        :: AlarmClock MonotonicTime
  }

data ErrorsState = ErrorsState
  { errorCount :: Int
  , errorTime  :: TimeSpec
  }

-- | Creates new 'ProbingBreaker'.
newProbingBreaker
  :: TimeSpec -- ^ Time before probe attempt
  -> Int -- ^ How many consecutive errors should happen before circuit is broken
  -> TimeSpec -- ^ Time between errors before error count resets
  -> IO ProbingBreaker
newProbingBreaker ts errc timeout = do
  tv <- newTVarIO CircuitWorking
  ProbingBreaker ts errc timeout tv
    <$> newTVarIO (ErrorsState 0 (TimeSpec 0 0))
    <*> newMVar ()
    <*> newAlarmClock (const $ reset tv)

-- | Destroys 'ProbingBreaker', freeing used resources.
destroyProbingBreaker
  :: ProbingBreaker
  -> IO ()
destroyProbingBreaker ProbingBreaker{..} =
  destroyAlarmClock cbAlarm

-- | Runs action with new 'ProbingBreaker' and destroys it afterwards.
withProbingBreaker
  :: TimeSpec
  -> Int
  -> TimeSpec
  -> (ProbingBreaker -> IO a)
  -> IO a
withProbingBreaker ts tc ms =
  bracket (newProbingBreaker ts tc ms) destroyProbingBreaker

-- | Monitor breaker status with STM
getStatusSTM :: ProbingBreaker -> STM CircuitStatus
getStatusSTM ProbingBreaker{..} = readTVar cbStatus

-- | Get breaker status without starting STM transaction
getStatus :: ProbingBreaker -> IO CircuitStatus
getStatus ProbingBreaker{..} = readTVarIO cbStatus

reset :: TVar CircuitStatus -> IO ()
reset st = atomically $ do
  v <- readTVar st
  case v of
    CircuitWorking -> pure ()
    CircuitProbing -> pure ()
    CircuitBroken  -> writeTVar st CircuitProbing


withBreaker'
  :: ProbingBreaker
  -> (MVar () -> IO (Maybe a))
  -> IO a
  -> IO a
withBreaker' ProbingBreaker{..} probe act = do
  s <- readTVarIO cbStatus -- fast path
  case s of
    CircuitBroken  -> throwIO BrokenCircuit
    CircuitWorking -> onException act markBroken >>= \r -> r <$ resetErrors
    CircuitProbing -> do
      r <- try $ probe cbProbeLock
      case r of
        Left e -> markBroken *> throwIO (e :: SomeException)
        Right res -> case res of
          Nothing -> throwIO BrokenCircuit
          Just v -> v <$ markWorking
  where
    resetErrors =
      atomically . writeTVar cbErrorState $ ErrorsState 0 (TimeSpec 0 0)

    markWorking = atomically $ do
      writeTVar cbStatus CircuitWorking
      writeTVar cbErrorState (ErrorsState 0 $ TimeSpec 0 0)

    markBroken = do
      t <- getTime Monotonic
      atomically $ do
        st <- readTVar cbStatus
        case st of
          CircuitBroken -> pure ()
          CircuitWorking -> do
            ErrorsState {..} <- readTVar cbErrorState
            let
              errTimedOut = t - errorTime > cbErrorTimeout
              numErrors = if errTimedOut then 0 else errorCount
            writeTVar cbErrorState $ ErrorsState (succ numErrors) t
            when (numErrors > cbMaxErrors) $ do
              writeTVar cbStatus CircuitBroken
              setAlarmSTM cbAlarm (MonotonicTime $ t + cbTimeout)
          CircuitProbing -> do
            writeTVar cbStatus CircuitBroken
            setAlarmSTM cbAlarm (MonotonicTime $ t + cbTimeout)

-- | Runs action checking circuit breaker status. If action throws any exception
-- then it is considered failure and circuit to be broken.
--
-- If circuit is in broken state, then this function throws 'CircuitBroken'
-- exception.
--
-- If breaker is in probing state, then only one thread will be run, others will
-- get 'BrokenCircuit' exception.
--
-- Any exception throws by the action is always re-thrown.
withBreaker :: ProbingBreaker -> IO a -> IO a
withBreaker pb act = withBreaker' pb probe act
  where
    probe cbProbeLock = mask $ \restore -> do
      mb <- tryTakeMVar cbProbeLock
      case mb of
        Nothing -> pure Nothing -- we only allow one thread to make a probe, other are dropped
        Just _ ->
          finally (restore (Just <$> act)) $ putMVar cbProbeLock ()

-- | A 'withBreaker' variant which returns 'Nothing' instead of throwing.
tryWithBreaker
  :: ProbingBreaker
  -> IO a
  -> IO (Maybe a)
tryWithBreaker b act =
  catch (Just <$> withBreaker b act)
  $ \BrokenCircuit -> pure Nothing

-- | As 'withBreaker', but in "probing" state all threads wait for probing
-- thread to finish instead of throwing 'BrokenCircuit' exception.
withBreakerWaiting :: ProbingBreaker -> IO a -> IO a
withBreakerWaiting pb act = withBreaker' pb probe act
  where
    probe cbProbeLock = mask $ \restore -> do
      join $ withMVar cbProbeLock $ \_ -> do
        s <- readTVarIO $ cbStatus pb -- re-check status now that we have lock
        case s of
          CircuitBroken  -> pure (pure Nothing) -- other thread probed
          CircuitWorking -> pure (Just <$> act) -- run action without lock
          CircuitProbing -> -- we are the probing thread
            pure . Just <$> restore act

-- | A variant of 'withBreakerWaiting' which returns 'Nothing' instead of
-- throwing.
tryWithBreakerWaiting :: ProbingBreaker -> IO a -> IO (Maybe a)
tryWithBreakerWaiting pb act =
  catch (Just <$> withBreakerWaiting pb act)
  $ \BrokenCircuit -> pure Nothing
