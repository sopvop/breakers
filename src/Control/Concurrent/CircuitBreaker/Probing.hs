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
import           Control.Monad (join)
import           System.Clock (Clock (Monotonic), TimeSpec (..), getTime)

import           Control.Concurrent.CircuitBreaker.Core

-- | A circuit breaker which probes resource.
data ProbingBreaker = ProbingBreaker
  { cbTimeout   :: TimeSpec
  , cbStatus    :: TVar CircuitStatus
  , cbProbeLock :: MVar ()
  , cbAlarm     :: AlarmClock MonotonicTime
  }

-- | Creates new 'ProbingBreaker'.
newProbingBreaker
  :: TimeSpec -- ^ Time before probe attempt
  -> IO ProbingBreaker
newProbingBreaker ts = do
  tv <- newTVarIO CircuitWorking
  ProbingBreaker ts tv
    <$> newMVar ()
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
  -> (ProbingBreaker -> IO a)
  -> IO a
withProbingBreaker ts =
  bracket (newProbingBreaker ts) destroyProbingBreaker

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
  -> (TVar CircuitStatus -> MVar () -> IO (Maybe a))
  -> IO a
  -> IO a
withBreaker' ProbingBreaker{..} probe act = do
  s <- readTVarIO cbStatus -- fast path
  case s of
    CircuitBroken  -> throwIO BrokenCircuit
    CircuitWorking -> onException act markBroken
    CircuitProbing -> throwBad $ onException (probe cbStatus cbProbeLock) markBroken
  where
    throwBad f = f >>= maybe (throwIO BrokenCircuit) pure

    markBroken = do
      t <- getTime Monotonic
      atomically $ do
        st <- readTVar cbStatus
        case st of
          CircuitBroken -> pure ()
          _ -> do
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
    probe cbStatus cbProbeLock = mask $ \restore -> do
      mb <- tryTakeMVar cbProbeLock
      case mb of
        Nothing -> pure Nothing -- we only allow one thread to make a probe, other are dropped
        Just _ ->
          finally
            (restore (Just <$> act)
             <* atomically (writeTVar cbStatus CircuitWorking))
            -- even if this interrupts, it does not break anything
            $ putMVar cbProbeLock ()

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
    probe cbStatus cbProbeLock = mask $ \restore -> do
      join $ withMVar cbProbeLock $ \_ -> do
        s <- readTVarIO cbStatus -- re-check status now that we have lock
        case s of
          CircuitBroken  -> pure (pure Nothing) -- other thread probed
          CircuitWorking -> pure (Just <$> act) -- run action without lock
          CircuitProbing -> -- we are the probing thread
            (pure . Just <$> restore act)
            <* atomically (writeTVar cbStatus CircuitWorking)

-- | A variant of 'withBreakerWaiting' which returns 'Nothing' instead of
-- throwing.
tryWithBreakerWaiting :: ProbingBreaker -> IO a -> IO (Maybe a)
tryWithBreakerWaiting pb act =
  catch (Just <$> withBreakerWaiting pb act)
  $ \BrokenCircuit -> pure Nothing
