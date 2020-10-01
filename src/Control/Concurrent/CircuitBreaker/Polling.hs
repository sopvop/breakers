{-# LANGUAGE LambdaCase #-}
-- | A circuit breaker which works by regularly polling provided action.
--
-- 'PollingBreaker' starts in "working" state. If action run with
-- 'withBreaker' throws exception, the circuit breaker switches into "broken"
-- state. In this state the breaker polls resource with provided action until
-- it returns successfully, in which case breaker is fixed. While it is broken,
-- any action run with 'withBreaker' will fail with 'BrokenCircuit' exception.
--
module Control.Concurrent.CircuitBreaker.Polling
  ( PollingBreaker
  , newPollingBreaker
  , destroyPollingBreaker
  , withPollingBreaker
  , withBreaker
  , tryWithBreaker
  , getStatusSTM
  , getStatus
  , CircuitStatus(..)
  , BrokenCircuit(..)
  ) where

import           Control.Concurrent.AlarmClock
    (AlarmClock, MonotonicTime (..), destroyAlarmClock, newAlarmClock,
    setAlarmSTM)
import           Control.Concurrent.STM
    (STM, TVar, atomically, newTVarIO, readTVar, readTVarIO, writeTVar)
import           Control.Exception
    (Exception (..), SomeAsyncException (..), SomeException, bracket, catch,
    mask, onException, throwIO)
import           Control.Monad (when)
import           System.Clock (Clock (Monotonic), TimeSpec (..), getTime)

import           Control.Concurrent.CircuitBreaker.Core

-- | A circuit breaker which polls resource.
data PollingBreaker = PollingBreaker
  { cbTimeout :: TimeSpec
  , cbStatus  :: TVar CircuitStatus
  , cbAlarm   :: AlarmClock MonotonicTime
  }

isSyncException :: Exception e => e -> Bool
isSyncException e =
    case fromException (toException e) of
        Just (SomeAsyncException _) -> False
        Nothing                     -> True

catchAny :: IO a -> (SomeException -> IO a) -> IO a
catchAny act f = catch act $ \case
  e | isSyncException e -> f e
    | otherwise -> throwIO e

poll
  :: TVar CircuitStatus
  -> TimeSpec
  -> IO Bool
  -> AlarmClock MonotonicTime
  -> IO ()
poll status tout act alarm = mask $ \restore -> do
  atomically $ writeTVar status CircuitProbing
  r <- catchAny (restore act) $
    \_ ->
      fmap (const True) . atomically $ do
          setAlarmSTM alarm (MonotonicTime tout)
          writeTVar status CircuitProbing
    -- setAlarm only calls modifyTVar and should not be ininterruptible
    -- how do we singal async exception happening inside act and breaking
    -- the whole thing though
  when r . atomically $ writeTVar status CircuitWorking

-- | Creates new 'PollingBreaker'.
newPollingBreaker
  :: TimeSpec
  -> IO Bool -- ^ Probing action. Returning 'False' or throwing exceptions is
             -- treated as failure.
  -> IO PollingBreaker
newPollingBreaker tout act = do
  st <- newTVarIO CircuitWorking
  PollingBreaker tout st
    <$> newAlarmClock (poll st tout act)

-- | Destroys 'PollingBreaker', freeing used resources.
destroyPollingBreaker
  :: PollingBreaker
  -> IO ()
destroyPollingBreaker PollingBreaker{..} =
  destroyAlarmClock cbAlarm


-- | Runs action with new 'PollingBreaker' and destroys it afterwards.
withPollingBreaker
  :: TimeSpec
  -> IO Bool
  -> (PollingBreaker -> IO a)
  -> IO a
withPollingBreaker ts probe =
  bracket (newPollingBreaker ts probe) destroyPollingBreaker


-- | Monitor breaker status with STM
getStatusSTM :: PollingBreaker -> STM CircuitStatus
getStatusSTM PollingBreaker{..} = readTVar cbStatus

-- | Get breaker status without starting STM transaction
getStatus :: PollingBreaker -> IO CircuitStatus
getStatus PollingBreaker{..} = readTVarIO cbStatus


-- | Runs action checking circuit breaker status. If action throws any exception
-- then it is considered failure and circuit to be broken.
--
-- If circuit is in broken state, then this function throws 'CircuitBroken'
-- exception.
--
-- Any exception thrown by the action is always re-thrown.
withBreaker
  :: PollingBreaker
  -> IO a
  -> IO a
withBreaker PollingBreaker{..} act = do
  s <- readTVarIO cbStatus -- fast path
  case s of
    CircuitBroken  -> throwIO BrokenCircuit
    CircuitProbing -> throwIO BrokenCircuit
    CircuitWorking -> onException act markBroken
  where
    markBroken = do
      t <- getTime Monotonic
      atomically $ do
        st <- readTVar cbStatus
        case st of
          CircuitBroken -> pure ()
          _ -> do
            writeTVar cbStatus CircuitBroken
            setAlarmSTM cbAlarm (MonotonicTime $ t + cbTimeout)

-- | A variant of 'withBreaker', which returns 'Nothing' instead of throwing.
tryWithBreaker
  :: PollingBreaker
  -> IO a
  -> IO (Maybe a)
tryWithBreaker pb act =
  catch (Just <$> withBreaker pb act)
  $ \BrokenCircuit -> pure Nothing
