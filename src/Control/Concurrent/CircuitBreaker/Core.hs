-- | Common functionality
module Control.Concurrent.CircuitBreaker.Core
  ( BrokenCircuit(..)
  , CircuitStatus(..)
  ) where

import           Control.Exception

-- | Status of circuit breaker.
data CircuitStatus
  = CircuitWorking
  | CircuitBroken
  | CircuitProbing
  deriving (Show)

-- | Exception thrown when circuit breaker is in broken state.
data BrokenCircuit = BrokenCircuit
  deriving (Show)

instance Exception BrokenCircuit
