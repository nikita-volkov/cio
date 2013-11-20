module CIO where

import CIO.Prelude hiding (sequence, sequence_, mapM, mapM_, forM, forM_)
import qualified Control.Concurrent.ParallelIO.Local as ParallelIO


-- | Concurrent IO. A composable monad of IO actions executable in a shared pool of threads.
newtype CIO r = CIO (ReaderT ParallelIO.Pool IO r)
  deriving (Functor, Applicative, Monad)

instance MonadIO CIO where
  liftIO io = CIO $ lift io

instance MonadSTM CIO where
  liftSTM = CIO . liftSTM

-- | Run with a pool of the specified size.
run :: Int -> CIO r -> IO r
run numCapabilities (CIO t) = ParallelIO.withPool numCapabilities $ runReaderT t

-- | Run with a pool the size of the amount of available processors.
run' :: CIO r -> IO r
run' cio = do
  numCapabilities <- getNumCapabilities
  run numCapabilities cio

-- | Same as @Control.Monad.'Control.Monad.sequence_'@, but does it concurrently. 
-- Blocks the calling thread until all actions are finished.
sequence_ :: [CIO a] -> CIO ()
sequence_ actions = 
  CIO $ do
    pool <- ask
    lift $ ParallelIO.parallel_ pool $ map (poolToCIOToIO pool) actions 
  where
    poolToCIOToIO pool (CIO t) = runReaderT t pool

-- | Same as @Control.Monad.'Control.Monad.sequence'@, but does it concurrently. 
sequence :: [CIO a] -> CIO [a]
sequence actions = 
  CIO $ do
    pool <- ask
    lift $ ParallelIO.parallel pool $ map (poolToCIOToIO pool) actions 
  where
    poolToCIOToIO pool (CIO t) = runReaderT t pool

-- | Same as 'sequence' with a difference that it does not maintain the order of results,
-- which allows it to execute a bit more effeciently.
sequenceInterleaved :: [CIO a] -> CIO [a]
sequenceInterleaved actions = 
  CIO $ do
    pool <- ask
    lift $ ParallelIO.parallelInterleaved pool $ map (poolToCIOToIO pool) actions 
  where
    poolToCIOToIO pool (CIO t) = runReaderT t pool

mapM :: (a -> CIO b) -> [a] -> CIO [b]
mapM f = sequence . map f

mapM_ :: (a -> CIO b) -> [a] -> CIO ()
mapM_ f = sequence_ . map f

forM :: [a] -> (a -> CIO b) -> CIO [b]
forM = flip mapM

forM_ :: [a] -> (a -> CIO b) -> CIO ()
forM_ = flip mapM_


