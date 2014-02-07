module CIO 
  (
    CIO,
    runCIO,
    runCIO',
    MonadCIO(..),
    mapMConcurrently,
    mapMConcurrently',
    mapMConcurrently_,
    forMConcurrently,
    forMConcurrently',
    forMConcurrently_,
    distributeConcurrently,
    distributeConcurrently_,
  )
  where

import CIO.Prelude
import qualified Control.Concurrent.ParallelIO.Local as ParallelIO


-- | Concurrent IO. A composable monad of IO actions executable in a shared pool of threads.
newtype CIO r = CIO (ReaderT (ParallelIO.Pool, Int) IO r)
  deriving (Functor, Applicative, Monad)

instance MonadIO CIO where
  liftIO io = CIO $ lift io

instance MonadSTM CIO where
  liftSTM = CIO . liftSTM

-- | Run with a pool of the specified size.
runCIO :: Int -> CIO r -> IO r
runCIO numCapabilities (CIO t) =
  ParallelIO.withPool numCapabilities $ \pool -> runReaderT t (pool, numCapabilities)

-- | Run with a pool the size of the amount of available processors.
runCIO' :: CIO r -> IO r
runCIO' cio = do
  numCapabilities <- getNumCapabilities
  runCIO numCapabilities cio


class (Monad m) => MonadCIO m where
  -- | Get the maximum number of available threads, which is set in 'runCIO'.
  getPoolNumCapabilities :: m Int
  -- | Same as @Control.Monad.'Control.Monad.sequence'@, but performs concurrently. 
  sequenceConcurrently :: [m a] -> m [a]
  -- | Same as 'sequenceConcurrently' with a difference that 
  -- it does not maintain the order of results,
  -- which allows it to execute a bit more efficiently.
  sequenceConcurrently' :: [m a] -> m [a]
  -- | Same as @Control.Monad.'Control.Monad.sequence_'@, but performs concurrently. 
  -- Blocks the calling thread until all actions are finished.
  sequenceConcurrently_ :: [m a] -> m ()

instance MonadCIO CIO where
  getPoolNumCapabilities =
    CIO $ do
      (_, z) <- ask
      return z
  sequenceConcurrently actions = 
    CIO $ do
      env@(pool, _) <- ask
      lift $ ParallelIO.parallel pool $ map (envToCIOToIO env) actions 
    where
      envToCIOToIO env (CIO t) = runReaderT t env
  sequenceConcurrently' actions = 
    CIO $ do
      env@(pool, _) <- ask
      lift $ ParallelIO.parallelInterleaved pool $ map (envToCIOToIO env) actions 
    where
      envToCIOToIO env (CIO t) = runReaderT t env
  sequenceConcurrently_ actions = 
    CIO $ do
      env@(pool, _) <- ask
      lift $ ParallelIO.parallel_ pool $ map (envToCIOToIO env) actions 
    where
      envToCIOToIO env (CIO t) = runReaderT t env

instance (MonadCIO m) => MonadCIO (ReaderT r m) where
  getPoolNumCapabilities = lift getPoolNumCapabilities
  sequenceConcurrently actions = do
    env <- ask
    let cioActions = map (flip runReaderT env) actions
    lift $ sequenceConcurrently cioActions
  sequenceConcurrently' actions = do
    env <- ask
    let cioActions = map (flip runReaderT env) actions
    lift $ sequenceConcurrently' cioActions
  sequenceConcurrently_ actions = do
    env <- ask
    let cioActions = map (flip runReaderT env) actions
    lift $ sequenceConcurrently_ cioActions

instance (MonadCIO m, Monoid w) => MonadCIO (WriterT w m) where
  getPoolNumCapabilities = lift getPoolNumCapabilities
  sequenceConcurrently actions = do
    let cioActions = map runWriterT actions
    WriterT $ do
      (as, ws) <- return . unzip =<< sequenceConcurrently cioActions
      return (as, mconcat ws)
  sequenceConcurrently' actions = do
    let cioActions = map runWriterT actions
    WriterT $ do
      (as, ws) <- return . unzip =<< sequenceConcurrently' cioActions
      return (as, mconcat ws)
  sequenceConcurrently_ actions = do
    let cioActions = map execWriterT actions
    WriterT $ do
      ws <- sequenceConcurrently' cioActions
      return ((), mconcat ws)


mapMConcurrently :: (MonadCIO m) => (a -> m b) -> [a] -> m [b]
mapMConcurrently f = sequenceConcurrently . map f

mapMConcurrently' :: (MonadCIO m) => (a -> m b) -> [a] -> m [b]
mapMConcurrently' f = sequenceConcurrently' . map f

mapMConcurrently_ :: (MonadCIO m) => (a -> m b) -> [a] -> m ()
mapMConcurrently_ f = sequenceConcurrently_ . map f

forMConcurrently :: (MonadCIO m) => [a] -> (a -> m b) -> m [b]
forMConcurrently = flip mapMConcurrently

forMConcurrently' :: (MonadCIO m) => [a] -> (a -> m b) -> m [b]
forMConcurrently' = flip mapMConcurrently'

forMConcurrently_ :: (MonadCIO m) => [a] -> (a -> m b) -> m ()
forMConcurrently_ = flip mapMConcurrently_

replicateMConcurrently :: (MonadCIO m) => Int -> m a -> m [a]
replicateMConcurrently n = sequenceConcurrently . replicate n

replicateMConcurrently' :: (MonadCIO m) => Int -> m a -> m [a]
replicateMConcurrently' n = sequenceConcurrently' . replicate n

replicateMConcurrently_ :: (MonadCIO m) => Int -> m a -> m ()
replicateMConcurrently_ n = sequenceConcurrently_ . replicate n

-- |
-- Run the provided side-effecting action on all available threads and 
-- collect the results. The order of results may vary from run to run.
distributeConcurrently :: (MonadCIO m) => m a -> m [a]
distributeConcurrently action = do
  n <- getPoolNumCapabilities
  replicateMConcurrently' n action

-- |
-- Run the provided side-effecting action on all available threads.
distributeConcurrently_ :: (MonadCIO m) => m a -> m ()
distributeConcurrently_ action = do
  n <- getPoolNumCapabilities
  replicateMConcurrently_ n action
