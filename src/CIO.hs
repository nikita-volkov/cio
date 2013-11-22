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
runAuto :: CIO r -> IO r
runAuto cio = do
  numCapabilities <- getNumCapabilities
  run numCapabilities cio


class (Monad m) => MonadCIO m where
  -- | Same as @Control.Monad.'Control.Monad.sequence'@, but performs concurrently. 
  sequence :: [m a] -> m [a]
  -- | Same as 'sequence' with a difference that it does not maintain the order of results,
  -- which allows it to execute a bit more effeciently.
  sequenceInterleaved :: [m a] -> m [a]
  -- | Same as @Control.Monad.'Control.Monad.sequence_'@, but performs concurrently. 
  -- Blocks the calling thread until all actions are finished.
  sequence_ :: [m a] -> m ()

instance MonadCIO CIO where
  sequence actions = 
    CIO $ do
      pool <- ask
      lift $ ParallelIO.parallel pool $ map (poolToCIOToIO pool) actions 
    where
      poolToCIOToIO pool (CIO t) = runReaderT t pool
  sequenceInterleaved actions = 
    CIO $ do
      pool <- ask
      lift $ ParallelIO.parallelInterleaved pool $ map (poolToCIOToIO pool) actions 
    where
      poolToCIOToIO pool (CIO t) = runReaderT t pool
  sequence_ actions = 
    CIO $ do
      pool <- ask
      lift $ ParallelIO.parallel_ pool $ map (poolToCIOToIO pool) actions 
    where
      poolToCIOToIO pool (CIO t) = runReaderT t pool

instance (MonadCIO m) => MonadCIO (ReaderT r m) where
  sequence actions = do
    env <- ask
    let cioActions = map (flip runReaderT env) actions
    lift $ sequence cioActions
  sequenceInterleaved actions = do
    env <- ask
    let cioActions = map (flip runReaderT env) actions
    lift $ sequenceInterleaved cioActions
  sequence_ actions = do
    env <- ask
    let cioActions = map (flip runReaderT env) actions
    lift $ sequence_ cioActions

instance (MonadCIO m, Monoid w) => MonadCIO (WriterT w m) where
  sequence actions = do
    let cioActions = map runWriterT actions
    WriterT $ do
      (as, ws) <- return . unzip =<< sequence cioActions
      return (as, mconcat ws)
  sequenceInterleaved actions = do
    let cioActions = map runWriterT actions
    WriterT $ do
      (as, ws) <- return . unzip =<< sequenceInterleaved cioActions
      return (as, mconcat ws)
  sequence_ actions = do
    let cioActions = map execWriterT actions
    WriterT $ do
      ws <- sequenceInterleaved cioActions
      return ((), mconcat ws)


mapM :: (MonadCIO m) => (a -> m b) -> [a] -> m [b]
mapM f = sequence . map f

mapMInterleaved :: (MonadCIO m) => (a -> m b) -> [a] -> m [b]
mapMInterleaved f = sequenceInterleaved . map f

mapM_ :: (MonadCIO m) => (a -> m b) -> [a] -> m ()
mapM_ f = sequence_ . map f

forM :: (MonadCIO m) => [a] -> (a -> m b) -> m [b]
forM = flip mapM

forMInterleaved :: (MonadCIO m) => [a] -> (a -> m b) -> m [b]
forMInterleaved = flip mapMInterleaved

forM_ :: (MonadCIO m) => [a] -> (a -> m b) -> m ()
forM_ = flip mapM_

