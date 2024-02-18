{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- |
-- Module      : Database.Monarch.Mock.Types
-- Copyright   : 2013 Noriyuki OHKAWA
-- License     : BSD3
--
-- Maintainer  : n.ohkawa@gmail.com
-- Stability   : experimental
-- Portability : unknown
--
-- Type definitions.
module Database.Monarch.Mock.Types
  ( MockT,
    MockDB,
    mockDB,
    newMockDB,
    emptyMockDB,
    runMock,
    TTValue (..),
  )
where

import Control.Concurrent.STM.TVar
import Control.Monad.Except
  ( ExceptT (..),
    MonadError,
    runExceptT,
  )
import Control.Monad.Reader
import qualified Data.ByteString as BS
import qualified Data.Map as M
import Database.Monarch.Types (Code)

-- | KVS Value type
data TTValue
  = TTString BS.ByteString
  | TTInt Int
  | TTDouble Double

-- | Connection with TokyoTyrant
newtype MockDB = MockDB
  { -- | DB
    mockDB :: M.Map BS.ByteString TTValue
  }

-- | The Mock monad transformer to provide TokyoTyrant access.
newtype MockT m a = MockT {unMockT :: ExceptT Code (ReaderT (TVar MockDB) m) a}
  deriving
    ( Functor,
      Applicative,
      Monad,
      MonadIO,
      MonadReader (TVar MockDB),
      MonadError Code
    )

-- | Empty mock DB
emptyMockDB :: MockDB
emptyMockDB = MockDB {mockDB = M.empty}

-- | Create mock DB
newMockDB :: IO (TVar MockDB)
newMockDB = newTVarIO emptyMockDB

-- | Run Mock with TokyoTyrant at target host and port.
runMock ::
  (MonadIO m) =>
  MockT m a ->
  TVar MockDB ->
  m (Either Code a)
runMock action =
  runReaderT (runExceptT $ unMockT action)
