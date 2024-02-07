{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

-- |
-- Module      : Database.Monarch.Mock.Types
-- Copyright   : 2013 Noriyuki OHKAWA
-- License     : BSD3
--
-- Maintainer  : n.ohkawa@gmail.com
-- Stability   : experimental
-- Portability : unknown
--
-- Mock actions.
module Database.Monarch.Mock.Action () where

import Control.Concurrent.STM.TVar
import Control.Monad.Reader
import Control.Monad.STM (atomically)
import Control.Monad.Trans.Control
import qualified Data.ByteString as BS
import Data.Map ((!))
import qualified Data.Map as M
import Database.Monarch.Mock.Types (MockDB, MockT, TTValue (..), emptyMockDB, mockDB)
import Database.Monarch.Types (MonadMonarch (..))

putDB :: BS.ByteString -> TTValue -> MockDB -> MockDB
putDB key value db = db {mockDB = M.insert key value (mockDB db)}

putDBS :: BS.ByteString -> BS.ByteString -> MockDB -> MockDB
putDBS key value = putDB key (TTString value)

putDBI :: BS.ByteString -> Int -> MockDB -> MockDB
putDBI key value = putDB key (TTInt value)

putDBD :: BS.ByteString -> Double -> MockDB -> MockDB
putDBD key value = putDB key (TTDouble value)

getDB :: BS.ByteString -> MockDB -> Maybe BS.ByteString
getDB key db =
  M.lookup key (mockDB db) >>= \mvalue ->
    case mvalue of
      TTString value -> return value
      _ -> error "get"

instance (MonadBaseControl IO m, MonadIO m) => MonadMonarch (MockT m) where
  put key value = do
    tdb <- ask
    liftIO $ atomically $ modifyTVar tdb $ putDBS key value

  multiplePut = mapM_ (uncurry put)

  putKeep key value = do
    tdb <- ask
    let modify db
          | M.member key (mockDB db) = db
          | otherwise = putDBS key value db
    liftIO $ atomically $ modifyTVar tdb modify

  putCat key value = do
    tdb <- ask
    let modify db
          | M.member key (mockDB db) =
              case mockDB db ! key of
                TTString v -> putDBS key (v <> value) db
                _ -> error "putCat"
          | otherwise =
              putDBS key value db
    liftIO $ atomically $ modifyTVar tdb modify

  putShiftLeft key value width = do
    tdb <- ask
    let modify db
          | M.member key (mockDB db) =
              case mockDB db ! key of
                TTString v -> putDBS key (BS.drop (BS.length (v <> value) - width) $ v <> value) db
                _ -> error "putShiftLeft"
          | otherwise =
              putDBS key value db
    liftIO $ atomically $ modifyTVar tdb modify

  putNoResponse = put

  out key = do
    tdb <- ask
    let modify db = db {mockDB = M.delete key (mockDB db)}
    liftIO $ atomically $ modifyTVar tdb modify

  multipleOut = mapM_ out

  get key = do
    tdb <- ask
    liftIO $ atomically $ fmap (getDB key) $ readTVar tdb

  multipleGet keys = do
    vs <- mapM (\k -> fmap (\v -> (k, v)) $ get k) keys
    return [(k, v) | (k, Just v) <- vs]

  valueSize = fmap (fmap BS.length) . get

  iterInit = return ()

  iterNext = error "not implemented"

  forwardMatchingKeys prefix n = do
    tdb <- ask
    let readKeys db = filter (BS.isPrefixOf prefix) $ M.keys $ mockDB db
    ks <- liftIO $ atomically $ fmap readKeys $ readTVar tdb
    case n of
      Nothing -> return ks
      Just x -> return $ take x ks

  addInt key n = do
    tdb <- ask
    let modify db
          | M.member key (mockDB db) =
              case mockDB db ! key of
                TTInt x -> putDBI key (x + n) db
                _ -> error "addInt"
          | otherwise =
              putDBI key n db
    let readDouble db = case mockDB db ! key of
          TTInt x -> x
          _ -> error "addInt"
    liftIO $ atomically $ modifyTVar tdb modify >> fmap readDouble (readTVar tdb)

  addDouble key n = do
    tdb <- ask
    let modify db
          | M.member key (mockDB db) =
              case mockDB db ! key of
                TTDouble x -> putDBD key (x + n) db
                _ -> error "addDouble"
          | otherwise =
              putDBD key n db
    let readDouble db = case mockDB db ! key of
          TTDouble x -> x
          _ -> error "addDouble"
    liftIO $ atomically $ modifyTVar tdb modify >> fmap readDouble (readTVar tdb)

  ext _func _opts _key _value = error "not implemented"

  sync = error "not implemented"

  optimize _param = return ()

  vanish = do
    tdb <- ask
    liftIO $ atomically $ modifyTVar tdb $ const emptyMockDB

  copy _path = error "not implemented"

  restore _path _usec _opts = error "not implemented"

  setMaster _host _port _usec _opts = error "not implemented"

  recordNum = do
    tdb <- ask
    liftIO $ atomically $ fmap (toEnum . M.size . mockDB) $ readTVar tdb

  size = error "not implemented"

  status = error "not implemented"

  misc _func _opts _args = error "not implemented"
