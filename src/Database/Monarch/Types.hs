{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

{-|
Module      : Database.Monarch.Types
Copyright   : 2013 Noriyuki OHKAWA
License     : BSD3

Maintainer  : n.ohkawa@gmail.com
Stability   : experimental
Portability : unknown

Type definitions.
-}
module Database.Monarch.Types
    ( Monarch
    , MonarchT
    , Connection
    , ConnectionPool
    , withMonarchConn
    , withMonarchPool
    , runMonarchConn
    , runMonarchPool
    , ExtOption (..)
    , RestoreOption (..)
    , MiscOption (..)
    , Code (..)
    , sendLBS
    , recvLBS
    , MonadMonarch (..)
    )
where

import Control.Monad.Reader (MonadReader, ReaderT (..), asks)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import Data.Function
import Data.Int (Int64)
import Data.Pool
    ( Pool
    , defaultPoolConfig
    , destroyResource
    , newPool
    , putResource
    , takeResource
    )
import Network.Socket
    ( AddrInfo (..)
    , AddrInfoFlag (..)
    , HostName
    , Socket
    , SocketType (..)
    , close
    , connect
    , defaultHints
    , getAddrInfo
    , socket
    )
import qualified Network.Socket.ByteString.Lazy as LBS
import UnliftIO (MonadIO (liftIO), MonadUnliftIO (withRunInIO))
import UnliftIO.Exception
    ( Exception
    , SomeException
    , bracket
    , catchAny
    , mask
    , onException
    , throwIO
    )
import Prelude


-- | Connection with TokyoTyrant
newtype Connection = Connection {connection :: Socket}


-- | Connection pool with TokyoTyrant
type ConnectionPool = Pool Connection


-- | Error code
data Code
    = Success
    | InvalidOperation
    | HostNotFound
    | ConnectionRefused
    | SendError
        { request :: LBS.ByteString
        , message :: String
        }
    | ReceiveError
    | ExistingRecord
    | NoRecordFound
    | MiscellaneousError
    deriving (Prelude.Eq, Prelude.Show)


instance Exception Code


-- | Options for scripting extension
data ExtOption
    = -- | record locking
      RecordLocking
    | -- | global locking
      GlobalLocking


-- | Options for restore
data RestoreOption
    = -- | consistency checking
      ConsistencyChecking


-- | Options for miscellaneous operation
data MiscOption
    = -- | omission of update log
      NoUpdateLog


-- | The Monarch monad transformer to provide TokyoTyrant access.
newtype MonarchT m a = MonarchT {unMonarchT :: ReaderT Connection m a}
    deriving
        ( Prelude.Functor
        , Prelude.Applicative
        , Prelude.Monad
        , MonadIO
        , MonadReader Connection
        )


instance (MonadUnliftIO m) => MonadUnliftIO (MonarchT m) where
    withRunInIO inner = MonarchT $ withRunInIO $ \run -> inner (run . unMonarchT)
    {-# INLINE withRunInIO #-}


-- | IO Specialized
type Monarch = MonarchT Prelude.IO


-- | Run Monarch with TokyoTyrant at target host and port.
runMonarch
    :: ( MonadUnliftIO m
       )
    => Connection
    -> MonarchT m a
    -> m a
runMonarch conn action =
    runReaderT (unMonarchT action) conn


{-| Create a TokyoTyrant connection and run the given action.
Don't use the given 'Connection' outside the action.
-}
withMonarchConn
    :: ( MonadUnliftIO m
       )
    => Prelude.String
    -- ^ host
    -> Prelude.Int
    -- ^ port
    -> (Connection -> m a)
    -> m a
withMonarchConn host port = bracket open' close_
    where
        open' = liftIO Prelude.$ getConnection host port
        close_ = liftIO Prelude.. close Prelude.. connection


{-| Create a TokyoTyrant connection pool and run the given action.
Don't use the given 'ConnectionPool' outside the action.
-}
withMonarchPool
    :: ( MonadIO m
       )
    => Prelude.String
    -- ^ host
    -> Prelude.Int
    -- ^ port
    -> Prelude.Int
    -- ^ number of connections
    -> (ConnectionPool -> m a)
    -> m a
withMonarchPool host port connections f =
    liftIO
        ( defaultPoolConfig open' close_ 20 connections
            & newPool
        )
        Prelude.>>= f
    where
        open' = getConnection host port
        close_ = close Prelude.. connection


-- | Run action with a connection.
runMonarchConn
    :: ( MonadUnliftIO m
       )
    => MonarchT m a
    -- ^ action
    -> Connection
    -- ^ connection
    -> m a
runMonarchConn action conn = runMonarch conn action


-- | Run action with a unused connection from the pool.
runMonarchPool
    :: ( MonadUnliftIO m
       )
    => MonarchT m a
    -- ^ action
    -> ConnectionPool
    -- ^ connection pool
    -> m a
runMonarchPool action pool = mask $ \unmask -> do
    (res, localPool) <- liftIO $ takeResource pool
    r <-
        unmask (runMonarch res action)
            `onException` liftIO (destroyResource pool localPool res)
    liftIO $ putResource localPool res
    pure r


throwError'
    :: (MonadIO m)
    => Code
    -> SomeException
    -> MonarchT m a
throwError' = Prelude.const Prelude.. throwIO


-- | Send.
sendLBS
    :: ( MonadUnliftIO m
       )
    => LBS.ByteString
    -> MonarchT m ()
sendLBS lbs = do
    conn <- asks connection
    liftIO (LBS.sendAll conn lbs) `catchAny` \e -> do
        throwIO $
            SendError
                { request = lbs
                , message = show e
                }


-- | Receive.
recvLBS
    :: ( MonadUnliftIO m
       )
    => Int64
    -> MonarchT m LBS.ByteString
recvLBS n = do
    conn <- asks connection
    lbs <- liftIO (LBS.recv conn n) `catchAny` throwError' ReceiveError
    if LBS.null lbs
        then throwIO ReceiveError
        else
            if n Prelude.== LBS.length lbs
                then Prelude.return lbs
                else LBS.append lbs Prelude.<$> recvLBS (n Prelude.- LBS.length lbs)


-- | Make connection from host and port.
getConnection
    :: HostName
    -> Prelude.Int
    -> Prelude.IO Connection
getConnection host port = do
    let
        hints =
            defaultHints
                { addrFlags = [AI_ADDRCONFIG]
                , addrSocketType = Stream
                }
    (addr : _) <-
        getAddrInfo
            (Prelude.Just hints)
            (Prelude.Just host)
            (Prelude.Just Prelude.$ Prelude.show port)
    sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
    let
        failConnect = (\e -> close sock Prelude.>> throwIO e) :: SomeException -> Prelude.IO ()
    connect sock (addrAddress addr) `catchAny` failConnect
    Prelude.return Prelude.$ Connection sock


-- | Monad Monarch interfaces
class (Prelude.Monad m) => MonadMonarch m where
    -- | Store a record.
    --   If a record with the same key exists in the database,
    --   it is overwritten.
    put
        :: BS.ByteString
        -- ^ key
        -> BS.ByteString
        -- ^ value
        -> m ()


    -- | Store records.
    --   If a record with the same key exists in the database,
    --   it is overwritten.
    multiplePut
        :: [(BS.ByteString, BS.ByteString)]
        -- ^ key & value pairs
        -> m ()


    -- | Store a new record.
    --   If a record with the same key exists in the database,
    --   this function has no effect.
    putKeep
        :: BS.ByteString
        -- ^ key
        -> BS.ByteString
        -- ^ value
        -> m ()


    -- | Concatenate a value at the end of the existing record.
    --   If there is no corresponding record, a new record is created.
    putCat
        :: BS.ByteString
        -- ^ key
        -> BS.ByteString
        -- ^ value
        -> m ()


    -- | Concatenate a value at the end of the existing record and shift it to the left.
    --   If there is no corresponding record, a new record is created.
    putShiftLeft
        :: BS.ByteString
        -- ^ key
        -> BS.ByteString
        -- ^ value
        -> Prelude.Int
        -- ^ width
        -> m ()


    -- | Store a record without response.
    --   If a record with the same key exists in the database, it is overwritten.
    putNoResponse
        :: BS.ByteString
        -- ^ key
        -> BS.ByteString
        -- ^ value
        -> m ()


    -- | Remove a record.
    out
        :: BS.ByteString
        -- ^ key
        -> m ()


    -- | Remove records.
    multipleOut
        :: [BS.ByteString]
        -- ^ keys
        -> m ()


    -- | Retrieve a record.
    get
        :: BS.ByteString
        -- ^ key
        -> m (Prelude.Maybe BS.ByteString)


    -- | Retrieve records.
    multipleGet
        :: [BS.ByteString]
        -- ^ keys
        -> m [(BS.ByteString, BS.ByteString)]


    -- | Get the size of the value of a record.
    valueSize
        :: BS.ByteString
        -- ^ key
        -> m (Prelude.Maybe Prelude.Int)


    -- | Initialize the iterator.
    iterInit :: m ()


    -- | Get the next key of the iterator.
    --   The iterator can be updated by multiple connections and then it is not assured that every record is traversed.
    iterNext :: m (Prelude.Maybe BS.ByteString)


    -- | Get forward matching keys.
    forwardMatchingKeys
        :: BS.ByteString
        -- ^ key prefix
        -> Prelude.Maybe Prelude.Int
        -- ^ maximum number of keys to be fetched. 'Prelude.Nothing' means unlimited.
        -> m [BS.ByteString]


    -- | Add an integer to a record.
    --   If the corresponding record exists, the value is treated as an integer and is added to.
    --   If no record corresponds, a new record of the additional value is stored.
    addInt
        :: BS.ByteString
        -- ^ key
        -> Prelude.Int
        -- ^ value
        -> m Prelude.Int


    -- | Add a real number to a record.
    --   If the corresponding record exists, the value is treated as a real number and is added to.
    --   If no record corresponds, a new record of the additional value is stored.
    addDouble
        :: BS.ByteString
        -- ^ key
        -> Prelude.Double
        -- ^ value
        -> m Prelude.Double


    -- | Call a function of the script language extension.
    ext
        :: BS.ByteString
        -- ^ function
        -> [ExtOption]
        -- ^ option flags
        -> BS.ByteString
        -- ^ key
        -> BS.ByteString
        -- ^ value
        -> m BS.ByteString


    -- | Synchronize updated contents with the file and the device.
    sync :: m ()


    -- | Optimize the storage.
    optimize
        :: BS.ByteString
        -- ^ parameter
        -> m ()


    -- | Remove all records.
    vanish :: m ()


    -- | Copy the database file.
    copy
        :: BS.ByteString
        -- ^ path
        -> m ()


    -- | Restore the database file from the update log.
    restore
        :: (Prelude.Integral a)
        => BS.ByteString
        -- ^ path
        -> a
        -- ^ beginning time stamp in microseconds
        -> [RestoreOption]
        -- ^ option flags
        -> m ()


    -- | Set the replication master.
    setMaster
        :: (Prelude.Integral a)
        => BS.ByteString
        -- ^ host
        -> Prelude.Int
        -- ^ port
        -> a
        -- ^ beginning time stamp in microseconds
        -> [RestoreOption]
        -- ^ option flags
        -> m ()


    -- | Get the number of records.
    recordNum :: m Int64


    -- | Get the size of the database.
    size :: m Int64


    -- | Get the status string of the database.
    status :: m BS.ByteString


    -- | Call a versatile function for miscellaneous operations.
    misc
        :: BS.ByteString
        -- ^ function name
        -> [MiscOption]
        -- ^ option flags
        -> [BS.ByteString]
        -- ^ arguments
        -> m [BS.ByteString]
