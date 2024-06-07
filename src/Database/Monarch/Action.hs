{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

{-|
Module      : Database.Monarch.Action
Copyright   : 2013 Noriyuki OHKAWA
License     : BSD3

Maintainer  : n.ohkawa@gmail.com
Stability   : experimental
Portability : unknown

TokyoTyrant Original Binary Protocol(<http://fallabs.com/tokyotyrant/spex.html#protocol>).
-}
module Database.Monarch.Action () where

import Control.Monad
import qualified Data.Binary as B
import Data.Binary.Put (putByteString, putWord32be)
import Data.ByteString.Char8 (unpack)
import Data.Int
import Data.Maybe
import Database.Monarch.Types
import Database.Monarch.Utils
import UnliftIO (MonadUnliftIO)
import UnliftIO.Exception (catch, throwIO)


instance (MonadUnliftIO m) => MonadMonarch (MonarchT m) where
    put key value =
        communicate request response
            `catch` \e ->
                case e of
                    SendError orig ->
                        throwIO $ SendError {message = "put[" ++ unpack key ++ "]: " ++ orig}
                    _ ->
                        throwIO e
        where
            request = do
                putMagic 0x10
                mapM_ (putWord32be . lengthBS32) [key, value]
                mapM_ putByteString [key, value]
            response Success = return ()
            response code = throwIO code


    multiplePut [] = return ()
    multiplePut kvs = void $ misc "putlist" [] (kvs >>= \(k, v) -> [k, v])


    putKeep key value =
        communicate request response
            `catch` \e ->
                case e of
                    SendError orig ->
                        throwIO $ SendError {message = "putKeep[" ++ unpack key ++ "]: " ++ orig}
                    _ ->
                        throwIO e
        where
            request = do
                putMagic 0x11
                mapM_ (putWord32be . lengthBS32) [key, value]
                mapM_ putByteString [key, value]
            response Success = return ()
            response InvalidOperation = return ()
            response code = throwIO code


    putCat key value =
        communicate request response
            `catch` \e ->
                case e of
                    SendError orig ->
                        throwIO $ SendError {message = "putCat[" ++ unpack key ++ "]: " ++ orig}
                    _ ->
                        throwIO e
        where
            request = do
                putMagic 0x12
                mapM_ (putWord32be . lengthBS32) [key, value]
                mapM_ putByteString [key, value]
            response Success = return ()
            response code = throwIO code


    putShiftLeft key value width =
        communicate request response
            `catch` \e ->
                case e of
                    SendError orig ->
                        throwIO $ SendError {message = "putShiftLeft[" ++ unpack key ++ "]: " ++ orig}
                    _ ->
                        throwIO e
        where
            request = do
                putMagic 0x13
                mapM_ (putWord32be . lengthBS32) [key, value]
                putWord32be $ fromIntegral width
                mapM_ putByteString [key, value]
            response Success = return ()
            response code = throwIO code


    putNoResponse key value = yieldRequest request
        where
            request = do
                putMagic 0x18
                mapM_ (putWord32be . lengthBS32) [key, value]
                mapM_ putByteString [key, value]


    out key =
        communicate request response
            `catch` \e ->
                case e of
                    SendError orig ->
                        throwIO $ SendError {message = "out[" ++ unpack key ++ "]: " ++ orig}
                    _ ->
                        throwIO e
        where
            request = do
                putMagic 0x20
                putWord32be $ lengthBS32 key
                putByteString key
            response Success = return ()
            response InvalidOperation = return ()
            response code = throwIO code


    multipleOut [] = return ()
    multipleOut keys = void $ misc "outlist" [] keys


    get key =
        communicate request response
            `catch` \e ->
                case e of
                    SendError orig ->
                        throwIO $ SendError {message = "get[" ++ unpack key ++ "]: " ++ orig}
                    _ ->
                        throwIO e
        where
            request = do
                putMagic 0x30
                putWord32be $ lengthBS32 key
                putByteString key
            response Success = Just <$> parseBS
            response InvalidOperation = return Nothing
            response code = throwIO code


    multipleGet keys =
        communicate request response
            `catch` \e ->
                case e of
                    SendError orig ->
                        throwIO $
                            SendError {message = "multipleGet[" ++ concatMap unpack keys ++ "]: " ++ orig}
                    _ ->
                        throwIO e
        where
            request = do
                putMagic 0x31
                putWord32be . fromIntegral $ length keys
                mapM_
                    ( \key -> do
                        putWord32be $ lengthBS32 key
                        putByteString key
                    )
                    keys
            response Success = do
                siz <- fromIntegral <$> parseWord32
                replicateM siz parseKeyValue
            response code = throwIO code


    valueSize key =
        communicate request response
            `catch` \e ->
                case e of
                    SendError orig ->
                        throwIO $ SendError {message = "multipleGet[" ++ unpack key ++ "]: " ++ orig}
                    _ ->
                        throwIO e
        where
            request = do
                putMagic 0x38
                putWord32be $ lengthBS32 key
                putByteString key
            response Success = Just . fromIntegral <$> parseWord32
            response InvalidOperation = return Nothing
            response code = throwIO code


    iterInit =
        communicate request response
            `catch` \e ->
                case e of
                    SendError orig ->
                        throwIO $ SendError {message = "iterInit: " ++ orig}
                    _ ->
                        throwIO e
        where
            request = putMagic 0x50
            response Success = return ()
            response code = throwIO code


    iterNext =
        communicate request response
            `catch` \e ->
                case e of
                    SendError orig ->
                        throwIO $ SendError {message = "iterNext: " ++ orig}
                    _ ->
                        throwIO e
        where
            request = putMagic 0x51
            response Success = Just <$> parseBS
            response InvalidOperation = return Nothing
            response code = throwIO code


    forwardMatchingKeys prefix n =
        communicate request response
            `catch` \e ->
                case e of
                    SendError orig ->
                        throwIO $
                            SendError {message = "forwardMatchingKeys[" ++ unpack prefix ++ "]: " ++ orig}
                    _ ->
                        throwIO e
        where
            request = do
                putMagic 0x58
                putWord32be $ lengthBS32 prefix
                putWord32be $ fromIntegral (fromMaybe (-1) n)
                putByteString prefix
            response Success = do
                siz <- fromIntegral <$> parseWord32
                replicateM siz parseBS
            response code = throwIO code


    addInt key n =
        communicate request response
            `catch` \e ->
                case e of
                    SendError orig ->
                        throwIO $ SendError {message = "addInt[" ++ unpack key ++ "]: " ++ orig}
                    _ ->
                        throwIO e
        where
            request = do
                putMagic 0x60
                putWord32be $ lengthBS32 key
                putWord32be $ fromIntegral n
                putByteString key
            response Success = fromIntegral <$> parseWord32
            response code = throwIO code


    addDouble key n =
        communicate request response
            `catch` \e ->
                case e of
                    SendError orig ->
                        throwIO $ SendError {message = "addDouble[" ++ unpack key ++ "]: " ++ orig}
                    _ ->
                        throwIO e
        where
            request = do
                putMagic 0x61
                putWord32be $ lengthBS32 key
                B.put (truncate n :: Int64)
                B.put (truncate (snd (properFraction n :: (Int, Double)) * 1e12) :: Int64)
                putByteString key
            response Success = parseDouble
            response code = throwIO code


    ext func opts key value =
        communicate request response
            `catch` \e ->
                case e of
                    SendError orig ->
                        throwIO $ SendError {message = "ext[" ++ unpack key ++ "]: " ++ orig}
                    _ ->
                        throwIO e
        where
            request = do
                putMagic 0x68
                putWord32be $ lengthBS32 func
                putOptions opts
                putWord32be $ lengthBS32 key
                putWord32be $ lengthBS32 value
                putByteString func
                putByteString key
                putByteString value
            response Success = parseBS
            response code = throwIO code


    sync =
        communicate request response
            `catch` \e ->
                case e of
                    SendError orig ->
                        throwIO $ SendError {message = "sync: " ++ orig}
                    _ ->
                        throwIO e
        where
            request = putMagic 0x70
            response Success = return ()
            response code = throwIO code


    optimize param =
        communicate request response
            `catch` \e ->
                case e of
                    SendError orig ->
                        throwIO $ SendError {message = "optimize: " ++ orig}
                    _ ->
                        throwIO e
        where
            request = do
                putMagic 0x71
                putWord32be $ lengthBS32 param
                putByteString param
            response Success = return ()
            response code = throwIO code


    vanish =
        communicate request response
            `catch` \e ->
                case e of
                    SendError orig ->
                        throwIO $ SendError {message = "vanish: " ++ orig}
                    _ ->
                        throwIO e
        where
            request = putMagic 0x72
            response Success = return ()
            response code = throwIO code


    copy path =
        communicate request response
            `catch` \e ->
                case e of
                    SendError orig ->
                        throwIO $ SendError {message = "copy[" ++ unpack path ++ ": " ++ orig}
                    _ ->
                        throwIO e
        where
            request = do
                putMagic 0x73
                putWord32be $ lengthBS32 path
                putByteString path
            response Success = return ()
            response code = throwIO code


    restore path usec opts =
        communicate request response
            `catch` \e ->
                case e of
                    SendError orig ->
                        throwIO $ SendError {message = "restore[" ++ unpack path ++ ": " ++ orig}
                    _ ->
                        throwIO e
        where
            request = do
                putMagic 0x74
                putWord32be $ lengthBS32 path
                B.put (fromIntegral usec :: Int64)
                putOptions opts
                putByteString path
            response Success = return ()
            response code = throwIO code


    setMaster host port usec opts =
        communicate request response
            `catch` \e ->
                case e of
                    SendError orig ->
                        throwIO $ SendError {message = "setMaster[" ++ unpack host ++ ": " ++ orig}
                    _ ->
                        throwIO e
        where
            request = do
                putMagic 0x78
                putWord32be $ lengthBS32 host
                putWord32be $ fromIntegral port
                B.put (fromIntegral usec :: Int64)
                putOptions opts
                putByteString host
            response Success = return ()
            response code = throwIO code


    recordNum =
        communicate request response
            `catch` \e ->
                case e of
                    SendError orig ->
                        throwIO $ SendError {message = "recordNum: " ++ orig}
                    _ ->
                        throwIO e
        where
            request = putMagic 0x80
            response Success = parseInt64
            response code = throwIO code


    size =
        communicate request response
            `catch` \e ->
                case e of
                    SendError orig ->
                        throwIO $ SendError {message = "size: " ++ orig}
                    _ ->
                        throwIO e
        where
            request = putMagic 0x81
            response Success = parseInt64
            response code = throwIO code


    status =
        communicate request response
            `catch` \e ->
                case e of
                    SendError orig ->
                        throwIO $ SendError {message = "status: " ++ orig}
                    _ ->
                        throwIO e
        where
            request = putMagic 0x88
            response Success = parseBS
            response code = throwIO code


    misc func opts args =
        communicate request response
            `catch` \e ->
                case e of
                    SendError orig ->
                        throwIO $ SendError {message = "misc: " ++ orig}
                    _ ->
                        throwIO e
        where
            request = do
                putMagic 0x90
                putWord32be $ lengthBS32 func
                putOptions opts
                putWord32be . fromIntegral $ length args
                putByteString func
                mapM_
                    ( \arg -> do
                        putWord32be $ lengthBS32 arg
                        putByteString arg
                    )
                    args
            response Success = do
                siz <- fromIntegral <$> parseWord32
                replicateM siz parseBS
            response code = throwIO code
