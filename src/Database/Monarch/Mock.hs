-- |
-- Module      : Database.Monarch.Mock
-- Copyright   : 2013 Noriyuki OHKAWA
-- License     : BSD3
--
-- Maintainer  : n.ohkawa@gmail.com
-- Stability   : experimental
-- Portability : unknown
--
-- Provide TokyoTyrant mock.
module Database.Monarch.Mock
  ( MockT,
    MockDB,
    newMockDB,
    runMock,
  )
where

import Database.Monarch.Mock.Action ()
import Database.Monarch.Mock.Types
