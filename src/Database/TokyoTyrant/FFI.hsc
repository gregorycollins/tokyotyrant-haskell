{-# LANGUAGE CPP                       #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ForeignFunctionInterface  #-}
{-# LANGUAGE EmptyDataDecls            #-}

{-# OPTIONS_GHC -fno-warn-unused-binds #-}


------------------------------------------------------------------------------
-- | This module provides a thin FFI binding to the libtokyotyrant C library
--   shipped with Mikio Hirabayashi's Tokyo
--   Tyrant. (<http://tokyocabinet.sourceforge.net/tyrantdoc/>)
--
--   It's intended to be imported qualified, e.g.:
--   @
--   import qualified Database.TokyoTyrant.FFI as TT
--   @
------------------------------------------------------------------------------

module Database.TokyoTyrant.FFI
  (
    -- * Opening/closing connections
    open
  , close

    -- * Fetching/storing single values from the store
  , get
  , put
  , putKeep

    -- * Fetching/storing multiple values from the store
  , mget
  , mput

    -- * Deleting keys
  , delete
  , vanish

    -- * Key prefix search
  , fwmkeys

    -- * Types
  , Connection
  ) where

import           Control.Monad.Error
import           Data.List hiding (delete)
import           Foreign.C
import           Foreign.ForeignPtr
import           Foreign.Marshal
import           Foreign.Ptr
import           Foreign.Storable
import           Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B

------------------------------------------------------------------------------
-- | Open a connection to Tokyo Tyrant.
--
open :: ByteString    -- ^ hostname
     -> Int           -- ^ port
     -> IO (Either String Connection)
open h p = do
    runErrorT (open' h p)



------------------------------------------------------------------------------
-- | Close a connection to Tokyo Tyrant
--
close :: Connection -> IO ()
close db_ = withForeignPtr (unConnection db_) $ \db -> do
                        tcrdbclose db >> return ()


------------------------------------------------------------------------------
-- | Get a value from the database
--
get :: Connection     -- ^ connection
    -> ByteString     -- ^ key
    -> IO (Either String (Maybe ByteString))
get db_ key = runErrorT action
  where
    getval :: IO (Maybe ByteString)
    getval = withForeignPtr (unConnection db_) $ \db ->
             alloca $ \(p_sz::Ptr CInt) ->
             B.useAsCStringLen key $ \(ckey,keylen) -> do
               cval <- tcrdbget db ckey (toEnum keylen) p_sz
               if cval == nullPtr then return Nothing
                 else do
                   sz  <- peek p_sz
                   rbs <- B.packCStringLen (cval, fromEnum sz)
                   free cval
                   return $ Just rbs


    action :: ErrorT String IO (Maybe ByteString)
    action = do
        mb <- liftIO getval

        case mb of
          Just _  -> return mb
          Nothing -> ErrorT maybeFail


    maybeFail :: IO (Either String (Maybe ByteString))
    maybeFail = do
      withForeignPtr (unConnection db_) $ \db -> do
        ecode <- tcrdbecode db >>= return . TConstant
        if ecode == errNoRec then
            return $ Right Nothing
          else do
            cerr <- tcrdberrmsg (unTConstant ecode)
            peekCString cerr >>= return . Left


------------------------------------------------------------------------------
-- | Store a value in the database (destructive, overwrites any existing
--   value)
--
put :: Connection     -- ^ connection
    -> ByteString     -- ^ key
    -> ByteString     -- ^ value
    -> IO (Either String ())
put (Connection db_) key value = do
    B.useAsCStringLen key   $ \(ckey,keylen)   ->
      B.useAsCStringLen value $ \(cvalue,vallen) ->
      withForeignPtr db_      $ \db              -> do
        rval <- tcrdbput db ckey (toEnum keylen) cvalue (toEnum vallen)
        checkErr db rval


------------------------------------------------------------------------------
-- | Store a value in the database (non-destructive, does nothing if the key
--   already exists)
--
putKeep :: Connection   -- ^ connection
        -> ByteString   -- ^ key
        -> ByteString   -- ^ value
        -> IO (Either String ())
putKeep (Connection db_) key value = do
    B.useAsCStringLen key   $ \(ckey,keylen)     ->
      B.useAsCStringLen value $ \(cvalue,vallen) ->
      withForeignPtr db_      $ \db              -> do
        rval <- tcrdbputkeep db ckey (toEnum keylen) cvalue (toEnum vallen)
        checkErr db rval


------------------------------------------------------------------------------
-- | Get multiple values from the database. On success, returns `Right kvps`.
--
mget :: Connection    -- ^ connection to DB
     -> [ByteString]  -- ^ list of keys to fetch
     -> IO (Either String [(ByteString,ByteString)])
mget (Connection db_) keys = withForeignPtr db_ $ \db -> do
    lst <- bsListToTCList keys
    res <- B.useAsCString "getlist" $ \s -> tcrdbmisc db s 0 lst
    tclistdel lst
    if res == nullPtr
       then checkErr' [] db 0
       else do
         newlst <- tclistToBSList res
         return . Right $ uninterleave newlst []
  where
    uninterleave (a:b:xs) l = uninterleave xs ((a,b):l)
    uninterleave _        l = reverse l


------------------------------------------------------------------------------
-- | Put multiple values to the database.
--
mput :: Connection                  -- ^ connection to DB
     -> [(ByteString, ByteString)]  -- ^ list of key-value pairs
     -> IO (Either String ())
mput (Connection db_) kvps = withForeignPtr db_ $ \db -> do
    lst <- bsListToTCList $ interleave kvps
    res <- B.useAsCString "putlist" $ \s -> tcrdbmisc db s 0 lst
    tclistdel lst
    if res == nullPtr
       then checkErr db 0
       else return $ Right ()
  where
    interleave = concatMap (\(k,v) -> [k,v])


------------------------------------------------------------------------------
-- | Delete a value from the DB
--
delete :: Connection    -- ^ connection
       -> ByteString    -- ^ key
       -> IO (Either String ())
delete (Connection db_) key = do
    B.useAsCStringLen key   $ \(ckey,keylen)   ->
      withForeignPtr db_      $ \db            -> do
        rval <- tcrdbout db ckey (toEnum keylen)
        checkErr db rval


------------------------------------------------------------------------------
-- | Delete all KVPs in the database.
vanish :: Connection -> IO (Either String ())
vanish (Connection db_) = withForeignPtr db_ $ \db -> do
                       ret <- tcrdbvanish db
                       checkErr db ret


------------------------------------------------------------------------------
-- | Search keys by prefix. Returns a list of matching keys.
--
fwmkeys :: Connection           -- ^ connection to DB
        -> ByteString           -- ^ key prefix
        -> Int                  -- ^ max # of returned keys; negative numbers
                                --   mean "no limit"
        -> IO (Either String [ByteString])
fwmkeys (Connection db_) key limit =
    B.useAsCStringLen key   $ \(ckey,keylen) ->
        withForeignPtr db_  $ \db -> do
          lst <- tcrdbfwmkeys db ckey (toEnum keylen) (toEnum (limit))
          if lst == nullPtr
            then checkErr' [] db 0
            else tclistToBSList lst >>= return . Right



------------------------------------------------------------------------------
-- | A Tokyo Tyrant connection type. Wraps the `TCRDB` type from the C
--   library.
newtype Connection = Connection { unConnection :: ForeignPtr () }


------------------------------------------------------------------------------
-- utility functions
------------------------------------------------------------------------------

open' :: ByteString -> Int -> ErrorT String IO Connection
open' host port = do
    db <- liftIO $ tcrdbnew

    if db == nullPtr then throwError "couldn't allocate DB object"
                     else return ()

    result <- liftIO $ B.useAsCString host
                     $ \chost -> tcrdbopen db chost (toEnum port)

    db' <- liftIO $ newForeignPtr p_tcrdbdel db
    ErrorT $ checkErr db result

    return $ Connection db'


------------------------------------------------------------------------------
tclistToBSList :: TCLIST -> IO [ByteString]
tclistToBSList lst = do
    n <- tclistnum lst
    l <- f 0 n []
    tclistdel lst
    return l

  where
    getN i = alloca $ \(p_sz :: Ptr CInt) -> do
                 cstr <- tclistval lst i p_sz
                 if cstr == nullPtr then
                     return B.empty
                   else do
                     sz <- peek p_sz
                     B.packCStringLen (cstr, fromEnum sz)


    f i n l | i >= n    = return $ reverse l
            | otherwise = do
                            bs <- getN i
                            f (i+1) n (bs:l)


------------------------------------------------------------------------------
bsListToTCList :: [ByteString] -> IO TCLIST
bsListToTCList strs = do
    lst <- tclistnew2 . toEnum $ length strs
    mapM_ (doOne lst) strs
    return lst

  where
    doOne lst s =
      B.useAsCStringLen s $ \(cstr,len) -> do
        tclistpush lst cstr (toEnum len)


------------------------------------------------------------------------------
checkErr :: ConnectionPtr -> CBool -> IO (Either String ())
checkErr db res =
    if res == 0
      then do
        ecode <- liftIO $ tcrdbecode db
        cerr  <- liftIO $ tcrdberrmsg ecode
        str   <- liftIO $ peekCString cerr
        return $ Left str
      else
        return $ Right ()


------------------------------------------------------------------------------
checkErr' :: a -> ConnectionPtr -> CBool -> IO (Either String a)
checkErr' v db res =
    if res == 0
      then do
        ecode <- liftIO $ tcrdbecode db
        cerr  <- liftIO $ tcrdberrmsg ecode
        str   <- liftIO $ peekCString cerr
        return $ Left str
      else
        return $ Right v



------------------------------------------------------------------------------
-- Types
------------------------------------------------------------------------------
type ConnectionPtr = Ptr ()
type CBool    = CInt
type TCLIST   = Ptr ()

newtype TConstant = TConstant { unTConstant :: CInt }
  deriving (Eq, Show)

#include <tcrdb.h>
#{enum TConstant, TConstant
 , errSuccess = TTESUCCESS
 , errInvalid = TTEINVALID
 , errNoHost  = TTENOHOST
 , errRefused = TTEREFUSED
 , errSend    = TTESEND
 , errRecv    = TTERECV
 , errKeep    = TTEKEEP
 , errNoRec   = TTENOREC
 , errMisc    = TTEMISC }


------------------------------------------------------------------------------
-- FFI: list stuff (from libtokyocabinet)
------------------------------------------------------------------------------

foreign import ccall unsafe "tcutil.h tclistdel"
        tclistdel :: TCLIST -> IO ()

foreign import ccall unsafe "tcutil.h tclistnew2"
        tclistnew2 :: CInt -> IO TCLIST

foreign import ccall unsafe "tcutil.h tclistpush"
        tclistpush :: TCLIST -> CString -> CInt -> IO ()

foreign import ccall unsafe "tcutil.h tclistnum"
        tclistnum :: TCLIST -> IO CInt

foreign import ccall unsafe "tcutil.h tclistval"
        tclistval :: TCLIST -> CInt -> Ptr CInt -> IO CString


------------------------------------------------------------------------------
-- FFI: tyrant stuff (libtokyotyrant)
------------------------------------------------------------------------------

foreign import ccall unsafe "tcrdb.h tcrdberrmsg"
        tcrdberrmsg :: CInt -> IO CString

foreign import ccall unsafe "tcrdb.h tcrdbnew"
        tcrdbnew :: IO ConnectionPtr

foreign import ccall unsafe "tcrdb.h &tcrdbdel"
        p_tcrdbdel :: FunPtr (ConnectionPtr -> IO ())

foreign import ccall unsafe "tcrdb.h tcrdbecode"
        tcrdbecode :: ConnectionPtr -> IO CInt

foreign import ccall unsafe "tcrdb.h tcrdbopen"
        tcrdbopen :: ConnectionPtr -> CString -> CInt -> IO CBool

foreign import ccall unsafe "tcrdb.h tcrdbclose"
        tcrdbclose :: ConnectionPtr -> IO CBool

foreign import ccall unsafe "tcrdb.h tcrdbput"
        tcrdbput :: ConnectionPtr -> CString -> CInt -> CString -> CInt -> IO CBool

foreign import ccall unsafe "tcrdb.h tcrdbputkeep"
        tcrdbputkeep :: ConnectionPtr -> CString -> CInt -> CString -> CInt -> IO CBool

foreign import ccall unsafe "tcrdb.h tcrdbout"
        tcrdbout :: ConnectionPtr -> CString -> CInt -> IO CBool

foreign import ccall unsafe "tcrdb.h tcrdbget"
        tcrdbget :: ConnectionPtr -> CString -> CInt -> Ptr CInt -> IO CString

foreign import ccall unsafe "tcrdb.h tcrdbfwmkeys"
        tcrdbfwmkeys :: ConnectionPtr -> CString -> CInt -> CInt -> IO TCLIST

foreign import ccall unsafe "tcrdb.h tcrdbvanish"
        tcrdbvanish :: ConnectionPtr -> IO CBool

foreign import ccall unsafe "tcrdb.h tcrdbmisc"
        tcrdbmisc :: ConnectionPtr -> CString -> CInt -> TCLIST -> IO TCLIST
