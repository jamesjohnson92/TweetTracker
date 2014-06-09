module Main where

import System.Environment
import System.Directory
--import SimulateAlphaPhi hiding (doit)
import Data.Array.Unboxed hiding ((//))
import Data.Array.IO
import Data.Array.Unsafe
import qualified Data.Array  as A
import Data.List
import Data.Function
import Control.Monad
import qualified Data.Map as Map
import System.Random
import Data.Word
import Data.Binary
import qualified Data.ByteString.Lazy as B
import Control.Exception
import Data.Maybe
import qualified Data.HashMap.Strict as HM

--instance Packable Integer where
--  pack i = B.toStrict $ encode i

--instance Unpackable Integer where
--  unpack i = decode $ B.fromStrict  i

--instance Packable Int where
--  pack i = B.toStrict $ encode i

--instance Unpackable Int where
--  unpack i = decode $ B.fromStrict  i


type ParamSelector = (Params -> IOUArray Int Word8, HM.HashMap Integer Int)

type Params = (IOUArray Int Word8, IOUArray Int Word8)

  
num_buckets = 8
              
get_param :: ParamSelector -> Params -> Integer -> IO Double
get_param (f,db) ps s' = do
  let (Just s) = HM.lookup s' db
  bits <- readArray (f ps) s
  return $ (fromIntegral bits) / (fromIntegral num_buckets-1)

set_param :: ParamSelector -> Params -> Integer -> Word8 -> IO ()
set_param (f,db) ps i' b = do
  let (Just i) = HM.lookup i' db
  writeArray (f ps) i b

modifyArray a i f = do
  x <- readArray a i 
  fx <- evaluate $ f x
  writeArray a i fx

a//b = (fromIntegral a)/(fromIntegral b)

update_param :: Double -> [Integer] -> IOUArray Word8 Double -> (Integer -> IO Double) -> (Word8 -> IO a) -> IO a
update_param targ nrts_ix potentials readParam writeParam = do
  forM [0..num_buckets-1] $ (flip (writeArray potentials) 0)
  if not $ null nrts_ix
    then do
    forM nrts_ix $ \i -> do
      x <- readParam i
      forM [0..num_buckets-1] $ \b -> do
        modifyArray potentials b (+(x*(b//(num_buckets -1)))/(1 - x*(b//(num_buckets -1))))
    (b,_) <- fmap (minimumBy (compare `on` snd))
             $ mapM (\b -> fmap (\x -> (b,abs $ x - targ)) $ readArray potentials b) [0..num_buckets-1]
    writeParam b
    else do
    writeParam $ num_buckets - 1
  
update_all_params :: FilePath -> Params -> ParamSelector -> ParamSelector -> IO ()
update_all_params path ps f g = do
--  f <- db
  datapoints <- fmap (map (map read) . map words . lines) $ readFiles path :: IO [[Integer]]
  potentials <- newArray (0,num_buckets-1) 0
  forM_ datapoints $ \(ix:targ:nrts_ix) -> do
    update_param (fromIntegral targ) nrts_ix potentials (get_param g ps) (set_param f ps ix)
--  close_selector f
                                
randomInit :: FilePath -> FilePath -> IO (ParamSelector, ParamSelector,Int,Int,Params)
randomInit alpha_file phi_file = do
  alphas <- fmap (map read . map head . map words . lines) $ readFiles alpha_file :: IO [Integer]
  --cdbMake "alpha.cdb" $ cdbAddMany $ zip alphas [0:: Int ..] 
  let f_alphas = HM.fromList $ zip alphas [0..]
    
  phis <- fmap (map read . map head . map words . lines) $ readFiles phi_file :: IO [Integer]
  --cdbMake "phi.cdb" $ cdbAddMany $ zip phis [0:: Int ..]
  let f_phis = HM.fromList $ zip phis [0..]
  
  let (num_as,num_ps) = (HM.size f_alphas,HM.size f_phis)

  alphas <- newArray (0,num_as -1) 0 :: IO (IOUArray Int Word8)
  phis <- newArray (0,num_ps - 1) 0 :: IO (IOUArray Int Word8)
  
  let ps = (alphas,phis)
      
  forM [0..(num_as - 1)] $ \i -> do
    randomRIO (0,num_buckets-1) >>= (writeArray alphas i)
  forM [0..(num_ps - 1)] $ \i -> do
    randomRIO (0,num_buckets-1) >>= (writeArray phis i)
  return ((fst,f_alphas), (snd,f_phis) ,num_as,num_ps,ps)
  
solveAlphaPhi :: FilePath -> FilePath -> String -> Int -> IO ()
solveAlphaPhi alphapath phipath out_suffix num_its = do
  (f_alpha,f_phi,num_as,num_phis,params) <- randomInit alphapath phipath
  forM_ [1..num_its] $ \i -> do
    putStrLn $ "   iteration = " ++ (show i)
    update_all_params alphapath params f_alpha f_phi
    putStrLn $ "      alphas optimized"
    update_all_params phipath params f_phi f_alpha
    putStrLn $ "      phis optimized"
    
    alphas <- fmap (map read . map head . map words . lines) $ readFiles alphapath :: IO [Integer]
    a_res <- forM alphas (\i -> fmap ((,)i) $ get_param f_alpha params i) :: IO [(Integer, Double)]
    writeFile ("alpha_estimate" ++ out_suffix ++ "_" ++  (show i)) $ unlines $ map (\(a,b) -> (show a) ++ " " ++ (show b)) a_res
  
    phis <- fmap (map read . map head . map words . lines) $ readFiles phipath :: IO [Integer]
    p_res <- forM phis (\i -> fmap ((,)i) $ get_param f_phi params i) :: IO [(Integer, Double)]
    writeFile ("phi_estimate" ++ out_suffix ++ "_" ++ (show i)) $ unlines $ map (\(a,b) -> (show a) ++ " " ++ (show b)) p_res

readFiles path = fmap concat $ do
  fs <- getDirectoryContents path
  forM (filter ((=="part-") . take 5) fs) $ \s -> readFile $ path ++ "/" ++ s
    
doit = solveAlphaPhi "alphafile" "phifile" "" 8

{-
main = do
  [afile,pfile,out] <- getArgs
  solveAlphaPhi afile pfile out 8
  return ()
-}

main = do
  [afile,pfile,out] <- getArgs
  ps <- fmap (Map.fromList . map (\[a,b] -> (read a,read b)). map words . lines) $ readFile pfile
  datapoints <- fmap (map (map read) . map words . lines) $ readFiles afile :: IO [[Integer]]
  potentials <- newArray (0,num_buckets-1) 0
  rs <- forM datapoints $ \(ix:targ:nrts_ix) -> do
    a <- update_param (fromIntegral targ) nrts_ix potentials (\i -> return $ fromJust $ Map.lookup i ps) return
    return (ix,a)
  writeFile out (unlines $ map (\(ix,a) -> (show ix) ++ " " ++ (show a) ++ " ") rs)

  