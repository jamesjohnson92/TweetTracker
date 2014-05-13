module SolveAlphaPhi where

import SimulateAlphaPhi hiding (doit)
import Data.Array.Unboxed hiding ((//))
import Data.Array.IO
import Data.Array.Unsafe
import qualified Data.Array  as A
import Data.Tuple.HT
import Data.List
import Data.Function
import Control.Monad
import qualified Data.Map as Map
import System.Random
import Data.Word
import Database.PureCDB
import Data.Binary
import qualified Data.ByteString.Lazy as B

type ParamSelector = (Params -> IOUArray Int Word8, ReadCDB)

type Params = (IOUArray Int Word8, IOUArray Int Word8)

get_alpha_selector = do
  f <- openCDB "alpha.cbd"
  return (fst,f)
get_phi_selector = do
  f <- openCDB "phi.cbd"
  return (snd,f)
close_selector (_,f) = closeCDB f
  
num_buckets = 16
              
get_param :: ParamSelector -> Params -> Integer -> IO Double
get_param (f,db) ps s' = do
  [s] <- fmap (map $ decode. B.fromStrict ) $ getBS db $ B.toStrict $ encode s'
  bits <- readArray (f ps) (s`div`2)
  let m_bits = if s`mod`2 == 0 then bits`mod`num_buckets else bits`div`num_buckets
  return $ (fromIntegral m_bits) / (fromIntegral num_buckets-1)

set_param :: ParamSelector -> Params -> Integer -> Word8 -> IO ()
set_param (f,db) ps i' b = do
  [i] <- fmap (map $ decode . B.fromStrict) $ getBS db $ B.toStrict $ encode i'
  let m_bits = if i`mod`2 == 0 then b else b * num_buckets
  writeArray (f ps) i m_bits

modifyArray a i f = (readArray a i) >>= (writeArray a i . f)

a//b = (fromIntegral a)/(fromIntegral b)

update_param :: Double -> [Integer] -> IOUArray Word8 Double -> (Integer -> IO Double) -> (Word8 -> IO ()) -> IO ()
update_param targ nrts_ix potentials readParam writeParam = do
  forM [0..num_buckets-1] $ (flip (writeArray potentials) 0)
  forM nrts_ix $ \i -> do
    x <- readParam i
    forM [0..num_buckets-1] $ \b -> do
      modifyArray potentials b (+(log $ 1 - x*(b//(num_buckets -1))))
  (b,_) <- fmap (minimumBy (compare `on` snd))
           $ mapM (\b -> fmap ((,) b . abs . (targ - )) $ readArray potentials b) [1..num_buckets-1]
  writeParam b
  
update_all_params :: FilePath -> Params -> IO ParamSelector -> IO ()
update_all_params path ps db = do
  f <- db
  datapoints <- fmap (map (map read) . map words . lines) $ readFile path
  potentials <- newArray (0,num_buckets-1) 0
  forM_ datapoints $ \(ix:targ:nrts_ix) -> 
    update_param (fromIntegral targ) nrts_ix potentials (get_param f ps) (set_param f ps ix)
  close_selector f
                                
randomInit :: FilePath -> FilePath -> IO (Int,Int,Params)
randomInit alpha_file phi_file = do
  num_as <- fmap length $ readFile alpha_file
  num_ps <- fmap length $ readFile phi_file
  
  alphas <- fmap (map head . map words . lines) $ readFile alpha_file
  flip makeCDB "alpha.cdb" $ do  
    forM_ (zip [0:: Int ..] alphas) $ \(i,a) -> addBS (B.toStrict $ encode $ (read a :: Integer)) (B.toStrict $ encode i)
    
  phis <- fmap (head . map words . lines) $ readFile phi_file
  flip makeCDB "phi.cdb" $ do
    forM_ (zip [0:: Int ..] phis ) $ \(i,a) -> addBS (B.toStrict $ encode $ (read a :: Integer)) (B.toStrict $ encode i)

  alphas <- newArray (0,(num_as`div`2) + (num_as`mod`2) - 1) 0 :: IO (IOUArray Int Word8)
  phis <- newArray (0,(num_ps`div`2) + (num_ps`mod`2) - 1) 0 :: IO (IOUArray Int Word8)
  
  let ps = (alphas,phis)
      
  forM [0..(num_as`div`2) + (num_as`mod`2) - 1] $ \i -> do
    randomIO >>= (writeArray alphas i)
  forM [0..(num_ps`div`2) + (num_ps`mod`2) - 1] $ \i -> do
    randomIO >>= (writeArray phis i)
  return (num_as,num_ps,ps)
  
solveAlphaPhi :: Int -> Int -> FilePath -> FilePath -> Int -> IO ()
solveAlphaPhi num_as num_phis alphapath phipath num_its = do
  (num_as,num_phis,params) <- randomInit alphapath phipath
  forM [1..num_its] $ \i -> do
    putStrLn $ "   iteration = " ++ (show i)
    update_all_params alphapath params get_alpha_selector
    putStrLn $ "      alphas optimized"
    update_all_params alphapath params get_phi_selector
    putStrLn $ "      phis optimized"
    
  f_alpha <- get_alpha_selector
  alphas <- fmap (map read . map head . map words . lines) $ readFile alphapath :: IO [Integer]
  a_res <- forM alphas (\i -> fmap ((,)i) $ get_param f_alpha params i) :: IO [(Integer, Double)]
  close_selector f_alpha
  writeFile "alpha_estimate" $ unlines $ map (\(a,b) -> (show a) ++ " " ++ (show b)) a_res
  
  f_phi <- get_phi_selector
  phis <- fmap (map read . map head . map words . lines) $ readFile phipath :: IO [Integer]
  p_res <- forM phis (\i -> fmap ((,)i) $ get_param f_phi params i) :: IO [(Integer, Double)]
  close_selector f_phi
  writeFile "phi_estimate" $ unlines $ map (\(a,b) -> (show a) ++ " " ++ (show b)) p_res
  
  