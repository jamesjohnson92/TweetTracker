module SolveAlphaPhi where

import SimulateAlphaPhi hiding (doit)
import Data.Array.Unboxed
import Data.Array.IO hiding (unsafeFreeze)
import Data.Array.Unsafe
import qualified Data.Array  as A
import Data.Tuple.HT
import Data.List
import Data.Function
import Control.Monad
import qualified Data.Map as Map
import System.Random

-- 303 Chiquita ave mt view #1

epsilon = 0.001

solve_monotone_system :: Double -> (Double -> Double) -> Double -> Double -> Double
solve_monotone_system epsilon f a0 a1 = let fx = f $ (a0 + a1)/2 
                                        in if (abs fx) < epsilon then (a0+a1)/2
                                           else if fx > 0 
                                                then solve_monotone_system epsilon f a0 $ (a0+a1)/2
                                                else solve_monotone_system epsilon f ((a0+a1)/2) a1
       
type TweetObs = (Int,[Int],[Int])
type Params = (IOUArray Int Double, IOUArray Int Double)
                                
read_data :: FilePath -> IO [TweetObs]
read_data file = fmap (map (\l -> let (rt,nrt) = partition thd3 l in (fst3 $ head $ l, map snd3 rt, map snd3 nrt))
                       . groupBy ((==) `on` fst3) 
                       . map (\[x,y,z] -> (read x, read y, z == "1")) 
                       . map words . lines) 
                 $ readFile file

randomInit :: Int -> Int -> IO Params
randomInit num_as num_ps = do
  alphas <- newArray (1,num_as) 0
  phis <- newArray (1,num_ps) 0
  forM [1..num_as] $ \i -> do
    randomIO >>= (writeArray alphas i)
  forM [1..num_ps] $ \i -> do
    randomIO >>= (writeArray phis i)
  return (alphas,phis)

doit = do
  res <- solveAlphaPhi "sap_out" $ replicate 100 $ randomInit 1000 1000
  writeFile "randinit_lcurves" $ unlines $ map (unwords . map show) $ map fst3 res
  writeFile "randinit_alphas" $ unlines $ map (unwords . map show) $ map (elems . snd3) res
  writeFile "randinit_phis" $ unlines $ map (unwords . map show) $ map (elems . thd3) res
  

solveAlphaPhi :: FilePath -> [IO Params] -> IO [([Double],UArray Int Double, UArray Int Double)]
solveAlphaPhi path ps = do
  alpha_obs <- read_data path
  let phi_obs = transpose_data alpha_obs
  forM (zip [1..] ps) $ \(i,p) -> do
    putStrLn $ "trial = " ++ (show i)
    ps <- p
    solveAlphaPhi_ 8 alpha_obs phi_obs ps

solveAlphaPhi_ :: Int -> [TweetObs] -> [TweetObs] -> Params -> IO ([Double],UArray Int Double, UArray Int Double)
solveAlphaPhi_ num_its alpha_obs phi_obs params = do
  res <- forM [1..num_its] $ \i -> do
    putStrLn $ "   iteration = " ++ (show i)
    optimize_alphas alpha_obs params
    putStrLn $ "      alphas optimized"
    optimize_phis phi_obs params
    putStrLn $ "      phis optimized"
    l <- likelihood alpha_obs params
    putStrLn $ "      likelihood = " ++ (show l)
    return l
  a1 <- unsafeFreeze $ fst params
  p1 <- unsafeFreeze $ snd params
  return (res,a1,p1)
  
    
optimize_alphas :: [TweetObs] -> Params -> IO ()
optimize_alphas obs ps@(alphas,phis) = do
  forM_ obs $ \o@(s,rt,nrt) -> do
    ofun <- optimize_alpha_objective o ps
    let new_a_s = solve_monotone_system epsilon ofun 0 1
    writeArray alphas s new_a_s

optimize_phis :: [TweetObs] -> Params -> IO ()
optimize_phis obs ps@(alphas,phis) = do
  forM_ obs $ \o@(u,rt,nrt) -> do
    ofun <- optimize_phi_objective o ps
    let new_p_u = solve_monotone_system epsilon ofun 0 1
    writeArray phis u new_p_u


optimize_alpha_objective :: TweetObs -> Params -> IO (Double -> Double)
optimize_alpha_objective (s,rt,nrt) (_,phis) = do
  rel_phis <- mapM (readArray phis) nrt
  let num_rt = fromIntegral $ length rt
  return $ \x -> (foldl' (+) 0 $ map (\p -> x/(1-x*p)) rel_phis) - num_rt
  
optimize_phi_objective :: TweetObs -> Params -> IO (Double -> Double)
optimize_phi_objective (u,rtd,nrtd) (alphas,_) = do
  nrt_alphas <- mapM (readArray alphas) nrtd
  let num_rt = fromIntegral $ length rtd
  return $ \x -> (foldl' (+) 0 $ map (\p -> x/(1-x*p)) nrt_alphas) - num_rt

transpose_data :: [TweetObs] -> [TweetObs]
transpose_data obs = map (\(i,(j,k)) -> (i,j,k)) $ Map.toList tmp
  where tmp :: Map.Map Int ([Int],[Int])
        tmp = foldl' (\mp (s,rts,nrts) -> 
                       foldl' (\mp i ->  Map.insertWith (\([i],[]) (x,y) -> (i:x,y)) i ([s],[]) mp)
                       (foldl' (\mp i -> Map.insertWith (\([],[i]) (x,y) -> (x,i:y)) i ([],[s]) mp) 
                        mp nrts)
                       rts)
              Map.empty
              obs 

likelihood :: [TweetObs] -> Params -> IO Double
likelihood obs (alphas,phis) = do
  fmap (foldl' (+) 0) $ forM obs $ \(s,retweets,nretweets) -> do
    a_s <- readArray alphas s
    from_rt <- fmap (foldl' (+) 0) $ forM retweets $ \u -> fmap  (log.(*a_s)) (readArray phis u) 
    from_nrt <- fmap (foldl' (+) 0) $ forM retweets $ \u -> fmap (\phi -> log $ 1-phi*a_s) (readArray phis u)
    return $ from_rt + from_nrt


how_good = do
  alphas <- fmap (head . map (map read) . map words . lines) $ readFile "randinit_alphas" :: IO [Double]
  true_alphas <- fmap (head . tail . map (map read) . map words . lines) $ readFile "alpha_out" :: IO [Double]
  print $ foldl' (+) 0 $ zipWith (\x y -> (x-y)^2/1000) alphas true_alphas
  phis <- fmap (head . map (map read) . map words . lines) $ readFile "randinit_phis" :: IO [Double]
  true_phis <- fmap (head . tail . map (map read) . map words . lines) $ readFile "phi_out" :: IO [Double]
  print $ foldl' (+) 0 $ zipWith (\x y -> (x-y)^2/1000) phis true_phis