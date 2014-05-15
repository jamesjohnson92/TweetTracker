module CompareOuts where

import qualified Data.Map as Map
import Data.List
import Data.Maybe

compare_estimates f1 f2 = do
  mp <- fmap (Map.fromList . map (\[x,y] -> (read x,read y)) . map words . lines) $ readFile f1 :: IO (Map.Map Integer Double)
  d2 <- fmap (map words . lines) $ readFile f2
  return $ (foldl' (+) 0 $ map (\[x,y] -> ((read y) - (fromJust $ Map.lookup (read x) mp))^2) d2)/(fromIntegral $ Map.size mp)
        