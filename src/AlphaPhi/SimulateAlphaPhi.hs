module SimulateAlphaPhi where
       
import Data.List
import Data.Array
import qualified Data.Set as Set
import System.Random
import Data.IORef
import Control.Monad
import Data.Random.Distribution.Beta
import Data.Random.Distribution.Categorical
import Data.Random.Distribution.ChiSquare
import Data.Random.Distribution.Dirichlet
import Data.RVar
import Data.Tuple.HT

type Graph = Array Int (Array Int Double, [Int]) 

type Entry = (Int, -- tweet_id
             Int, -- user_id
             Int, -- tau
             Bool) -- was retweeted

type GraphMetadata = Array Int (Double,Double,Double) -- we generate alpha from beta distribution with the first two params
                                                      -- the last param is the tweeting prob

generateMetadata :: Int -> IO GraphMetadata
generateMetadata num_users = do
  fmap (array (1,num_users)) $ forM [1..num_users] $ \i -> do
    prob <- sampleRVar $ chiSquare 1 :: IO Double
    alpha <- sampleRVar $ chiSquare 2 :: IO Double
    beta <- sampleRVar $ chiSquare 5 :: IO Double
    return (i,(alpha,beta,prob))

readGraph :: Int -> FilePath -> IO Graph
readGraph num_topics filename = do
  mat <- fmap (map words . lines) $ readFile filename
  let edgeLists = map (\l -> map fst $ filter ((=="1").snd) $ zip [1..] l) mat
  fmap (array (1, length mat)) $ forM (zip [1..] edgeLists) $ \(i,es) -> do
    phi <- fmap (array (1,num_topics) . zip [1..num_topics] ) $ sampleRVar $ dirichlet $ replicate num_topics 0.1
    return (i,(phi,es))
    
type SimConsts = (Int,Double,Int,Graph) -- (tweet,alpha,tau,graph)

simulateAlphaPhi :: Int -> Graph -> GraphMetadata -> IO [Entry]
simulateAlphaPhi num_tweets graph metadata = do
  let tweeterDist = categorical $ map (\(i,(_,_,p)) -> (p,i)) $ assocs metadata
  fmap concat $ forM [1..num_tweets] $ \tweet_id -> do
    tweeter <- sampleRVar tweeterDist
    topic <- sampleRVar $ categorical $ map swap $ assocs $ fst $ graph!tweeter
    alpha <- sampleRVar $ beta (fst3 $ metadata!tweeter) (snd3 $ metadata!tweeter)
    simulateTweet tweet_id alpha topic tweeter graph

simulateTweet :: Int -> Double -> Int -> Int -> Graph -> IO [Entry]
simulateTweet tweet alpha tau poster graph = do
  seen <- newIORef $ Set.singleton poster
  simulateTweet_ (tweet,alpha,tau,graph) poster seen

simulateTweet_ cs@(tweet,alpha,tau,graph) node seen = do
  pre_seen <- readIORef seen
  modifyIORef seen $ \s -> foldl' (flip Set.insert) s $ snd $ graph!node
  fmap concat $ forM (filter (not . flip Set.member pre_seen) $ snd $ graph!node) $ \n -> do
    p <- randomIO :: IO Double
    if p < alpha * ((fst $ graph!n)!tau)
      then fmap ((tweet,n,tau,True):) $ simulateTweet_ cs n seen
      else return [(tweet,n,tau,False)]
           
toInt False = 0
toInt True = 1
           
doit = do
  graph <- readGraph 10 "jomat"
  let (1,n) = bounds graph
  md <- generateMetadata n
  res <- simulateAlphaPhi 1000 graph md 
  writeFile "sap_out" $ unlines $ flip map res $ \(a,b,c,d) -> unwords $ map show [a,b,c,toInt d]