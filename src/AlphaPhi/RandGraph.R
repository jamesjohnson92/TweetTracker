rm(list=ls())



library(igraph)
library(MASS)

writeRandMat <- function(name, size, con, prob, modprob)
{
  res = as.matrix(get.adjacency(watts.strogatz.game(1, size, con, prob, loops = T)))
  for (i in 1:size)
  {
    for (j in 1:size)
    {
      if (modprob > runif(1))
      {
        res[i,j] = (1 + res[i,j]) %% 2
      }
    }
  }
  write.matrix(res,name)
}
