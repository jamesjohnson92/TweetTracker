rm(list=ls())

data <- (as.numeric(read.csv("phis",sep=' ',header=F)[,2])-1)/7
data.no.zero = data[which(data!=0)]
data.no.zeroone = data.no.zero[which(data.no.zero!=1)]

