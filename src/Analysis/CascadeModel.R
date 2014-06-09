rm(list=ls())
library(cvTools)
library(e1071)
library(Metrics)
library(randomForest)

my.data = as.data.frame(read.csv("the_alpha_phi_cascade_table",header=F,sep=' '))

colnames(my.data) = c("tweet_id","alphatotal","retweetrank","friends","followers",
            Map(function(i) { paste("retweettime",2^i,sep="") }, (0:29)),
            Map(function(i) { paste("alphatime",2^i,sep="") }, (0:29)),
            Map(function(i) { paste("phisumtime",2^i,sep="") }, (0:29)),
            Map(function(i) { paste("ranktime",2^i,sep="") }, (0:29)))

results = matrix(0,5,8)


for (i in 1:8)
{
    print (i)

    rows = which(my.data[,i+5] != -1)
    if (length(rows) == 0) { break }
    response = as.factor(-1 != my.data[rows,i+6])
    rttimes = my.data[rows,6:(i+5)]
    alphas = my.data[rows,36:(i+35)]
    phis = my.data[rows,66:(i+65)]
    ranks = my.data[rows,96:(i+95)]
    base = my.data[rows,c(3,4)]
    pro = my.data[rows,2]
    rtr = my.data[rows,3]

    
    results[1,i] = svm(x=cbind(rttimes,alphas,base,pro),y=response,type='C-classification',cross=3)$tot.accuracy
    print("fit1")
    results[2,i] = svm(x=cbind(rttimes,alphas,base),y=response,type='C-classification',cross=3)$tot.accuracy
    print("fit2")
    results[3,i] = svm(x=cbind(rttimes,base),y=response,type='C-classification',cross=3)$tot.accuracy
    print("fit3")
    results[4,i] = svm(x=cbind(rttimes,ranks,rtr,base,pro),y=response,type='C-classification',cross=3)$tot.accuracy
    print("fit5")
    results[5,i] = svm(x=cbind(rttimes,phis,base,pro),y=response,type='C-classification',cross=3)$tot.accuracy
    print("fit6")

    
}

png("jurerocks.png")
matplot(x=1:8,y=t(results),type='b',pch=1,lty=1,main="Cascade accuracy rate vs log(k)",xlab="log(k)",ylab="accuracy",col=c('red','blue','green',"cyan","black"))
legend("bottomright",9.5,c("Partial and Total Alphas",
                           "Only Partial Alphas",
                           "Retweeter Phi Average",
                           "Partial and Total RetweetRank",
                           "No Special Features"),col=c('red','blue','green',"cyan","black"),lty=1)
dev.off()
