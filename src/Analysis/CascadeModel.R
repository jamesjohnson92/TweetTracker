rm(list=ls())
library(cvTools)
library(e1071)
library(Metrics)
library(randomForest)

my.data = as.data.frame(read.csv("the_alpha_phi_cascade_table",header=F,sep=' '))

colnames(my.data) = c("tweet_id","alphatotal","friends","followers",
            Map(function(i) { paste("retweettime",2^i,sep="") }, (0:29)),
            Map(function(i) { paste("alphatime",2^i,sep="") }, (0:29)))

results = matrix(0,3,8)


for (i in 1:8)
{
    print (i)

    rows = which(my.data[,i+4] != -1)
    if (length(rows) == 0) { break }
    response = as.factor(-1 != my.data[rows,i+5])
    rttimes = my.data[rows,5:(i+4)]
    alphas = my.data[rows,35:(i+34)]
    base = my.data[rows,c(3,4)]
    pro = my.data[rows,2]

    
    results[1,i] = svm(x=cbind(rttimes,alphas,base,pro),y=response,type='C-classification',cross=3)$tot.accuracy
    print("fit1")
    results[2,i] = svm(x=cbind(rttimes,alphas,base),y=response,type='C-classification',cross=3)$tot.accuracy
    print("fit2")
    results[3,i] = svm(x=cbind(rttimes,base),y=response,type='C-classification',cross=3)$tot.accuracy
    print("fit3")

    
}

png("jurerocks.png")
matplot(x=1:8,y=t(results),type='b',pch=1,lty=1,main="Cascade accuracy rate vs log(k)",xlab="log(k)",ylab="accuracy",col=c('red','blue','green'))
legend("bottomright",9.5,c("Partial and Total Alphas",
                          "Only Partial Alphas",
                          "No Alphas"),col=c('red','blue','green'),lty=1)
dev.off()
