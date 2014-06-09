rm(list=ls())
library(cvTools)
library(e1071)
library(Metrics)
library(randomForest)

my.data = as.data.frame(read.csv("full_table",header=F,sep=' '))

colnames(my.data) = c("tweet_id","alphatotal","friends","followers",
            Map(function(i) { paste("phiavg",i,sep="") }, 30*(1:30)),
            Map(function(i) { paste("alpha",i,sep="") }, 30*(1:30)),
            Map(function(i) { paste("logrrt",i,sep="") }, 20*(1:180)),
            Map(function(i) { paste("partialretweetrank",i,sep="") }, 20*(1:40)),
            "retweetrank")

results = matrix(0,9,10)


for (i in 1:10)
{
    print (i)
    break
    rtrstr = paste(paste("log(0.00001 + partialretweetrank",20*(1:(3*i)),")",sep=''),collapse="+")
    logrrtstr = paste(paste("logrrt",20*(1:(3*i)),sep=''),collapse="+")
    fmlafull = as.formula(paste("logrrt3360 ~ log(0.00001 + retweetrank) + log(friends+1) + log(followers+1) + ",rtrstr,"+",logrrtstr))

    fit1 = lm(fmlafull,data=my.data)
    print("fit1")
#    fit2 = lm(fmlaalpha,data=my.data)
#    print("fit2")
#    fit3 = lm(fmlalogrrt,data=my.data)
#    print("fit3")
    
    fit4 = svm(fmlafull,data=my.data,type='eps-regression',gamma=0.5,cost=8,cross=3)
    print("fit4")
#    fit5 = svm(fmlaalpha,data=my.data,type='eps-regression',cross=3)
#    print("fit5")
 #   fit6 = svm(fmlalogrrt,data=my.data,type='eps-regression',cross=3)
 #   print("fit6")

#    fit7 = randomForest(fmlafull,data=my.data,type='regression')
#    print("fit7")
 #   fit8 = randomForest(fmlaalpha,data=my.data,scale=F,type='regression')
  #  print("fit8")
  #  fit9 = randomForest(fmlalogrrt,data=my.data,scale=F,type='regression')
  #  print("fit9")

    results[1,i] = cvFit(fit1,data=my.data, y=my.data$logrrt180, cost=rmspe, K=5,R=2)$cv
    print("cv1")
   # results[2,i] = cvFit(fit2,data=my.data, y=my.data$logrrt180, cost=rmspe, K=5,R=2)$cv
   # print("cv2")
   # results[3,i] = cvFit(fit3,data=my.data, y=my.data$logrrt180, cost=rmspe, K=5,R=2)$cv
   # print("cv3")


    results[4,i] = sqrt(fit4$tot.MSE)
    print("cv4")
    #results[5,i] = sqrt(fit5$tot.MSE)
    #print("cv5")
    #results[6,i] = sqrt(fit6$tot.MSE)
    #print("cv6")


 #   results[7,i] = cvFit(fit7,data=my.data, y=my.data$logrrt180, cost=rmspe, K=3,R=2)$cv
 #   print("cv7")
    #results[8,i] = cvFit(fit8,data=my.data, y=my.data$logrrt180, cost=rmspe, K=3,R=2)$cv
    #print("cv8")
    #results[9,i] = cvFit(fit9,data=my.data, y=my.data$logrrt180, cost=rmspe, K=3,R=2)$cv
    #print("cv9")
    
}

