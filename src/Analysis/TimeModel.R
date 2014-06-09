rm(list=ls())
library(cvTools)
library(e1071)
library(Metrics)
library(randomForest)

my.data = as.data.frame(read.csv("the_alpha_phi_table",header=F,sep=' '))
my.results = as.matrix(read.csv("the_final_results",header=T,sep=' '))

colnames(my.data) = c("tweet_id","alphatotal","friends","followers",
            Map(function(i) { paste("phiavg",i,sep="") }, 30*(1:30)),
            Map(function(i) { paste("alpha",i,sep="") }, 30*(1:30)),
            Map(function(i) { paste("logrrt",i,sep="") }, 20*(1:180)))

rownames(my.results) = c("full_linear","ap_linear","rt_linear",
            "full_svm","ap_svm","rt_svm",
            "full_rf","ap_rf","rt_rf",
            "times_linear","times_svm","times_rf",
            "psum_linear","psum_svm","psum_rf",
            "timeslog_linear","timeslog_svm")

results = matrix(0,9,10)

png("alphadist.png")
plot(jitter(my.data$alphatotal,2),my.data$logrrt3600,main="Tweet alpha vs Log of number of retweets after 1 hour",
     xlab = "alpha", ylab= "log of number of retweets")
dev.off()

png("alphalognfdist.png")
plot(jitter(my.data$alphatotal,2)*log(my.data$followers),my.data$logrrt3600,main="Tweet alpha vs Log of number of retweets after 1 hour",
     xlab = "alpha * log(num_followers+1)", ylab= "log of number of retweets")
dev.off()

png("linearattempts.png")
matplot(1:10,t(my.results[c(1,2,3,10,13,16),]),type='b',col=c('purple','blue','red','cyan','green','black'),lty=c(1,1,1,1,1,1),pch = 1,
        main = "Linear models for retweets after an hour RMSLE vs time", ylab="RMSLE",xlab="Time after tweet creation (minutes)",ylim=c(0.55,1))
legend("bottomright",9.5,c("Alphas and log(Retweet Rates)",
                          "Just Alphas",
                          "Just log(Retweet Rates)",
                          "Alpha * log(NumFollowers)",
                          "Average Phi of Retweeters and log(Retweet Rates)",
                          "Alpha * log(NumFollowers) and log(Retweet Rates)"),
       col=c('purple','blue','red','cyan','green','black'),lty=1)
dev.off()

png("svmattempts.png")

matplot(1:10,t(my.results[c(4,5,6,11,14,17),]),type='b',col=c('purple','blue','red','cyan','green','black'),lty=c(1,1,1,1,1,1),pch = 1,
        main = "SVM models for retweets after an hour RMSLE vs time", ylab="RMSLE",xlab="Time after tweet creation (minutes)",ylim=c(0.55,1))
legend("bottomright",9.5,c("Alphas and log(Retweet Rates)",
                          "Just Alphas",
                          "Just log(Retweet Rates)",
                          "Alpha * log(NumFollowers)",
                          "Average Phi of Retweeters and log(Retweet Rates)",
                          "Alpha * log(NumFollowers) and log(Retweet Rates)"),
       col=c('purple','blue','red','cyan','green','black'),lty=1)
dev.off()

png("rfattempts.png")
matplot(1:10,t(my.results[c(7,8,9,12,15),]),type='b',col=c('purple','blue','red','cyan','green'),lty=c(1,1,1,1,1),pch = 1,
        main = "RandomForrest models for retweets after an hour RMSLE vs time", ylab="RMSLE",xlab="Time after tweet creation (minutes)",ylim=c(0.55,1))
legend("bottomright",9.5,c("Alphas and log(Retweet Rates)",
                          "Just Alphas",
                          "Just log(Retweet Rates)",
                          "Alpha * log(NumFollowers)",
                          "Average Phi of Retweeters and log(Retweet Rates)"),
       col=c('purple','blue','red','cyan','green'),lty=1)
dev.off()

for (i in 1:10)
{
    print (i)
    break
    alphastr = paste(paste("log(followers+1) * alpha",30*(1:(2*i)),sep=''),collapse="+")
    logrrtstr = paste(paste("logrrt",20*(1:(3*i)),sep=''),collapse="+")
    fmlafull = as.formula(paste("logrrt3360 ~ alphatotal + alphatotal * log(followers + 1) + log(friends+1) + log(followers+1) + ",alphastr,"+",logrrtstr))

#    fit1 = lm(fmlafull,data=my.data)
#    print("fit1")
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

 #   results[1,i] = cvFit(fit1,data=my.data, y=my.data$logrrt180, cost=rmspe, K=5,R=2)$cv
 #   print("cv1")
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
  #  print("cv7")
    #results[8,i] = cvFit(fit8,data=my.data, y=my.data$logrrt180, cost=rmspe, K=3,R=2)$cv
    #print("cv8")
    #results[9,i] = cvFit(fit9,data=my.data, y=my.data$logrrt180, cost=rmspe, K=3,R=2)$cv
    #print("cv9")
    
}

