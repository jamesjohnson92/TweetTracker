rm(list=ls())
library(cvTools)
library(e1071)
library(Metrics)
library(randomForest)

my.data = as.data.frame(read.csv("full_table",header=F,sep=' '))
my.results = as.matrix(read.csv("the_final_results",header=T,sep=' '))

colnames(my.data) = c("tweet_id","alphatotal","friends","followers",
            Map(function(i) { paste("phiavg",i,sep="") }, 30*(1:30)),
            Map(function(i) { paste("alpha",i,sep="") }, 30*(1:30)),
            Map(function(i) { paste("logrrt",i,sep="") }, 20*(1:180)),
            Map(function(i) { paste("partialretweetrank",i,sep="") }, 20*(1:40)),
            "retweetrank")


rownames(my.results) = c("full_linear","ap_linear","rt_linear",
            "full_svm","ap_svm","rt_svm",
            "full_rf","ap_rf","rt_rf",
            "times_linear","times_svm","times_rf",
            "psum_linear","psum_svm","psum_rf",
            "timeslog_linear","timeslog_svm",
            "retweetrank_linear","retweetrank_svm",
            "constant predictor","retweetrank_rf")

png("alphadist.png")
plot(jitter(my.data$alphatotal,2),my.data$logrrt3600,main="Tweet alpha vs Log of number of retweets after 1 hour",
     xlab = "alpha", ylab= "log of number of retweets")
dev.off()

png("alphalognfdist.png")
plot(jitter(my.data$alphatotal,2)*log(my.data$followers),my.data$logrrt3600,main="Tweet alpha vs Log of number of retweets after 1 hour",xlab = "alpha * log(num_followers+1)", ylab= "log of number of retweets")
dev.off()

png("linearattempts.png")
matplot(1:10,t(my.results[c(1,2,3,10,13,16,18,20),]),type='b',col=c('purple','blue','red','cyan','green','black','orange','violet'),lty=c(1,1,1,1,1,1,1),pch = 1,
        main = "Linear models for retweets after an hour RMSLE vs time", ylab="RMSLE",xlab="Time after tweet creation (minutes)",ylim=c(0.45,1))
legend("bottomleft",9.5,c("Alphas and log(Retweet Rates)",
                           "Just Alphas",
                           "Just log(Retweet Rates)",
                           "Alpha * log(NumFollowers)",
                           "Average Phi of Retweeters and log(Retweet Rates)",
                           "Alpha * log(NumFollowers) and log(Retweet Rates)",
                           "Total and Partial RetweetRank","Constant Predictor"),
       col=c('purple','blue','red','cyan','green','black',"orange","violet"),lty=1)
dev.off()

png("svmattempts.png")

matplot(1:10,t(my.results[c(4,5,6,11,14,17,19,20),]),type='b',col=c('purple','blue','red','cyan','green','black',"orange",'violet'),lty=c(1,1,1,1,1,1),pch = 1,
        main = "SVM models for retweets after an hour RMSLE vs time", ylab="RMSLE",xlab="Time after tweet creation (minutes)",ylim=c(0.45,1))
legend("bottomright",9.5,c("Alphas and log(Retweet Rates)",
                           "Just Alphas",
                           "Just log(Retweet Rates)",
                           "Alpha * log(NumFollowers)",
                           "Average Phi of Retweeters and log(Retweet Rates)",
                           "Alpha * log(NumFollowers) and log(Retweet Rates)",
                           "Total and Partial RetweetRank","Constant Predictor"),
       col=c('purple','blue','red','cyan','green','black',"orange","violet"),lty=1)
dev.off()

png("rfattempts.png")
matplot(1:10,t(my.results[c(7,8,9,12,15,21,20),]),type='b',col=c('purple','blue','red','cyan','green','violet'),lty=c(1,1,1,1,1),pch = 1,
        main = "RF models for retweets after an hour RMSLE vs time", ylab="RMSLE",xlab="Time after tweet creation (minutes)",ylim=c(0.45,1))
legend("bottomleft",9.5,c("Alphas and log(Retweet Rates)",
                          "Just Alphas",
                          "Just log(Retweet Rates)",
                          "Alpha * NumFollowers",
                          "Average Phi of Retweeters and log(Retweet Rates)",
                           "Total and Partial RetweetRank","Constant Predictor"),
       col=c('purple','blue','red','cyan','green',"violet"),lty=1)
dev.off()
