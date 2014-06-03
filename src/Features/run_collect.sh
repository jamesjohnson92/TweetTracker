#!/bin/sh

apdir='s3n://tweettrack/AlphaPhi_Output';
trdir='s3n://tweettrack/Twitterrank_Full_Output';
featdir='s3n://tweettrack/Feature_Output';
temphdfsdir='hdfs:///collecttemp/';
numtopics=30;
s3distcpjar='/home/hadoop/lib/emr-s3distcp-1.0.jar' #cluster itself's home
setnums='--jobconf mapreduce.map.tasks=5 --jobconf mapreduce.reduce.tasks=5 --num-ec2-instances 5 --ec2-instance-type c3.xlarge'

python RunCollectHive.py emr $apdir $trdir $featdir $temphdfsdir 1 $s3distcpjar;

