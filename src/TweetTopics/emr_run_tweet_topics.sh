#!/bin/sh

indir='s3n://tweettrack/Twitter_Thimble/';
outdir='s3n://tweettrack/TweetTopics_Output/';
temphdfsdir='hdfs:///tweettopictemp/';
temphdfsdir2='hdfs:///tweettopictemp2/';
rankdir='s3n://tweettrack/Twitterrank_Output/';
numtopics=30;
nummappers=3;
numreducers=3;
mrldajar='s3n://mrldajarbucket/Mr.LDA-0.0.1.jar';
stopwords='s3n://mrldajarbucket/stopwords';
setnums='--jobconf mapreduce.map.tasks=5 --jobconf mapreduce.reduce.tasks=5 --num-ec2-instances 6 --ec2-instance-type m3.xlarge'
s3distcpjar='/home/hadoop/lib/emr-s3distcp-1.0.jar' #cluster itself's home

#python GenerateTweetCorpus.py $setnums -r emr $indir --output-dir $outdir/corpus;
#python RunMrJobs.py emr $mrldajar $outdir $nummappers $numreducers $stopwords $temphdfsdir $temphdfsdir2 $s3distcpjar $rankdir

priors='/home/curuinor/dev/twitterdat/tweettopic/tweet_priors';
wc='/home/curuinor/dev/twitterdat/tweettopic/combined_wc';
wp='/home/curuinor/dev/twitterdat/tweettopic/combined_wp';
python TweetTopics.py $priors $wc $wp;
