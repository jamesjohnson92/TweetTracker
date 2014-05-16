#!/bin/sh

indir='s3n://tweettrack/Twitter_Thimble';
outdir='s3n://tweettrack/AlphaPhi_Output';
topicdir='s3n://tweettrack/TweetTopics_Output';
temphdfsdir='hdfs:///tweettracktemp/';
numtopics=30;
s3distcpjar='/home/hadoop/lib/emr-s3distcp-1.0.jar' #cluster itself's home
setnums='--jobconf mapreduce.map.tasks=5 --jobconf mapreduce.reduce.tasks=5 --num-ec2-instances 6 --ec2-instance-type m3.xlarge'

#python TweetTable.py -r emr $indir/ --output-dir $outdir/tweettable --no-output;
#python RunAlphaPhiHive.py emr $outdir $topicdir $numtopics

for i in `seq 1 $numtopics`
do
    python AlphaFile.py $setnums -r emr $outdir/alphaphiin$i/ --mode 'alpha' --output-dir $outdir/alphafile$i/ --no-output
    python AlphaFile.py $setnums -r emr $outdir/alphaphiin$i/ --mode 'phi' --output-dir $outdir/phifile$i/ --no-output
done
