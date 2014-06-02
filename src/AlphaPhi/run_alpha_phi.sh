#!/bin/sh

indir='s3n://tweettrack/Twitter_Firehose_Subset';
outdir='s3n://tweettrack/AlphaPhi_Output';
topicdir='s3n://tweettrack/TweetTopics_Output';
temphdfsdir='hdfs:///alphaphitemp/';
numtopics=30;
s3distcpjar='/home/hadoop/lib/emr-s3distcp-1.0.jar' #cluster itself's home
setnums='--jobconf mapreduce.map.tasks=5 --jobconf mapreduce.reduce.tasks=5 --num-ec2-instances 5 --ec2-instance-type c3.xlarge'

#python TweetTable.py $setnums -r emr $indir/ --output-dir $outdir/tweettable --no-output;
#python RunEstimateGraphHive.py emr $outdir/ $temphdfsdir $s3distcpjar;

python2.7 EstimateTwitterGraph.py $setnums -r emr $outdir/twitterpregraph/ --output-dir $outdir/twittergraph --no-output;

#python RunAlphaPhiHive.py emr $outdir $topicdir $numtopics
for i in `seq 1 $numtopics`
do
echo i
 #   python AlphaFile.py $setnums -r emr $outdir/alphaphiin$i/ --mode 'alpha' --output-dir $outdir/alphafile$i/ --no-output
 #   python AlphaFile.py $setnums -r emr $outdir/alphaphiin$i/ --mode 'phi' --output-dir $outdir/phifile$i/ --no-output
done
