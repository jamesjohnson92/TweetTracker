#!/bin/sh

indir='s3n://tweettrack/Twitter_Firehose_Subset';
outdir='s3n://tweettrack/AlphaPhi_Output';
topicdir='s3n://tweettrack/Twitterrank_Full_Output';
temphdfsdir='hdfs:///alphaphitemp';
numtopics=30;
s3distcpjar='/home/hadoop/lib/emr-s3distcp-1.0.jar' #cluster itself's home
setnums='--jobconf mapreduce.map.tasks=5 --jobconf mapreduce.reduce.tasks=5 --num-ec2-instances 5 --ec2-instance-type c3.xlarge'

#python TweetTable.py $setnums -r emr $indir/ --output-dir $outdir/tweettable --no-output;
#python RunEstimateGraphHive.py emr $outdir/ $temphdfsdir $s3distcpjar;

#python EstimateTwitterGraph.py $setnums -r emr $outdir/twitterpregraph/ --output-dir $outdir/twittergraph --no-output;

#python RunAlphaPhiHive.py emr $outdir $topicdir $numtopics $temphdfsdir $s3distcpjar

for i in `seq 25 30`
do
     echo 'i = '$i
#     ./Main thedata/thedata$i/alphafile thedata/thedata$i/phifile out$i
done


for i in `seq 1 30`
do
     echo 'i = '$i
     for j in `seq 1 30`
     do
	 echo 'j = '$j
#	 rm -rf thedata/thedata$i/alphafile_time$(($j*30))
# 	 python AlphaFile.py thedata/thedata$i/alphaphiin_time$(($j*30))/ --mode 'alpha' --output-dir thedata/thedata$i/alphafile_time$(($j*30)) --no-output
	 ./Main thedata/thedata$i/alphafile_time$(($j*30)) phi_estimateout$(($i))_8 thedata/thedata$i/alpha_estimate_time$(($j*30))
     done
done
