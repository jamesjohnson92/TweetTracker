#!/bin/sh

indir='s3n://tweettrack/Twitter_Thimble/';
outdir='s3n://tweettrack/Twitterrank_Output/';
temphdfsdir='hdfs:///tweettracktemp/';
numtopics=30;
nummappers=3;
numreducers=3;
mahoutjar='s3n://mrldajarbucket/mahout-examples-1.0-SNAPSHOT-job.jar'; ##mahout jar
mrldajar='s3n://mrldajarbucket/Mr.LDA-0.0.1.jar';
s3distcpjar='/home/hadoop/lib/emr-s3distcp-1.0.jar' #cluster itself's home
stopwords='s3n://mrldajarbucket/stopwords';
setnums='--jobconf mapreduce.map.tasks=5 --jobconf mapreduce.reduce.tasks=5 --num-ec2-instances 6 --ec2-instance-type m3.xlarge'

#python GenerateCorpus.py $setnums -r emr $indir --output-dir $outdir/corpus --no-output;
#python RunMrJobs.py emr $mrldajar $outdir $nummappers $numreducers $numtopics $stopwords $temphdfsdir $s3distcpjar;
#python FollowersTable.py  $setnums -r emr $indir --output-dir $outdir/followertable | python ldapreprocesspostprocess.py $numtopics;
#s3cmd put something something something0
python RunHive.py emr $outdir;
#python GenerateGraph.py  $setnums -r emr $outdir/pregraph --numtopics $numtopics --sumgamma $outdir/gammasums/000000_0 --output-dir $outdir/graph --no-output;
#python GraphRank.py  $setnums -r emr $outdir/graph --output-dir $outdir/twitterrank --no-output;
