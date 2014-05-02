#!/bin/sh

indir='s3n://tweettrack/Twitter_Teacup/';
outdir='s3n://tweettrack/Twitterrank_Output/';
temp_hdfs_dir='hdfs:///stuff';
numtopics=30;
nummappers=3;
numreducers=3;
mrldajar='s3n://mrldajarbucket/mahout-examples-1.0-SNAPSHOT-job.jar'; ##mahout jar
stopwords='s3n://mrldajarbucket/stopwords';
setnums='--jobconf mapreduce.map.tasks=5 --jobconf mapreduce.reduce.tasks=5 --num-ec2-instances 6 --ec2-instance-type m3.xlarge'

#python GenerateCorpus.py $setnums -r emr $indir --output-dir $outdir/corpus --no-output;
python RunMahout.py emr $mrldajar $outdir $nummappers $numreducers $numtopics $stopwords #don't need no-output
#python FollowersTable.py  $setnums -r emr $indir --output-dir $outdir/followertable | python ldapreprocesspostprocess.py $numtopics;
#send to s3
#python RunHive.py emr $outdir #don't need no-output
#python GenerateGraph.py  $setnums -r emr $outdir/pregraph --numtopics $numtopics --sumgamma $outdir/gammasums/000000_0 --output-dir $outdir/graph --no-output;
#python GraphRank.py  $setnums -r emr $outdir/graph --output-dir $outdir/twitterrank --no-output;
