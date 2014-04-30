#!/bin/sh

indir='s3://tweettrack/Twitter_Teacup/';
outdir='s3://tweettrack/Twitterrank_Output/';
numtopics=30;
nummappers=3;
numreducers=3;
mrldajar='s3n://mrldajarbucket/Mr.LDA-0.0.1.jar';
stopwords='s3n://mrldajarbucket/stopwords';
setnums='--jobconf mapreduce.map.tasks=5 --jobconf mapreduce.reduce.tasks=5 --num-ec2-instances 6 --ec2-instance-type m3.xlarge'

#python GenerateCorpus.py $setnums -r emr $indir --output-dir $outdir/corpus --no-output;
python RunMrLDA.py emr $mrldajar $outdir $nummappers $numreducers $numtopics $stopwords #don't need no-output
#python FollowersTable.py  $setnums -r emr $indir --output-dir $outdir/followertable | python ldapreprocesspostprocess.py $numtopics;
#send to s3
#python RunHive.py emr $outdir #don't need no-output
#python GenerateGraph.py  $setnums -r emr $outdir/pregraph --numtopics $numtopics --sumgamma $outdir/gammasums/000000_0 --output-dir $outdir/graph --no-output;
#python GraphRank.py  $setnums -r emr $outdir/graph --output-dir $outdir/twitterrank --no-output;
