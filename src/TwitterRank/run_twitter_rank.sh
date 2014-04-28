#!/bin/sh

indir='s3:///tweettrack/Twitter_Teacup/';
outdir='s3:///tweettrack/Twitterrank_Output/';
numtopics=30;
nummappers=1;
numreducers=1;
mrldajar='s3:///mrldajarbucket/Mr.LDA-0.0.1.jar';
stopwords='s3:///mrldajarbucket/stopwords';
setnums='--jobconf mapreduce.map.tasks=1 --jobconf mapreduce.reduce.tasks=1'

python GenerateCorpus.py $setnums -r emr $indir --output-dir $outdir/corpus --no-output;
python RunMrLDA.py emr $mrldajar $outdir $nummappers $numreducers $numtopics $stopwords #don't need no-output
python FollowersTable.py  $setnums -r emr $indir --output-dir $outdir/followertable > python ldapreprocesspostprocess.py $numtopics;
python RunHive.py emr $outdir #don't need no-output
python GenerateGraph.py  $setnums -r emr $outdir/pregraph --numtopics $numtopics --sumgamma $outdir/gammasums/000000_0 --output-dir $outdir/graph --no-output;
python GraphRank.py  $setnums -r emr $outdir/graph --output-dir $outdir/twitterrank --no-output;
