#!/bin/sh

indir='s3n://tweettrack/Twitter_Firehose/';
outdir='s3n://tweettrack/Twitterrank_Full_Output/';
outdirnoslash='s3://tweettrack/Twitterrank_Full_Output';
temphdfsdir='hdfs:///tweettracktemp/';
numtopics=30;
nummappers=5; #these are for the manual things
numreducers=5;
mahoutjar='s3n://mrldajarbucket/mahout-examples-1.0-SNAPSHOT-job.jar'; ##mahout jar
mrldajar='s3n://mrldajarbucket/Mr.LDA-0.0.1.jar';
s3distcpjar='/home/hadoop/lib/emr-s3distcp-1.0.jar' #cluster itself's home
stopwords='s3n://mrldajarbucket/stopwords';

python GenerateCorpus.py -c mrjob.conf -r emr $indir --output-dir $outdir/corpus --no-output;
#python RunMrJobsParse.py emr $mrldajar $outdir $nummappers $numreducers $numtopics $stopwords $temphdfsdir $s3distcpjar;
#python RunMrJobsVarInf.py emr $mrldajar $outdir $nummappers $numreducers $numtopics $stopwords $temphdfsdir $s3distcpjar;
#python RunMrJobsDispDoc.py emr $mrldajar $outdir $nummappers $numreducers $numtopics $stopwords $temphdfsdir $s3distcpjar;
#python FollowersTable.py  $setnums -r emr $indir --output-dir $outdir/followertable ###create the ldapostprocess now
#python RunHive.py emr $outdir;
#python GenerateGraph.py  $setnums -r emr $outdirnoslash/pregraph/ --numtopics $numtopics --sumgamma $outdirnoslash/gammasums/80bc4c70-e2c5-46b9-83fe-aa458371a2a2_000000 --output-dir $outdirnoslash/graph --enable-emr-debugging --no-output; ##this is very fragile, do the gammasums right
#python GraphRank.py  $setnums -r emr $outdirnoslash/graph/ --output-dir $outdirnoslash/twitterrank --enable-emr-debugging --no-output;





#python RunMrJobs.py emr $mrldajar $outdir $nummappers $numreducers $numtopics $stopwords $temphdfsdir $s3distcpjar;
