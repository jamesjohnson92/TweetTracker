#!/bin/sh

indir='s3n://tweettrack/Twitter_Firehose_Subset';
outdir='s3n://tweettrack/TweetTopic_Full_Output';
rankdir='s3n://tweettrack/Twitterrank_Full_Output';
numtopics=30;
s3distcpjar='/home/hadoop/lib/emr-s3distcp-1.0.jar' #cluster itself's home
setnums='--jobconf mapreduce.map.tasks=5 --jobconf mapreduce.reduce.tasks=5 --num-ec2-instances 5 --ec2-instance-type c3.xlarge'
temphdfsdir='hdfs:///alphaphitemp';
mrldajar='s3n://mrldajarbucket/Mr.LDA-0.0.1.jar';
stopwords='s3n://mrldajarbucket/stopwords';

python GenerateTweetCorpus.py -r emr $indir/ --output-dir $outdir/corpus;
hadoop jar $mrldajar cc.mrlda.ParseCorpus -index $rankdir'/parsecorpus/term' -input $outdir/corpus -output $outdir/parsecorpus -mapper $nummappers -reducer $numreducers -stoplist stopwords;
hadoop jar $mrldajar cc.mrlda.DisplayDocument -input $outdir/parsecorpus/document -output $outdir/wordcounts;
hadoop jar $mrldajar cc.mrlda.DisplayBeta -input $rankdir/ldapreout/beta-30 -output $outdir/wordprobs; 
hadoop jar $mrldajar cc.mrlda.DisplayPrior -input $rankdir/ldapreout/alpha-30 -output $outdir/priors; 
#hive -hiveconf TTPATH=$outdir -f tweettopichive.q;
