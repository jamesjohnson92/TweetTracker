#!/bin/sh

indir='hdfs:///user/cloudera/twitterdata/threehours';
outdir='hdfs:///user/cloudera/twitterrankout';
numtopics=30;
nummappers=1;
numreducers=1;
mrldajar='/home/cloudera/Mr.LDA/bin/Mr.LDA-0.0.1.jar';
setnums='--jobconf mapreduce.map.tasks=1 --jobconf mapreduce.reduce.tasks=1'

hadoop fs -rm -r $outdir;
hadoop fs -mkdir $outdir;

python GenerateCorpus.py $setnums -r hadoop $indir --output-dir $outdir/corpus;
hadoop jar $mrldajar cc.mrlda.ParseCorpus -input $outdir/corpus -output $outdir/parsecorpus -mapper $nummappers -reducer $numreducers -stoplist stopwords;
hadoop jar $mrldajar cc.mrlda.VariationalInference -input $outdir/parsecorpus/document -output $outdir/ldapreout -mapper $nummappers - reducer $numreducers -term 10000 -topic $numtopics -directemit;
hadoop jar $mrldajar cc.mrlda.DisplayDocument -input $outdir/ldapreout/gamma-30 -output $outdir/ldaout;
python FollowersTable.py  $setnums -r hadoop $indir --output-dir $outdir/followertable;
python ldapreprocesspostprocess.py $numtopics;
hive -hiveconf TROPATH=$outdir -f ldapostprocess.q;
python GenerateGraph.py  $setnums -r hadoop $outdir/pregraph --numtopics $numtopics --sumgamma $outdir/gammasums/000000_0 --output-dir $outdir/graph;
python GraphRank.py  $setnums -r hadoop $outdir/graph --output-dir $outdir/twitterrank;
