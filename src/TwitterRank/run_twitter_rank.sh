#!/bin/sh

indir='hdfs:///user/cloudera/twitterdata';
outdir='hdfs:///user/cloudera/twitterrankout';
numtopics=5;
nummappers=1;
numreducers=1

hadoop fs -rm -r $outdir;
hadoop fs -mkdir $outdir;

python GenerateCorpus.py -r hadoop $indir/smallsample.txt --output-dir $outdir/corpus;
hadoop jar /home/cloudera/Mr.LDA/bin/Mr.LDA-0.0.1.jar cc.mrlda.ParseCorpus -input $outdir/corpus -output $outdir/parsecorpus -mapper $nummappers -reducer $numreducers;
hadoop jar /home/cloudera/Mr.LDA/bin/Mr.LDA-0.0.1.jar cc.mrlda.VariationalInference -input $outdir/parsecorpus/document -output $outdir/ldapreout -mapper $nummappers - reducer $numreducers -term 10000 -topic $numtopics;
hadoop jar /home/cloudera/Mr.LDA/bin/Mr.LDA-0.0.1.jar cc.mrlda.DisplayDocument -input $outdir/ldapreout/gamma-30 -output $outdir/ldaout;
python FollowersTable.py -r hadoop $indir/smallsample.txt --output-dir $outdir/followertable;
python ldapreprocesspostprocess.py $numtopics;
hive -hiveconf TROPATH=$outdir -f ldapostprocess.q;
python GenerateGraph.py -r hadoop $outdir/pregraph --numtopics $numtopics --sumgamma $outdir/gammasums/000000_0 --output-dir $outdir/graph;
python GraphRank.py -r hadoop $outdir/graph --output-dir $outdir/twitterrank;
