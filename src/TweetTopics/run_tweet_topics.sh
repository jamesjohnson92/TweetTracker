#!/bin/sh

indir='hdfs:///user/cloudera/twitterdata/threehours';
outdir='hdfs:///user/cloudera/tweettopicout';
numtopics=30;
nummappers=2;
numreducers=2;
mrldajar='/home/cloudera/Mr.LDA/bin/Mr.LDA-0.0.1.jar'

hadoop fs -rm -r $outdir;
hadoop fs -mkdir $outdir;

python GenerateTweetCorpus.py -r hadoop $indir/smallsample.txt --output-dir $outdir/corpus;
hadoop jar $mrldajar cc.mrlda.ParseCorpus -input $outdir/corpus -output $outdir/parsecorpus -mapper $nummappers -reducer $numreducers -stopwords ../stopwords;
