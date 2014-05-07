#!/bin/sh

indir='hdfs:///user/cloudera/twitterdata/smallsample.txt';
outdir='hdfs:///user/cloudera/tweettopicout';
rankdir='hdfs:///user/cloudera/smallsampleout';
numtopics=30;
nummappers=1;
numreducers=1;
mrldajar='/home/cloudera/Mr.LDA/bin/Mr.LDA-0.0.1.jar'

#hadoop fs -rm -r $outdir;
#hadoop fs -mkdir $outdir;

#python GenerateTweetCorpus.py -r hadoop $indir --output-dir $outdir/corpus;
#hadoop jar $mrldajar cc.mrlda.ParseCorpus -index $rankdir'/parsecorpus/term' -input $outdir/corpus -output $outdir/parsecorpus -mapper $nummappers -reducer $numreducers -stoplist stopwords;
#hadoop jar $mrldajar cc.mrlda.DisplayDocument -input $outdir/parsecorpus/document -output $outdir/wordcounts;
#hadoop jar $mrldajar cc.mrlda.DisplayBeta -input $rankdir/ldapreout/beta-30 -output $outdir/wordprobs; 
#hadoop jar $mrldajar cc.mrlda.DisplayPrior -input $rankdir/ldapreout/alpha-30 -output $outdir/priors; 
hive -hiveconf TTPATH=$outdir -f tweettopichive.q;
