#!/bin/sh

indir='hdfs:///user/cloudera/twitterdata/smallsample.txt';
outdir='hdfs:///user/cloudera/tweettopicout';
rankdir='hdfs:///user/cloudera/smallsampleout';
numtopics=30;
nummappers=3;
numreducers=3;
mrldajar='/home/cloudera/Mr.LDA/bin/Mr.LDA-0.0.1.jar'
stopwords='s3n://mrldajarbucket/stopwords';

#python GenerateTweetCorpus.py -r emr $indir --output-dir $outdir/corpus;
python RunMrJobs.py emr $mrldajar $outdir $nummappers $numreducers $stopwords
#hadoop jar $mrldajar cc.mrlda.ParseCorpus -index $rankdir'/parsecorpus/term' -input $outdir/corpus -output $outdir/parsecorpus -mapper $nummappers -reducer $numreducers -stoplist stopwords;
#hadoop jar $mrldajar cc.mrlda.DisplayDocument -input $outdir/parsecorpus/document -output $outdir/wordcounts;
#hadoop jar $mrldajar cc.mrlda.DisplayBeta -input $rankdir/ldapreout/beta-30 -output $outdir/wordprobs; 
#hadoop jar $mrldajar cc.mrlda.DisplayPrior -input $rankdir/ldapreout/alpha-30 -output $outdir/priors; 
python RunTweetTopicHive.py emr $outdir;
hive -hiveconf TTPATH=$outdir -f tweettopichive.q;
