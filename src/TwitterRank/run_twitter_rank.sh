#!/bin/sh

# $0 = input
# $1 = output director, will be deleted when you call the script

hadoop fs -rm-dir $1;
hadoop fs -mkdir $1;

python GenerateCorpus.py -r hadoop $0 --output-dir $1/corpus;
hadoop jar /home/cloudera/Mr.LDA/bin/Mr.LDA-0.0.1.jar cc.mrlda.ParseCorpus -input $1/corpus/document -output $1/ldaout;
python FollowersTable.py -r hadoop $0 --output-dir $1/followertable;
python TweetCounts.py -r hadoop $0 --output-dir $1/tweetcounts;
hive -f ldapostprocess.q hdfspath=$1;
python GenerateGraph.py -r hadoop $1/ldapost --output-dir $1/twittergraph --numtopics 5;
python GraphRank.py -r hadoop $1/ldajoined --output-dir $1/twitterrank;
