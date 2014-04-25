import boto
#don't forget to block until job completes

#hadoop jar $mrldajar cc.mrlda.ParseCorpus -input $outdir/corpus -output $outdir/parsecorpus -mapper $nummappers -reducer $numreducers -stoplist $stopwords;
#hadoop jar $mrldajar cc.mrlda.VariationalInference -input $outdir/parsecorpus/document -output $outdir/ldapreout -mapper $nummappers -reducer $numreducers -term 10000 -topic $numtopics -directemit;
#hadoop jar $mrldajar cc.mrlda.DisplayDocument -input $outdir/ldapreout/gamma-30 -output $outdir/ldaout;
