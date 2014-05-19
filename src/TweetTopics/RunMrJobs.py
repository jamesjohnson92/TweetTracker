import boto, boto.emr, sys, time
from subprocess import call
from utils import *

if __name__ == "__main__":
    if sys.argv[1] == "emr":
        conn = boto.emr.connect_to_region("us-east-1") #north virginia
        log_uri = u's3n://tweettrack/Twitterrank_Log/TweetTopics_log'
        print sys.argv
        mrjobsjar = unicode(sys.argv[2])
        outdir = unicode(sys.argv[3])
        nummappers = unicode(sys.argv[4])
        numreducers = unicode(sys.argv[5])
        stopwords = unicode(sys.argv[6])
        tempdir = unicode(sys.argv[7])
        tempdir2 = unicode(sys.argv[8])
        s3distcpjar = unicode(sys.argv[9])
        rankdir = unicode(sys.argv[10])

        jars = (s3distcpjar, s3distcpjar, mrjobsjar, mrjobsjar, mrjobsjar, mrjobsjar)
        class3 = u'cc.mrlda.ParseCorpus'
        class4 = u'cc.mrlda.DisplayDocument'
        class5 = u'cc.mrlda.DisplayBeta'
        class6 = u'cc.mrlda.DisplayPrior'
        classes = (None, None, class3, class4, class5, class6)
        args1 = [u'--src', outdir + "corpus",
                 u'--dest', tempdir + "corpus"]
        args2 = [u'--src', rankdir + "parsecorpus/term",
                 u'--dest', tempdir2 + "parsecorpus/term"]
        args3 = [
                u'-index', tempdir2 + "parsecorpus/term",
                u'-input', tempdir + "corpus",
                u'-output', tempdir + "parsecorpus",
                u'-mapper', nummappers,
                u'-reducer', numreducers,
                u'-stoplist', stopwords]
        args4 = [
                u'-input', tempdir + "parsecorpus/document",
                u'-output', outdir + "wordcounts"]
        args5 = [
                u'-input', rankdir + "ldapreout/beta-5",
                u'-output', outdir + "wordprobs"]
        args6 = [
                u'-input', rankdir + "ldapreout/alpha-5",
                u'-output', outdir + "priors"]
        args = (args1, args2, args3, args4, args5, args6)
        steps = []
        names = ("Copy indir to hdfs", "Copy rankdir to hdfs", "Parse Corpus", "Display Document", "Display Beta", "Display Priors")
        for i in xrange(6):
            step = boto.emr.JarStep(names[i],
                           jars[i],
                           main_class=classes[i],
                           step_args=args[i],
                           )
            steps.append(step)
        jobid = conn.run_jobflow("Mr. LDA Doing TweetTopics and Hungering for the Souls of the Living", log_uri=log_uri,
                                           steps=steps,
                                           master_instance_type=master, #see utils
                                           slave_instance_type=slave,
                                           num_instances=num_instances,
                                           enable_debugging=True,
                                           ami_version="latest",
                                           hadoop_version="2.2.0")
        wait_until(lambda: check_connection(conn, jobid), 86400)
    else:
        pass
        """
        Without elastic map reduce: explode now
        """
