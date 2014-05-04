import boto, boto.emr, sys, time
from subprocess import call
from utils import *

if __name__ == "__main__":
    if sys.argv[1] == "emr":
        conn = boto.emr.connect_to_region("us-east-1") #north virginia
        log_uri = u's3n://tweettrack/Twitterrank_Log/LDA_log'
        mrjobsjar = unicode(sys.argv[2])
        outdir = unicode(sys.argv[3])
        nummappers = unicode(sys.argv[4])
        numreducers = unicode(sys.argv[5])
        numtopics = unicode(sys.argv[6])
        stopwords = unicode(sys.argv[7])
        tempdir = unicode(sys.argv[8])
        s3distcpjar = unicode(sys.argv[9])

        jars = (s3distcpjar, mrjobsjar, mrjobsjar, mrjobsjar, s3distcpjar)
        class2 = u'cc.mrlda.ParseCorpus'
        class3 = u'cc.mrlda.VariationalInference'
        class4 = u'cc.mrlda.DisplayDocument'
        classes = (None, class2, class3, class4, None)
        args1 = [u'--src', outdir + "corpus",
                 u'--dest', tempdir + "corpus"]
        args2 = [
                u'-input', tempdir + "corpus",
                u'-output', tempdir + "parsecorpus",
                u'-mapper', nummappers,
                u'-reducer', numreducers,
                u'-stoplist', stopwords]
        args3 = [
                u'-input',  tempdir + "parsecorpus/document",
                u'-output', tempdir + "ldapreout",
                u'-mapper', nummappers,
                u'-iteration', '5',
                u'-reducer', numreducers,
                u'-term', u'5000',
                u'-topic', numtopics,
                u'-directemit']
        args4 = [
                u'-input',
                tempdir + "ldapreout/gamma-5",
                u'-output',
                tempdir + "ldaout"]
        args5 = [u'--src', tempdir + "ldaout",
                 u'--dest', outdir + "ldaout"]
        args = (args1, args2, args3, args4, args5)
        steps = []
        names = ("Copy to hdfs", "Parse Corpus", "Variational Inference", "Display Document", "Copy from hdfs")
        for i in xrange(5):
            step = boto.emr.JarStep(names[i],
                           jars[i],
                           main_class=classes[i],
                           step_args=args[i],
                           )
            steps.append(step)
        master_instance_type = "m3.xlarge"
        slave_instance_type = "m3.xlarge"
        num_instances = 7
        jobid = conn.run_jobflow("Mr. LDA Risen From the Dead", log_uri=log_uri,
                                           steps=steps,
                                           master_instance_type=master_instance_type,
                                           slave_instance_type=slave_instance_type,
                                           num_instances=num_instances,
                                           enable_debugging=True,
                                           ami_version="latest",
                                           hadoop_version="2.2.0")
        wait_until(lambda: check_connection(conn, jobid), 86400)
    else:
        pass
        """
        Without elastic map reduce
        """
        """
        for i in xrange(2, 8):
            assert sys.argv[i]
        mahoutjar = sys.argv[2]
        outdir = sys.argv[3]
        nummappers = sys.argv[4]
        numreducers = sys.argv[5]
        numtopics = sys.argv[6]
        stopwords = sys.argv[7]
        call(["hadoop", "jar", mahoutjar, "org.apache.mahout.clustering.lda.cvb"])
        ###figure this out later, params are fucked and other stuff
        """
