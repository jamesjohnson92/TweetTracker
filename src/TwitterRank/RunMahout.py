import boto, boto.emr, sys, time
from subprocess import call
from utils import *

if __name__ == "__main__":
    if sys.argv[1] == "emr":
        conn = boto.emr.connect_to_region("us-east-1") #north virginia
        log_uri = u's3n://tweettrack/Twitterrank_Log/LDA_log'
        mahoutjar = unicode(sys.argv[2])
        outdir = unicode(sys.argv[3])
        nummappers = unicode(sys.argv[4])
        numreducers = unicode(sys.argv[5])
        numtopics = unicode(sys.argv[6])
        stopwords = unicode(sys.argv[7])

		jars = (s3distcpjar, mahoutjar, mahoutjar, mahoutjar, s3distcpjar)
		class1 = u'stuff'
        class2 = u'org.apache.mahout.text.SequenceFilesFromDirectory'
        class3 = u'org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles'
        class4 = u'org.apache.mahout.clustering.lda.cvb.CVB0Driver'
		class5 = u'stuff'
        classes = (class1, class2, class3, class4, class5)
		args1 = []
        args2 = [
                 u'--input', outdir + "corpus",
                 u'--output', outdir + "seqfiles",
                 u'-c', u'UTF-8'
                ]
        args3 = [
                 u'--input', outdir + "seqfiles",
                 u'--output', outdir + "vecs",
                 u'-wt', u't'
                ]
        args4 = [
                 u'--input', outdir + "vecs",
                 u'--output', outdir + "ldaout",
                 u'--num_reduce_tasks', numreducers,
                 u'--num_terms', u'10000',
                 u'--maxIter', u'3000',
                 u'--num_topics', numtopics]
		args4 = []
        args = (args1, args2, args3, args4, args5)
        steps = []
        names = ("Copy to hdfs", "Convert to SequenceFile", "Sequence File to Parsed", "Mahout Job", "Copy from hdfs")
        for i in xrange(5):
            step = boto.emr.JarStep(names[i],
                           jars[i],
                           main_class=classes[i],
                           step_args=args[i],
                           )
            steps.append(step)
        master_instance_type = "m3.xlarge"
        slave_instance_type = "m3.xlarge"
        num_instances = 2
        jobid = conn.run_jobflow("Mahout", log_uri=log_uri,
                                           steps=steps,
                                           master_instance_type=master_instance_type,
                                           slave_instance_type=slave_instance_type,
                                           num_instances=num_instances,
                                           enable_debugging=True,
                                           ami_version="latest",
                                           hadoop_version="2.2.0")
        wait_until(lambda: check_connection(conn, jobid), 86400)
    else:
        """
        Without elastic map reduce
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
