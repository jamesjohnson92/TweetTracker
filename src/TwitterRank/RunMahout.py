import boto, boto.emr, sys, time
from subprocess import call
from utils import *

if __name__ == "__main__":
    if sys.argv[1] == "emr":
        conn = boto.emr.connect_to_region("us-east-1") #north virginia
        log_uri = u's3://tweettrack/Twitterrank_Log/LDA_log'
        mahoutjar = unicode(sys.argv[2])
        outdir = unicode(sys.argv[3])
        nummappers = unicode(sys.argv[4])
        numreducers = unicode(sys.argv[5])
        numtopics = unicode(sys.argv[6])
        stopwords = unicode(sys.argv[7])
        class1 = u'org.apache.mahout.clustering.lda.cvb'
        classes = (class1,)
        #classes = (class1, class2, class3)

        args1 = [
                 u'--input', outdir + "corpus",
                 u'--output', outdir + "ldaout",
                 u'--num_reduce_tasks', numreducers,
                 u'--num_terms', u'10000',
                 u'--num_topics', numtopics]
        args = (args1,)
        #args = (args1, args2, args3)
        steps = []
        names = ("Mahout Job",)
        #names = ("Parse Corpus", "Variational Inference", "Display Document")
        for i in xrange(1):
            step = boto.emr.JarStep(names[i],
                           mahoutjar,
                           main_class=classes[i],
                           step_args=args[i],
                           #action_on_failure="CANCEL_AND_WAIT"
                           )
            steps.append(step)
        master_instance_type = "m3.xlarge"
        slave_instance_type = "m3.xlarge"
        num_instances = 2
        #step_args = [conn._build_step_args(step) for step in steps]
        #print step_args
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
        call(["hadoop", "jar", mahoutjar, "org.apache.mahout.clustering.lda.cvb"]) ###figure this out later
