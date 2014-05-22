import boto, boto.emr, sys, time
from subprocess import call
from utils import *

if __name__ == "__main__":
    conn = boto.emr.connect_to_region("us-east-1") #north virginia
    log_uri = u's3n://tweettrack/Twitterrank_Log/LDA_log'
    mrjobsjar, outdir, nummappers, numreducers, numtopics, stopwords, tempdir, s3distcpjar = read_mrjobs_args(sys.argv)
    jars = (s3distcpjar, mrjobsjar, s3distcpjar)
    varinfclass = u'cc.mrlda.VariationalInference'
    classes = (None, varinfclass, None)
    args1 = [u'--src', outdir + "parsecorpus",
             u'--dest', tempdir + "parsecorpus"]
    args2 = [
            u'-input',  tempdir + "parsecorpus/document",
            u'-output', tempdir + "ldapreout",
            u'-mapper', nummappers,
            u'-iteration', '5',
            u'-reducer', numreducers,
            u'-term', u'5000',
            u'-topic', numtopics,
            u'-directemit']
    args3 = [u'--src', tempdir + "ldapreout",
             u'--dest', outdir + "ldapreout"]
    args = (args1, args2, args3)
    steps = []
    names = ("Copy to hdfs", "Variational Inference", "Copy out from variational inf")
    for i in xrange(3):
        step = boto.emr.JarStep(names[i],
                       jars[i],
                       main_class=classes[i],
                       step_args=args[i],
                       )
        steps.append(step)
    jobid = conn.run_jobflow("Mr. LDA Variational Inference", log_uri=log_uri,
                                       steps=steps,
                                       master_instance_type=master, #see utils
                                       slave_instance_type=slave,
                                       num_instances=num_instances,
                                       enable_debugging=True,
                                       ami_version="latest",
                                       hadoop_version="2.2.0")
    wait_until(lambda: check_connection(conn, jobid), 86400)
