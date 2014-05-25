import boto, boto.emr, sys, time
from subprocess import call
from utils import *

if __name__ == "__main__":
    conn = boto.emr.connect_to_region("us-east-1") #north virginia
    log_uri = u's3n://tweettrack/Twitterrank_Log/LDA_log'
    mrjobsjar, outdir, nummappers, numreducers, numtopics, stopwords, tempdir, s3distcpjar = read_mrjobs_args(sys.argv)
    jars = (s3distcpjar, mrjobsjar, s3distcpjar)
    dispdocclass = u'cc.mrlda.DisplayDocument'
    classes = (None, dispdocclass, None)
    args1 = [u'--src', outdir + "ldapreout",
             u'--dest', tempdir + "ldapreout"]
    args2 = [
            u'-input',
            tempdir + "ldapreout/gamma-5",
            u'-output',
            tempdir + "ldaout"]
    args3 = [u'--src', tempdir + "ldaout",
             u'--dest', outdir + "ldaout"]
    args = (args1, args2, args3)
    steps = []
    names = ("Copy to hdfs", "Display Document", "Copy out from disp")
    for i in xrange(3):
        step = boto.emr.JarStep(names[i],
                       jars[i],
                       main_class=classes[i],
                       step_args=args[i],
                       )
        steps.append(step)
    jobid = conn.run_jobflow("Mr. LDA Display Documents", log_uri=log_uri,
                                       steps=steps,
                                       master_instance_type=master, #see utils
                                       slave_instance_type=slave,
                                       num_instances=num_instances,
                                       enable_debugging=True,
                                       ami_version="latest",
                                       hadoop_version="2.2.0")
    wait_until(lambda: check_connection(conn, jobid), 86400)
