import boto, boto.emr, sys, time
from subprocess import call
from utils import *

if __name__ == "__main__":
    conn = boto.emr.connect_to_region("us-east-1") #north virginia
    log_uri = u's3n://tweettrack/Twitterrank_Log/LDA_log'
    mrjobsjar, outdir, nummappers, numreducers, numtopics, stopwords, tempdir, s3distcpjar = read_mrjobs_args(sys.argv)
    jars = (s3distcpjar, mrjobsjar, s3distcpjar)
    parseclass = u'cc.mrlda.ParseCorpus'
    classes = (None, parseclass, None)
    args1 = [u'--src', outdir + "corpus",
             u'--dest', tempdir + "corpus"]
    args2 = [
            u'-input', tempdir + "corpus",
            u'-output', tempdir + "parsecorpus",
            u'-mapper', nummappers,
            u'-reducer', numreducers,
            u'-stoplist', stopwords]
    args3 = [u'--src', tempdir + "parsecorpus",
             u'--dest', outdir + "parsecorpus"]
    boot_params = ['-s', 'mapred.map.max.attempts=5',
			'-s', 'mapred.child.java.opts=-Xmx15G -XX:+UseConcMarkSweepGC -XX:-UseGCOverheadLimit'] #turn off gc upside
    config_bootstrapper = boto.emr.BootstrapAction('Embiggen memory', 's3://elasticmapreduce/bootstrap-actions/configure-hadoop', boot_params)
    args = (args1, args2, args3)
    steps = []
    names = ("Copy to hdfs", "Parse Corpus", "Copy out from parse")
    for i in xrange(3):
        step = boto.emr.JarStep(names[i],
                       jars[i],
                       main_class=classes[i],
                       step_args=args[i],
                       )
        steps.append(step)
    jobid = conn.run_jobflow("Mr.LDA for Parsing Corpus", log_uri=log_uri,
                                       steps=steps,
                                       master_instance_type=master, #see utils
                                       slave_instance_type=slave,
                                       num_instances=num_instances,
                                       bootstrap_actions=[config_bootstrapper],
                                       enable_debugging=True,
                                       ami_version="latest",
                                       hadoop_version="2.2.0")
    wait_until(lambda: check_connection(conn, jobid), 86400)
