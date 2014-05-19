import boto, boto.emr, sys, time
from subprocess import call
from utils import *

if __name__ == "__main__":
    if sys.argv[1] == "emr":
        conn = boto.emr.connect_to_region("us-east-1")
        s3_query_file_uri = u's3://mrldajarbucket/tweettopichive.q' ################
        log_uri = u's3://tweettrack/Twitterrank_Log/TweetTopicHive_log'
        srcdir = sys.argv[2]
        tempdir = sys.argv[3]
        postprocess_args = ["-hiveconf","TTPATH=%s" % tempdir]
        print postprocess_args
        s3distcpjar = unicode(sys.argv[4])
        s3distcpargs0 = [u'--src', srcdir, u'--dest', tempdir]
        s3distcpargs4 = [u'--src', tempdir, u'--dest', srcdir]
        step0 = boto.emr.JarStep("Copy to hdfs", s3distcpjar, step_args=s3distcpargs0)
        step1 = boto.emr.step.InstallHiveStep()
        step2 = boto.emr.step.HiveStep("Run tweet topic hive", s3_query_file_uri, hive_args=postprocess_args)
        step3 = boto.emr.JarStep("Copy from hdfs", s3distcpjar, step_args=s3distcpargs4)
        steps = [step0, step1, step2, step3]
        master_instance_type = "m3.xlarge"
        slave_instance_type = "m3.xlarge"
        jobid = conn.run_jobflow("Hive step for tweet topic, man", log_uri=log_uri,
                                           steps=steps,
                                           master_instance_type=master_instance_type,
                                           slave_instance_type=slave_instance_type,
                                           num_instances=6,
                                           enable_debugging=True,
                                           ami_version="latest",
                                           hadoop_version="2.2.0")
        wait_until(lambda: check_connection(conn, jobid), 86400)
    else:
        pass
        """
        Without elastic map reduce: explore
        """
