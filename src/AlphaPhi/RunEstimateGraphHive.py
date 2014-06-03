import boto, boto.emr, sys, time
from subprocess import call
from utils import *

if __name__ == "__main__":
    if sys.argv[1] == "emr":
        conn = boto.emr.connect_to_region("us-east-1") #north virginia
        s3_query_file_uri = u's3://mrldajarbucket/estimategraph.q'
        log_uri = u's3://tweettrack/AlphaPhi_Log/Hive_log'
        outdir = sys.argv[2]
        tempdir = sys.argv[3]
        s3distcpjar = sys.argv[4]
        postprocess_args = ["-hiveconf","APPATH=%s" % tempdir]
        print postprocess_args, tempdir
###figure out subfolder
        step1 = boto.emr.JarStep("Copy to hdfs", s3distcpjar, step_args=[u'--src', outdir + "tweettable", u'--dest', tempdir + "tweettable"])
        step2 = boto.emr.step.InstallHiveStep()
        step3 = boto.emr.step.HiveStep("Run hive", s3_query_file_uri, hive_args=postprocess_args)
        step4 = boto.emr.JarStep("Copy from hdfs", s3distcpjar, step_args=[u'--src', tempdir + "twitterpregraph", u'--dest', outdir + "twitterpregraph"])
        steps = [step1, step2, step3, step4]
        jobid = conn.run_jobflow("Hive step for estimate twitter graph, bro", log_uri=log_uri,
                                           steps=steps,
                                           master_instance_type=master, ##see utils
                                           slave_instance_type=slave,
                                           num_instances=num_instances,
                                           enable_debugging=True,
                                           ami_version="latest",
                                           hadoop_version="2.2.0")
        wait_until(lambda: check_connection(conn, jobid), 86400)
    else:
        """
        Without elastic map reduce
        """
        pass
        """
        assert sys.argv[2]
        outdir_str = "TROPATH=%s" % sys.argv[2]
        call(["hive", "-hiveconf", outdir_str, "-f", "ldapostprocess.q"])
        """
