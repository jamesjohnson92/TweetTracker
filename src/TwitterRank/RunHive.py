import boto, boto.emr, sys, time
from subprocess import call
from utils import *

if __name__ == "__main__":
    if sys.argv[1] == "emr":
        conn = boto.emr.connect_to_region("us-east-1") #north virginia
        s3_query_file_uri = u's3://mrldajarbucket/ldapostprocess.q'
        log_uri = u's3://tweettrack/Twitterrank_Log/Hive_log'
        postprocess_args = ["-hiveconf","TROPATH=%s" % sys.argv[2]]
        print postprocess_args
        step1 = boto.emr.step.InstallHiveStep()
        step2 = boto.emr.step.HiveStep("Run lda postprocess hive", s3_query_file_uri, hive_args=postprocess_args)
        steps = [step1, step2]
        master_instance_type = "m3.xlarge"
        slave_instance_type = "m3.xlarge"
        jobid = conn.run_jobflow("Hive step, bro", log_uri=log_uri,
                                           steps=steps,
                                           master_instance_type=master_instance_type,
                                           slave_instance_type=slave_instance_type,
                                           num_instances=6,
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
