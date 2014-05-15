import boto, boto.emr, sys, time
from subprocess import call
from utils import *

if __name__ == "__main__":
    if sys.argv[1] == "emr":
        conn = boto.emr.connect_to_region("us-east-1") #north virginia
        s3_query_file_uri = u's3://mrldajarbucket/alphaphepreprocess.q'
        log_uri = u's3://tweettrack/AlphaPhi_Log/Hive_log'
        step1 = boto.emr.step.InstallHiveStep()
        steps = [step1]
        for i in xrange(int(sys.argv[4])):
                args = ["-hiveconf","APPATH=%s" % sys.argv[2],
                        "-hiveconf","TTPATH=%s" % sys.argv[3],
                        "-hiveconf","TOPIC=%d" % (i+1)]
                steps.append(boto.emr.step.HiveStep("Run alpha phi preprocess hive", s3_query_file_uri, hive_args=args))
                
        master_instance_type = "m3.xlarge"
        slave_instance_type = "m3.xlarge"
        jobid = conn.run_jobflow("AlphaPhi Hive Step, Bro", log_uri=log_uri,
                                           steps=steps,
                                           master_instance_type=master_instance_type,
                                           slave_instance_type=slave_instance_type,
                                           num_instances=1,
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
