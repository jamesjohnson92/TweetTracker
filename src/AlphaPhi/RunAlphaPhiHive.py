import boto, boto.emr, sys, time
from subprocess import call
from utils import *

if __name__ == "__main__":
    if sys.argv[1] == "emr":
        conn = boto.emr.connect_to_region("us-east-1") #north virginia
        s3_query_file_uri = u's3://mrldajarbucket/alphaphepreprocess.q'
        log_uri = u's3://tweettrack/AlphaPhi_Log/Hive_log'
        tempdir = sys.argv[5]
        s3distcpjar = sys.argv[6]
        steps = []
        steps.append(boto.emr.JarStep("Copy to hdfs", s3distcpjar, step_args=[u'--src', sys.argv[2] + "/tweettable",
                                                                              u'--dest', tempdir + "/tweettable"]))
        steps.append(boto.emr.JarStep("Copy to hdfs", s3distcpjar, step_args=[u'--src', sys.argv[3] + "/tt_file",
                                                                              u'--dest', tempdir + "/tweettopics"]))
        steps.append(boto.emr.step.InstallHiveStep())
        for i in xrange(int(sys.argv[4])):
                args = ["-hiveconf","PATH=%s" % tempdir,
                        "-hiveconf","TOPIC=%d" % (i+1)]
                steps.append(boto.emr.step.HiveStep("Run alpha phi preprocess hive", s3_query_file_uri, hive_args=args))
#                steps.append(boto.emr.JarStep("Copy from hdfs", s3distcpjar, step_args=[u'--src', tempdir + "/alphaphiin%d" % (i+1),
 #                                                                                       u'--dest', sys.argv[2] + "/alphaphiin%d" % (i+1)]))
                steps.append(boto.emr.JarStep("Copy from hdfs", s3distcpjar, step_args=[u'--src', tempdir + "/topictweettable%d" % (i+1),
                                                                                        u'--dest', sys.argv[2] + "/topictweettable%d" % (i+1)]))

                
        master_instance_type = "c3.xlarge"
        slave_instance_type = "c3.xlarge"
        jobid = conn.run_jobflow("AlphaPhi Hive Step, Bro", log_uri=log_uri,
                                           steps=steps,
                                           master_instance_type=master_instance_type,
                                           slave_instance_type=slave_instance_type,
                                           num_instances=5,
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
