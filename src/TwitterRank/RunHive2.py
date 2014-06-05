import boto, boto.emr, sys, time
from subprocess import call
from utils import *

if __name__ == "__main__":
    if sys.argv[1] == "emr":
        conn = boto.emr.connect_to_region("us-east-1") #north virginia
        s3_query_file_uri = u's3://mrldajarbucket/ldapostprocess2.q'
        log_uri = u's3://tweettrack/Twitterrank_Log/Hive_log'
        outdir = sys.argv[2]
        tempdir = sys.argv[3]
        s3distcpjar = sys.argv[4]
        postprocess_args = ["-hiveconf","TROPATH=%s" % tempdir]
        print postprocess_args, tempdir
###figure out subfolder
        step1 = boto.emr.JarStep("Copy to hdfs", s3distcpjar, step_args=[u'--src', outdir + "followertable", u'--dest', tempdir + "followertable"])
        step2 = boto.emr.JarStep("Copy to hdfs", s3distcpjar, step_args=[u'--src', outdir + "ldaout", u'--dest', tempdir + "ldaout"])
        step3 = boto.emr.step.InstallHiveStep()
        step4 = boto.emr.step.HiveStep("Run lda postprocess hive 2", s3_query_file_uri, hive_args=postprocess_args)
        step5 = boto.emr.JarStep("Copy from hdfs", s3distcpjar, step_args=[u'--src', tempdir + "pregraph", u'--dest', outdir + "pregraph"])
        step6 = boto.emr.JarStep("Copy from hdfs", s3distcpjar, step_args=[u'--src', tempdir + "gammasums", u'--dest', outdir + "gammasums"])
        steps = [step1, step2, step3, step4, step5, step6]
        boot_params = ['-s', 'mapred.map.max.attempts=5',
            '-s', 'mapred.child.java.opts=-Xmx5G -XX:+UseConcMarkSweepGC -XX:-UseGCOverheadLimit'] #turn off gc upside
        config_bootstrapper = boto.emr.BootstrapAction('Embiggen memory', 's3://elasticmapreduce/bootstrap-actions/configure-hadoop', boot_params)
        jobid = conn.run_jobflow("Hive step 2, bro", log_uri=log_uri,
                                           steps=steps,
                                           master_instance_type=master, ##see utils
                                           slave_instance_type=slave,
                                           num_instances=num_instances,
                                           bootstrap_actions=[config_bootstrapper],
                                           enable_debugging=True,
										   keep_alive=True,
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
