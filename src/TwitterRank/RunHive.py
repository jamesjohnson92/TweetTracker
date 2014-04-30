import boto, boto.emr, sys, time
from subprocess import call
from utils import *

if __name__ == "__main__":
    if sys.argv[1] == "emr":
        conn = boto.emr.connect_to_region("us-east-1") #north virginia
        s3_query_file_uri = u's3://mrldajarbucket/ldapostprocess_stub.q'
        log_uri = u's3://tweettrack/Twitterrank_Log/Hive_log'
        args1 = [u's3://us-west-2.elasticmapreduce/libs/hive/hive-script',
                 u'--base-path',
                 u's3://us-west-2.elasticmapreduce/libs/hive/',
                 u'--install-hive',
                 u'--hive-versions',
                 u'0.11.0.2']
        args2 = [u's3://us-west-2.elasticmapreduce/libs/hive/hive-script',
                 u'--base-path',
                 u's3://us-west-2.elasticmapreduce/libs/hive/',
                 u'--hive-versions',
                 u'0.11.0.2',
                 u'--run-hive-script',
                 u'--args',
                 u'-f',
                 s3_query_file_uri]
        steps = []
        for name, args in zip(('Setup Hive','Run Hive Script'),(args1,args2)):
            step = boto.emr.JarStep(name,
                           's3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar',
                           step_args=args,
                           #action_on_failure="CANCEL_AND_WAIT"
                           )
            steps.append(step)
        master_instance_type = "m1.small"
        slave_instance_type = "m1.xlarge"
        jobid = conn.run_jobflow(name, log_uri=log_uri,
                                           steps=steps,
                                           master_instance_type=master_instance_type,
                                           slave_instance_type=slave_instance_type,
                                           num_instances=5,
                                           enable_debugging=True,
                                           hadoop_version="0.20")
        wait_until(lambda: check_connection(conn, jobid), 86400)
    else:
        """
        Without elastic map reduce
        """
        assert sys.argv[2]
        outdir_str = "TROPATH=%s" % sys.argv[2]
        call(["hive", "-hiveconf", outdir_str, "-f", "ldapostprocess.q"])
