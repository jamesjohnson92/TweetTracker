import boto, sys, time
from subprocess import call
#hadoop jar $mrldajar cc.mrlda.ParseCorpus -input $outdir/corpus -output $outdir/p
#hadoop jar $mrldajar cc.mrlda.VariationalInference -input $outdir/parsecorpus/doc
#hadoop jar $mrldajar cc.mrlda.DisplayDocument -input $outdir/ldapreout/gamma-30 -

def check_connection(conn, jobid):
	status = conn.describe_jobflow(jobid)
	#terminal state
	if status in [u'COMPLETED', u'FAILED', u'TERMINATED']:
		return True
	return False

def wait_until(pred, timeout, period=30):
	mustend = time.time() + timeout
	while time.time() < mustend:
		if pred(): return True
		time.sleep(period)

if __name__ == "__main__":
	if sys.argv[1] == "emr":
		conn = boto.emr.connect_to_region(something)
		mrlda_jar = u's3://somethingsomethingsomethingsomething'
		class_1 = u'cc.mrlda.ParseCorpus'
		class_2 = u'cc.mrlda.VariationalInference'
		class_3 = u'cc.mrlda.DisplayDocument'
###do the args properly
		args1 = [u's3://us-east-1.elasticmapreduce/libs/hive/hive-script',
				 u'--base-path',
				 u's3://us-east-1.elasticmapreduce/libs/hive/',
				 u'--install-hive',
				 u'--hive-versions',
				 u'0.7']
		args2 = [u's3://us-east-1.elasticmapreduce/libs/hive/hive-script',
				 u'--base-path',
				 u's3://us-east-1.elasticmapreduce/libs/hive/',
				 u'--hive-versions',
				 u'0.7',
				 u'--run-hive-script',
				 u'--args',
				 u'-f',
				 s3_query_file_uri]
		steps = []
		for name, args in zip(('Parse Corpus','Variational Inference','Display Document'),(args1,args2,args3)):
			step = JarStep(name,
						   's3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar',
						   step_args=args,
						   #action_on_failure="CANCEL_AND_WAIT"
						   )
			steps.append(step)
		jobid = conn.run_jobflow(name, s3_log_uri,
										   steps=steps,
										   master_instance_type=master_instance_type,
										   slave_instance_type=slave_instance_type,
										   num_instances=num_instances,
										   hadoop_version="0.20")
		wait_until(lambda: check_connection(conn, jobid), 86400)
	else:
		"""
		Without elastic map reduce
		"""
		for i in xrange(2, 8):
			assert sys.argv[i]
		mrldajar = sys.argv[2]
		outdir = sys.argv[3]
		nummappers = sys.argv[4]
		numreducers = sys.argv[5]
		numtopics = sys.argv[6]
		stopwords = sys.argv[7]
		call(["hadoop", "jar", mrldajar, "cc.mrlda.ParseCorpus", "-input", "%s/corpus" % outdir, "-output", "%s/parsecorpus" % outdir, "-mapper", nummappers, "-reducer", numreducers, "-stoplist", stopwords])
		call(["hadoop", "jar", mrldajar, "cc.mrlda.VariationalInference", "-input", "%s/parsecorpus/document" % outdir, "-output", "%s/ldapreout" % outdir, "-mapper", nummappers, "-reducer", numreducers, "-term", "10000", "-topic", numtopics, "-directemit"])
		call(["hadoop", "jar", mrldajar, "cc.mrlda.DisplayDocument", "-input", "%s/ldapreout/gamma-30" % outdir, "-output", "%s/ldaout" % outdir])
