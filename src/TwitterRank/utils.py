import boto, boto.emr, sys, time

def check_connection(conn, jobid):
    status = conn.describe_jobflow(jobid).state
    #terminal state
    print status
    if status in [u'COMPLETED', u'FAILED', u'TERMINATED']:
        return True
    return False

def wait_until(pred, timeout, period=30):
    mustend = time.time() + timeout
    while time.time() < mustend:
        if pred(): return True
        print "Checked: ", time.time()
        time.sleep(period)

def read_mrjobs_args(args):
    mrjobsjar = unicode(args[2])
    outdir = unicode(args[3])
    nummappers = unicode(args[4])
    numreducers = unicode(args[5])
    numtopics = unicode(args[6])
    stopwords = unicode(args[7])
    tempdir = unicode(args[8])
    s3distcpjar = unicode(args[9])
    return (mrjobsjar, outdir, nummappers, numreducers, numtopics, stopwords, tempdir, s3distcpjar)

master = "m1.large"
slave = "r3.xlarge"
num_instances = 6
