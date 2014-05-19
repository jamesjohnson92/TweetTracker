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

master = "m1.large"
slave = "m3.2xlarge"
num_instances = 18
