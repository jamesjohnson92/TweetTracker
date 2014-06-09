import sys, os, re, time, resource, gc
import ujson, boto
import boto.s3.connection
from collections import defaultdict

access_key = os.environ["AWS_ACCESS_KEY"]
secret_key = os.environ["AWS_SECRET_KEY"]

def key_iterator(key):
    """
    Iterator for line by line, for going through the whole contents of a key
    """
    unfinished_line = ""
    for byte in key:
        byte = unfinished_line + byte
        lines = byte.split("\n")
        unfinished_line = lines.pop()
        for line in lines:
            yield line

def main(args):
    """
    Main method
    Rolling like it's 2006
    """
    conn = boto.connect_s3(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key)
    bucket = conn.get_bucket("tweettrack")
    count = 0
    keys = bucket.list("Twitterrank_Full_Output/graph")
    for f in keys:
        print f
        f_iter = key_iterator(f)
        for line in f_iter:
            count += 1
            if line.find("score") == -1:
                print "fuck this shit, we fucked up"
                print line
                sys.exit(0)
            if count % 50000 == 0:
                print "count is: %d" % (count,)
    conn.close()

if __name__ == "__main__":
    main(sys.argv)
