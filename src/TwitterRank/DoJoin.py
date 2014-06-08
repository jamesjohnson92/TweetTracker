import sys, os, re, time
import simplejson, boto
import boto.s3.connection
from collections import defaultdict

access_key = os.environ["AWS_ACCESS_KEY"]
secret_key = os.environ["AWS_SECRET_KEY"]

#the join is on:
    #friend_id in followertable being same as twitter_id in gammas
    #follower_id in followertable being same as twitter_id in gammas

def progress_cb(bytes_transmitted, size):
    print "%d out of %d bytes done on the file you're DLing" % (bytes_transmitted, size)

def read_followertable(folder_name, bucket):
    keys = bucket.list(folder_name)
    followertable = defaultdict(int) #100mb about, it'll fit
    for f in keys:
        print f
        time.sleep(0.2)
        contents = f.get_contents_as_string(cb=progress_cb)
        for line in contents.split('\n'):
            split_line = line.split()
            if split_line and len(split_line) == 3:
                follower_id, friend_id, number = line.split()
                followertable[str((friend_id, follower_id))] = number
    return followertable

def key_iterator(key):
    unfinished_line = ""
    for byte in key:
        byte = unfinished_line + byte
        lines = byte_split("\n")
        unfinished_line = lines.pop()
        for line in lines:
            yield line

def do_join(folder_name, followertable, bucket):
    keys = bucket.list(folder_name)
    buf = [] #something
	count = 0
    for f in keys:
        f_iter = key_iterator(f)
        for line in f_iter:
			table_key = str((follower_id, friend_id))
			if table_key in followertable:
				count += 1
				newline = something something ####
				buf.append(newline)
				if count % 50000000 == 0:
					save this shit to remote ####@
					buf = []

def main(args):
    """
    Main method
    Rolling like it's 2006
    """
    conn = boto.connect_s3(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key)
    bucket = conn.get_bucket("tweettrack")
    if len(sys.argv) == 3:
        followertable = read_followertable(args[1], bucket)
        assert followertable is not None
        print "followertable is this long: %d, and we're saving it" % (len(followertable),)
        with open("followertable.json", "w") as followertable_file:
            simplejson.dump(followertable, followertable_file)
    else:
        with open(sys.argv[3], "r") as followertable_file:
            followertable = simplejson.load(followertable_file)
    do_join(args[2], followertable, bucket)
    conn.close()

if __name__ == "__main__":
    assert len(sys.argv) >= 3
    main(sys.argv)
