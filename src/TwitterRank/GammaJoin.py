import sys, os, re, time, resource, gc
import ujson, boto
import boto.s3.connection
from collections import defaultdict

access_key = os.environ["AWS_ACCESS_KEY"]
secret_key = os.environ["AWS_SECRET_KEY"]

#the join is on:
    #friend_id in followertable being same as twitter_id in gammas
    #follower_id in followertable being same as twitter_id in gammas

#a question to ask yourself: why should I run this on a big-memory ec2 instance (r3 2xlarge for the win)?

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

def get_gammas(folder_name, bucket):
    """
    This is barely kosher because ldaout is 4.5 gigs
    Use a big fucking instance, accept that you are going to run out of memory
    """
    keys = bucket.list(folder_name)
    gammas = {}
    count = 0
    for f in keys:
        print f
        f_iter = key_iterator(f)
        for line in f_iter:
            count += 1
            split_line = line.split()
            if split_line and len(split_line) == 31:
                gammas[str(split_line[0])] = map(float, split_line[1:])
            if count % 100000 == 0:
                print "count is now : %d" % (count,)
                print "usage is : %d kb" % (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,)
    return gammas

def do_join(output_folder_name, followertable, gammas, bucket):
    """
    Goes through follower gammas and does the join, saves it to remote in real time like
    """
    buf = []
    count = 0
    save_size = 100000 
    for followtup, tweet_count in followertable.iteritems():
        tup = tuple(int(v) for v in re.findall("[0-9]+", followtup))
        friend_id = str(tup[0])
        follower_id = str(tup[1])
        if friend_id in gammas and follower_id in gammas:
            count += 1
            toJoin = []
            toJoin.append(follower_id)
            toJoin = toJoin + gammas[follower_id]
            toJoin.append(friend_id)
            toJoin.append(tweet_count)
            toJoin = toJoin + gammas[friend_id]
            joined_line = " ".join(map(str, toJoin))
            buf.append(joined_line)
            if count % save_size == 0:
                output_name = output_folder_name + "/part-m-000" + str(count // save_size)
                print "output name is: %s " % (output_name,)
                print "here's a thing from the buffer: %s", buf[1]
                new_file = bucket.new_key(output_name)
                new_file.set_contents_from_string("\n".join(buf))
                print "saving... %d" % (count // save_size,)
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
    if len(sys.argv) == 4:
        followertable = read_followertable(args[1], bucket)
        assert followertable is not None
        print "followertable is this long: %d, and we're saving it" % (len(followertable),)
        with open("followertable.json", "w") as followertable_file:
            ujson.dump(followertable, followertable_file)
    else:
        print "followerstable..."
        with open(sys.argv[4], "r") as followertable_file:
            followertable = ujson.load(followertable_file)
        print "followerstable done..."
        #print "gammas..."
        #with open(sys.argv[5], "r") as gamma_file:
        #    gammas = ujson.load(gamma_file)
        #    gc.collect()
        #print "gammas done..."
    gammas = get_gammas(args[2], bucket)
    #with open("gammas.json", "w") as gamma_file:
    #    ujson.dump(gammas, gamma_file)
    do_join(args[3], followertable, gammas, bucket)
    conn.close()

if __name__ == "__main__":
    assert len(sys.argv) >= 4
    main(sys.argv)
