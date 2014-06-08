

for i in range(30):
    print "On table %d" % i

    thedata = {}
    otheredges = {}

    lasttweet = 0

    
    with open('thedata/thedata%d/topictable/topictable' % (i+1),'r') as f:
        for line in f:
            splitline = line.split(' ')
            creator = int(splitline[1])
            if not creator in thedata:
                thedata[creator] = {}
                otheredges[creator] = []

                
            tweet = int(splitline[0])
            tweetor = int(splitline[2])
            if not tweet in thedata[creator]:
                thedata[creator][tweet] = {}

            if not tweetor in otheredges:
                otheredges[tweetor] = []

            thedata[creator][tweet][tweetor] = int(splitline[3])
        
    with open('thedata/tweettable/tweettable','r') as f:
        for line in f:
            splitline = line.split(' ')
            friend = int(splitline[1])
            follower = int(splitline[2])
            if friend in thedata:
                for tweet, table in thedata[friend].items():
                    if not follower in table:
                        table[follower] = -1
            if friend in otheredges:
                otheredges[friend].append(follower)

            

    for a,ax in thedata.items():
        for tweet, table in ax.items():
            for b, rt in table.items():
                if rt >= 0:
                    for c in otheredges[b]:
                        if not c in table:
                            table[c] = -1
                    

    with open('thedata/thedata%d/alphaphiin/part' % (i+1), 'w') as f:
        for user,ax in thedata.items():
            for tweet, table in ax.items():
                for k, v in table.items():
                    if not k == user:
                        if v >= 0:
                            print >> f, '%d %d 1' % (tweet,k)
                        else:
                            print >> f, '%d %d 0' % (tweet,k)
            
