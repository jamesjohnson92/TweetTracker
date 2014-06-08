

def fmt(x):
    if x[0] == 0:
        return "1"
    return str(x[1]/float(x[0]))

thedata = {}
thephis = {}

for i in range(30):

    print "topic = %d" % i
    
    with open('phi_estimateout%d_8' % (i+1), 'r') as f:
        for line in f:
            splitline = line.split(' ')
            thephis[int(splitline[0])] = float(splitline[1])
            
    
    with open('thedata/thedata%d/topictable/topictable' % (i+1), 'r') as f:
        for line in f:
            splitline = line.split(' ')
            if not splitline[1] == splitline[2]:
                tweet = int(splitline[0])
                tweeter = int(splitline[2])
                dt = int(splitline[3])
                if not tweet in thedata:
                    thedata[tweet] = [(0,0.0)] * 30
                ix = dt/30
                for j in range(ix,30):
                    thedata[tweet][j] = (1 + thedata[tweet][j][0],thephis[tweeter] + thedata[tweet][j][1])

with open('thedata/phi_sums', 'w') as f:
    for tweet, ls in thedata.items():
        print >>f, "%d %s" % (tweet, ' '.join(map(fmt,ls)))
