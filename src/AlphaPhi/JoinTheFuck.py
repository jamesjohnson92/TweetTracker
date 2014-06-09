from math import log


def ratedatacascade(history,alphas):
    j = 3
    res = []
    for i in range(30):
        while j < len(history) and history[j] <= 2**(i+1):
            j = j + 1
        if j < len(history):
            res.append(20*(j-3))
        else:
            res.append(-1)

    for i in range(30):
        if res[i] == -1:
            res.append(-1)
        else:
            print res[i]
            ix = (res[i] - (res[i]%30))/30
            res.append(alphas[min(len(alphas)-1,(res[i] - (res[i]%30))/30)])
        

    return ' '.join(map(str,res))
    

thedata = {}
thealphas = {}

for i in range(30):
    print "On table %d" % i
    
    for dt in xrange(30):
        print "dt = %d" % ((dt+1)*30)
        with open('thedata/thedata%d/alpha_estimate_time%d' % (i+1,(dt+1)*30), 'r') as f:
            for line in f:
                splitline = line.split(' ')
                tweet = int(splitline[0])
                alpha = int(splitline[1])
                if not tweet in thedata:
                    thedata[tweet] = [-1]*30
                thedata[tweet][dt] = alpha

    with open('alpha_estimateout%d_8' % (i+1),'r') as f:
            for line in f:
                splitline = line.split(' ')
                tweet = int(splitline[0])
                alpha = float(splitline[1])
                thealphas[tweet] = alpha

ratedata = {}

with open('thedata/retweetrate/retweetrate','r') as f:
    for line in f:
        splitline = line.split(' ')
        if int(splitline[0]) in thedata:
            ratedata[int(splitline[0])] = map(int,splitline[1:184])


phisumdata = {}
with open('thedata/phi_sums','r') as f:
    for line in f:
        splitline = line.split(' ')
        phisumdata[int(splitline[0])] = ' '.join(splitline[1:31])[:-1]
    
with open('thedata/the_alpha_phi_table', 'w') as f:
    for tweet,alphas in thedata.items():
        if tweet in ratedata:
            print >> f, '%d %f %d %d %s %s %s' % (tweet,thealphas[tweet],ratedata[tweet][0],ratedata[tweet][1],
                                                  phisumdata[tweet],
                                                  ' '.join(map(lambda x: str(x/7.0),alphas)),
                                                  ' '.join(map(lambda x: str(log(x+1)),ratedata[tweet][3:])))

with open('thedata/the_alpha_phi_cascade_table', 'w') as f:
    for tweet,alphas in thedata.items():
        if tweet in ratedata:
            print >> f, '%d %f %d %d %s' % (tweet,thealphas[tweet],ratedata[tweet][0],ratedata[tweet][1],
                                            ratedatacascade(ratedata[tweet],alphas))
