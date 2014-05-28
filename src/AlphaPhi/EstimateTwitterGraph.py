from mrjob.protocol import JSONValueProtocol, RawValueProtocol
from mrjob.step import MRStep
from mrjob.job import MRJob
from scipi.stats import binom

class EstimateTwitterGraph(MRJob):

    OUTPUT_PROTOCOL = RawValueProtocol

    def mapper(self, _, line):
        sp = line.split(' ')
        friend = int(sp[0])
        follower = int(sp[1])
        fu = int(sp[2])
        ruu = int(sp[3])
        ru = int(sp[4])
        yield (friend, fu, ru), (follower, ruu)

    def reducer(self, key, values):
        friend = sk[0]
        fu = sk[1]
        ru = sk[2]
        hist = {} # asumed to be very small relative to fs
        fs = [] # hopefully not too too big, maybe in the hundreds of thousands.  

        for follower, ruu in values:
            fs.append((follower,ruu))
            if ruu in hist:
                hist[ruu] = hist[ruu] + 1
            else:
                hist[ruu] = 1

        cdf = {}
        for k,v in hist:
            cdf[k] = v
            for k2, v2 in hist:
                if k2 < k1:
                    cdf[k] = cdf[k] + v2

        for follower, ruu in fs:
            edgeprob = (fu//cdf[ruu]) * binom.sf(ruu,ru,1//fu)
            yield '%d %d %f ' % (friend, follower, edgeprob)

if __name__ == '__main__':
    EstimateTwitterGraph.run()
