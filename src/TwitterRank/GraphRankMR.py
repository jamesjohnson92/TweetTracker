# Map-Reduce GraphRank 
from mrjob.job import MRJob
import mmap
import struct

FLOATSIZE = 4

# Uses MRJob to compute graph rank on an arbitrary stochastic graph with arbitrary transition probability vector.
# Will work best if matrix is stored in column major order, so that we don't have to do so much IO if
# rank_estimates is OOC.  
# 
class GraphRankJob(MRJob):

    def configure_options(self):
        super(GraphRankJob, self).configure_options()
        self.add_file_option('--rankfile')
        self.add_file_option('--telepprobs')
        self.add_passthrough_option('--beta',default = '0.85')

    def mapper_init(self):
        self.rank_estimate = array('d')
        self.open_file = open(self.options.rankefile, 'r')
        self.rank_estimates = mmap.mmap(self.open_file.fileno(),0)
        

    def mapper_final(self):
        self.rank_estimates.close()
        self.open_file.close()

    # Mapper Takes key = (i,j) and in_value = P_t(i,j)
    # The extra parameter is the an array representing the current estimate of the rank
    #
    def mapper(self, _, line):
        splitline = line.split(' ')
        yeild (int(splitline[0]), float(splitline[2]) * self.rank_estimate(int(splitline[1])))

    # Obviouse Optimization
    def combiner(self, in_key, in_values):
        yeild (in_key, sum(in_values))

    def reducer_init(self):
        self.open_file = open(self.options.telepprobs, 'r')
        self.teleportation_vector = mmap.mmap(self.open_file.fileno(),0)
        self.options.beta = float(self.options.beta)

    def teleport_prob(self, i):
        return struct.unpack(self.teleportation_vector[i * FLOATSIZE : (i+1) * FLOATSIZE])
    def rank_estimate(self, i):
        return struct.unpack(self.rank_estimates[i * FLOATSIZE : (i+1) * FLOATSIZE])

    
    # Reducer takes key = row index, and values = the entries from that row times the current rank estimates
    # the extra parameters are the beta param and teleportation vector
    #
    def reducer(self, in_key, in_values):
        yeild (None, self.options.beta * sum(in_values) + (1-self.beta) * self.teleport_prob(in_key))    

    def reducer_final(self):
        self.teleportation_vector.close()
        self.open_file.close()

