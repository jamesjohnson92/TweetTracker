from mrjob.protocol import JSONProtocol
from mrjob.step import MRStep
from mrjob.job import MRJob


def encode_node(links, teleport_prob, score):
    node = {}
    node['links'] = sorted(links)

    node['score'] = score
    node['telep_prob'] = teleport_prob

    return node


class GenerateTRGraph(MRJob):

    def configure_options(self):
        super(GenerateTRGraph, self).configure_options()
        self.add_passthrough_option('--numtopics', default=5, type='int')
        self.add_file_option('--sumgamma')

    # Mapper assumes each value comes in in the form
    # <src> <src_gamma> <trg> <trg_num_tweets> <trg_gamma>
    def mapper(self, _, line):
        splitline = line.split(' ')
        yield ' '.join(splitline[0:1+self.options.numtopics]), ' '.join(splitline[1+self.options.numtopics:])
        yield ' '.join([splitline[1+self.options.numtopics]] + splitline[3+self.options.numtopics:]), 'no_val'
        

    def reducer_init(self):
        with open(self.options.sumgamma, "r") as f:
            self.gamma_sums = map(float, f.read().split(' '))

    # It is assumed each value is coming to the reducer in the form
    # <trg> <trg_num_tweets> <trg_gamma_1> ... <trg_gamma_T>
    # and also somehow src_gamma is known
    # where <src> and <trg> are the source and target of a twitter following relationship (src follows target)
    # and the gammas were computed from LDA
    def reducer(self, key, vals):
        splitkey = key.split(' ')
        src = int(splitkey[0])
        src_gamma = map(float, splitkey[1:])
        links = []
        out_tweets = 0
        src_gamma_sum = sum(src_gamma)
        for line in vals:
            if line == 'no_val':
                continue
            splitline = line.split(' ')
            trg = int(splitline[0])
            trg_num_tweets = int(splitline[1])
            out_tweets += trg_num_tweets
            trg_gamma = map(float, splitline[2:])
            links.append((trg, trg_num_tweets, trg_gamma))
                
        for i in xrange(len(links)):
            trg = links[i][0]
            trg_num_tweets = links[i][1]
            trg_gamma = links[i][2]
            trg_gamma_sum = sum(trg_gamma)
            weights = [(1 - abs(sg/src_gamma_sum - tg/trg_gamma_sum)) * trg_num_tweets / out_tweets for sg,tg in zip (src_gamma,trg_gamma)]
            links[i] = (trg, weights)

        src_telep_prob = [g/sg for g,sg in zip(src_gamma,self.gamma_sums)]
            
        yield src, encode_node(links, src_telep_prob, [1] * len(src_telep_prob))
    
if __name__ == '__main__':
    GenerateTRGraph.run()
