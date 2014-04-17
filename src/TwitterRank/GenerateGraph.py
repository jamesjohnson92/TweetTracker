from mrjob.protocol import JSONProtocol
from mrjob.step import MRStep


def encode_node(node_id, links, teleport_prob, score):
    node = {}
    node['links'] = sorted(links.items())

    node['score'] = score
    node['telep_prob'] = teleport_prob

    return JSONProtocol.write(node_id, node) + '\n'


class GenerateTRGrapg(MRJob):

    def configure_options(self):
        super(MRPageRank, self).configure_options()
        self.add_passthrough_option('--numtopics', default=5, type='int')

    # Mapper assumes each value comes in in the form
    # <src> <src_gamma> <trg> <trg_num_tweets> <trg_gamma>
    def mapper(self, _, line):
        splitline = line.split(' ')
        yield ' '.join(splitline[0:1+self.option.numtopics]), ' '.join(splitline[1+self.option.numtopics:-1])
        

    # It is assumed each value is coming to the reducer in the form
    # <trg> <trg_num_tweets> <trg_gamma_1> ... <trg_gamma_T>
    # and also somehow src_gamma is known
    # where <src> and <trg> are the source and target of a twitter following relationship (src follows target)
    # and the gammas were computed from LDA
    def reducer(self, key, vals):
        splitkey = key.split(' ')
        src = int(splitkey[0])
        src_gamma = map(float, splitkey[1:1+self.option.numtopics])
        links = []
        out_tweets = 0
        src_gamma = []
        src_gamma_sum = 0
        for line in vals:
            splitline = line.split(' ')
            trg = splitline[0]
            trg_num_tweets = int(splitline[1])
            out_tweets += trg_num_tweets
            trg_gamma = map(float, splitline[2:2+self.option.numtopics])
            trg_gamma_sum = sum(trg_gamma)
            links.append((trg, trg_num_tweets, trg_gamma))
                
        for i in (len(links)):
            trg, trg_num_tweets, trg_gamma = links[i]
            weights = [(1 - abs(sg/src_gamma_sum - tg/trg_gamma_summ))/out_tweets for sg,tg in zip (src_gamma,trg_gamma)]
            links[i] = (trg, weights)

        # TODO(JoYo): src_telep_prob
        src_telep_prob = []
            
        yield src, encode_node(src, links, src_telep_prob, [1] * len(src_telep_prob))
    
