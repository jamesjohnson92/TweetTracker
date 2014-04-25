from mrjob.protocol import JSONProtocol
from mrjob.step import MRStep
from mrjob.job import MRJob



class SGD(MRJob):

    def configure_options(self):
        super(SGD, self).configure_options()
        self.add_passthrough_option('--numiterations', default=5, type='int')
        self.add_passthrough_option('--learningrate', default=0.1, type='float')

    # Mapper assumes each value comes in in the form
    # <s> <u> <tau_s> <b_su> <phi_ts> <alpha_s>
    def mapper(self, _, line):
        splitline = line.split(' ')
        yield ('alpha',int(splitline[0])), (int(splitline[3]), float(splitline[4]), float(splitline[5]))
        yield ('phi', int(splitline[1]), int(splitline[2])), (int(splitline[3]), float(splitline[4]), float(splitline[5]))

    def reducer(self, key, vals):
        result = 0
        count = 0
        if key[0] == 'alpha':  # this is the update of alpha
            for s, b, phi, alpha in vals:
                count += 1
                if b == 1:
                    result += alpha + self.option.learningrate * (1/alpha)
                else:
                    result += alpha - self.option.learningrate * phi / (1-alpha * phi)
        else: # this is the update of phi
            for b, phi, alpha in vals:
                count += 1
                if b == 1:
                    result += phi + self.option.learningrate * (1/phi)
                else:
                    result += phi - self.option.learningrate * alpha / (1-alpha * phi)
        result = result/count
        yield key, result
    
if __name__ == '__main__':
    SGD.run()
