from mrjob.protocol import JSONValueProtocol, RawValueProtocol
from mrjob.step import MRStep
from mrjob.job import MRJob

USERID_PREFIX = 'id:twitter.com:'


class AlphaFile(MRJob):

    OUTPUT_PROTOCOL = RawValueProtocol

    def configure_options(self):
        super(AlphaFile, self).configure_options()
        self.add_passthrough_option('--mode', default='alpha', type='string')



    def mapper(self, _, line):
        splitline = line.split(' ');
        if (self.options.mode == 'alpha'):
            yield splitline[0], (splitline[1],'1' == splitline[2])
        else:
            yield splitline[1], (splitline[0],'1' == splitline[2])

            
    def reducer(self, key, value):
        result = []
        r_count = 0
        for follower, retweeted in value:
            if retweeted:
                r_count = r_count + 1
            else:
                result.append(follower)

        yield None, "%s %d %s" % (key, r_count, ' '.join(result))

if __name__ == '__main__':
    AlphaFile.run()
