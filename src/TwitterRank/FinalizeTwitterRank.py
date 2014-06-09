from mrjob.protocol import JSONProtocol, RawValueProtocol
from mrjob.step import MRStep
from mrjob.job import MRJob

USERID_PREFIX = 'id:twitter.com:'


class FinalizeTwitterRank(MRJob):

    INPUT_PROTOCOL = JSONProtocol
    MAPPER_OUTPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    def mapper(self, key, user):
        yield None, ("%d %s " % (key, ' '.join(map(str,user["score"]))))

if __name__ == '__main__':
    FinalizeTwitterRank.run()
