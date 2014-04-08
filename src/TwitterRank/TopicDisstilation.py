# THIS IS BAD CODE

import pymc
import numpy as np

NUM_TOPICS = 4
NUM_WORDS = 10
NUM_DOCUMENTS = 10

DOCUMENT_NUM_WORDS = [8,4,1,30,10,12,15,62,25,22]

topic_dist = pymc.CompletedDirichlet('topic_dist',pymc.Dirichlet('pre_topic_dist', theta = [1.0] * NUM_TOPICS))
word_dist = [pymc.CompletedDirichlet('word_dist_%i' % i,pymc.Dirichlet('pre_word_dist_%i' % i, theta = [1.0] * NUM_WORDS))
             for i in range(NUM_DOCUMENTS)]

@pymc.stochastic(dtype=int)
def topic(value=[0], td=topic_dist):

    def logp(value,td):
        return pymc.multinomial_like(np.histogram(value, bins=range(NUM_TOPICS+1))[0],
                                     n=len(value), p=topic_dist)

    def random(td):
        pass

document = np.empty(NUM_DOCUMENTS, dtype=object)
for i in range(NUM_DOCUMENTS):
    document[i] = pymc.Multinomial('document_%i' % i, n=1,
                                   p=pymc.Index('word_dist_doc_%i' %i, word_dist,
                                                  0))
    


