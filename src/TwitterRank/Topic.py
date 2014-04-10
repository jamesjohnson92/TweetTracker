import pymc
import numpy as np
import random
from numpy.random import choice

# creates a model useable by pymc for topic selection
# word_counts is a list of word count histograms
#
def make_topic_model(num_topics, word_counts):
    DOCUMENT_WORD_COUNTS = word_counts
    NUM_TOPICS = num_topics
    NUM_DOCUMENTS = len(word_counts)
    NUM_WORDS = len(word_counts[0])

    # Distribution of topics
    #
    topic_dist = pymc.CompletedDirichlet('topic_dist',pymc.Dirichlet('pre_topic_dist', theta = [1.0] * NUM_TOPICS))

    # Array of word distributions per topic, word_dist[i][j] is the probabiliy of word j in topic i
    #
    word_dist = [pymc.CompletedDirichlet('word_dist_%i' % i,pymc.Dirichlet('pre_word_dist_%i' % i, theta = [1.0] * NUM_WORDS))
                 for i in range(NUM_TOPICS)]

    # Array of topic assignments
    #
    @pymc.stochastic(dtype=int)
    def topic(value=np.empty(NUM_DOCUMENTS), td=topic_dist):

        def logp(value,td):
            bins = range(NUM_TOPICS+1)
            bins[-1] = bins[-1] - 0.1
            return pymc.multinomial_like(np.histogram(value, bins=bins)[0],
                                             n=NUM_DOCUMENTS, p=td)

        def random(td):
            result = []
            elements = np.arange(len(td))
            for i in range(NUM_DOCUMENTS):
                result.append(choice(elements, p=td))
            return result

    # Observed word counts, which are modeled as multinomial with probabilities from word_dist[topic[i]].
    #
    document = np.empty(NUM_DOCUMENTS, dtype=object)
    for i in range(NUM_DOCUMENTS):
        document[i] = pymc.Multinomial('document_%i' % i, n=sum(DOCUMENT_WORD_COUNTS[i]),
                                       p=pymc.Index('word_dist_doc_%i' %i,
                                                    word_dist, topic[i]),
                                       observed = True,
                                       value = DOCUMENT_WORD_COUNTS[i])

    return locals()

# This is the function you call to fit the model
#
def topic_model(num_topics, word_counts, iter=10000, burn=1000, thin=10):
    M = pymc.MCMC(make_topic_model(num_topics, word_counts))
    M.sample(iter=iter, burn=burn, thin=thin)
    return M
