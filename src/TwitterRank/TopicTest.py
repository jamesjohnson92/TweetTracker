import Topic
import pymc

# Simple generation of documents given their topics, the word distributions and the lengths
#
def simulate_word_counts(topics, distributions, nd):
    result = []
    for t in range(len(topics)):
        result.append(pymc.rmultinomial(n = nd[t], p = distributions[topics[t]]))
    return result
    
synthetic_word_count = simulate_word_counts(
    [0,0,1,1,2,2,3,3],
    [[0.15,0.15,0.15,0.15,0.15] + ([0.25/15] * 15),
     ([0.25/14] * 5) + [0.25,0.1,0.1,0.15,0.05,0.10] + ([0.25/14] * 9),
     ([0.25/15] * 10) + [0.55,0.05,0.05,0.05,0.05] + ([0.25/15] * 5),
     ([0.25/15] * 15) + [0.15,0.15,0.15,0.15,0.15]],
    [500,1000,500,1000,1050,3000,1000,3000]
)

M = Topic.topic_model(4,synthetic_word_count)
# this should be considered working well if M.topic.value has each adjacent pair the same value
