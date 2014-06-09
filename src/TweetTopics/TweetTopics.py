import sys
import simplejson
from collections import defaultdict


def read_priors(priors_filename):
	priors = {}
	print "Beginning on priors..."
	with open(priors_filename, 'r') as priors_file:
		for line in priors_file:
			topic, logprior = line.split()
			priors[topic] = float(logprior)
	print "Finished priors."
	return priors

def read_wc(wc_filename):
	wordcts = {}
	print "Beginning on wc..."
	with open(wc_filename, 'r') as wc_file:
		for line in wc_file:
			wc_id, word, freq = line.split()
			wordcts[word] = (wc_id, int(freq))
	print "Finishing wc..."
	return wordcts

def process_wp(wp_filename, wordcts, priors, ll_filename="ll_file"):
	"""
	Process word probabilities into log likelihoods
	No more returning now, this shit's 600mb on 2 hours
	(probably won't fit in memory at weeks, in other words)

	print>> is pretty rarely used, so it's got to be said
	It's an alright method of piping text directly to a file
	"""
	print "Beginning on wp to ll..."
	logrelprobs = defaultdict(float)
	###do a progress bar just for this one, I think
	progress = 0
	with open(wp_filename, 'r') as wp_file:
		for line in wp_file: #note doesn't fit in memory
			#however, Python is smart in this specific instance
			#and uses iterators like a good boy
			word, topic, logrelprob = line.split()
			progress += 1
			if progress % 1000000 == 0:
				print "We have looked at this many wp's: ", progress
			if word in wordcts:
				if topic in priors:
					logrelprobs[(wordcts[word][0], topic)] += float(logrelprob) * wordcts[word][1]
	with open(ll_filename, 'w') as ll_file:
		for wordtup, val in logrelprobs.iteritems():
			ll_line = " ".join((wordtup[0], wordtup[1], str(val)))
			print>>ll_file, ll_line
	print "Finishing wp to ll..."

##I suspect all the rest will fit in memory

def process_ll(priors, ll_filename="ll_file", lrp_filename="lrp_file"):
	print "Beginning on  ll to lrp.."
	with open(ll_filename, 'r') as ll_file:
		with open(lrp_filename, 'w') as lrp_file:
			for line in ll_file:
				ll_id, topic, logprior = line.split()
				if topic in priors:
					logrelprior = float(logprior) + priors[topic]
					print>>lrp_file, " ".join((ll_id, topic, str(logrelprior)))
	print "Finishing on ll to lrp.."

def process_lrp(lrp_filename="lrp_file", ml_filename="ml_file"):
	print "Beginning on lrp to ml.."
	max_lrps = defaultdict(lambda: float("-inf"))
	max_topics = defaultdict(lambda: int("0"))
	with open(lrp_filename, 'r') as lrp_file:
		for line in lrp_file:
			lrp_id, topic, logrelprob = line.split()
			if max_lrps[lrp_id] < float(logrelprob):
				max_lrps[lrp_id] = logrelprob #note stays a string
				max_topics[lrp_id] = topic
	with open(ml_filename, 'w') as ml_file:
		for ml_id, logrelprob in max_lrps.iteritems():
			print>>ml_file, " ".join((ml_id, max_topics[ml_id], logrelprob))
	print "Finishing on lrp to ml.."

def process_tt(ml_filename="ml_file", lrp_filename="lrp_file", tt_filename="tt_file"):
	print "Beginning on ml to tweet topics.."
	maxlrps = set()
	with open(ml_filename, 'r') as ml_file:
		for line in ml_file:
			ml_id, logrelprob = line.split()
			maxlrps.add(ml_id)
	with open(lrp_filename, 'r') as lrp_file:
		with open(tt_filename, 'w') as tt_file:
			for line in lrp_file:
				lrp_id, topic, logrelprob = line.split()
				if lrp_id in maxlrps:
					print>>tt_file, " ".join((lrp_id, topic, logrelprob))

	print "Finishing on ml to tweet topics.."

def main(args):
	"""
	Main method
	Rolling like it's 1999
	"""
	priors_filename = args[1]
	wc_filename = args[2]
	wp_filename = args[3]
	priors = read_priors(priors_filename)
	print "Here's some priors: ", priors
	wordcts = read_wc(wc_filename)
	print "Here's 10 wordcounts out of many: ", wordcts.items()[:10]
	process_wp(wp_filename, wordcts, priors)
	process_ll(priors)
	process_lrp()
	process_tt()
	print "Done."

if __name__ == '__main__':
	assert sys.argv[1] #name of priors: fits in memory
	assert sys.argv[2] #name of wordcounts: fits in memory
	assert sys.argv[3] #name of wordprobs: fits in disk, maybe not memory
	main(sys.argv)
