import sys
import simplejson

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
			wordcts[word] = int(freq)
	print "Finishing wc..."
	return wordcts

def process_wp(wp_filename, ll_out="ll_file"):
	"""
	Process word probabilities into log likelihoods
	No more returning now, this shit's 600mb on 2 hours
	(probably won't fit in memory at weeks, in other words)
	"""
	print "Beginning on wp to ll..."
	with open(wordprob_filename, 'r') as wp_file:
		with open(ll_out, 'w') as ll_out_file:
			for line in wp_file: #note doesn't fit in memory
				word, topic, logrelprob = line.split()
				if word in wordcts:
					if topic in priors:
						#wordcounts id as id
						#wordprobs topic as topic
						#sum(wordprobs.logrelprob * worcounts.freq) as logrelprob
						#make ll_line
						print>>ll_out_file, ll_line
	print "Finishing wp to ll..."

def process_ll(ll_filename="ll_file", lrp_filename="lrp_file"):
	print "Beginning on  ll to lrp.."
	with open(loglikes, 'r') as ll_file:
		for line in ll_file:
			ll_id, topic, loglikes = line.split()
			#make logrelprobs
	print "Finishing on ll to lrp.."

def process_lrp(lrp_filename="lrp_file", ml_filename="ml_file"):
	print "Beginning on lrp to ml.."
	with open(logrelprobs, 'r') as lrp_file:
		for line in lrp_file:
			lrp_id, topic, logrelprob = line.split()
			#make maxlikes
	print "Finishing on lrp to ml.."

def process_tt(ml_filename="ml_file", tt_filename="tt_file"):
	print "Beginning on ml to tweet topics.."
	with open(maxlikes, 'r') as ml_file:
		for line in ml_file:
			ml_id, logrelprob = line.split()
			#make tweettopics
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
	process_wp(wp_filename)
	process_ll()
	process_lrp()
	process_tt()
	print "Done."

if __name__ == '__main__':
	assert sys.argv[1] #name of priors: fits in memory
	assert sys.argv[2] #name of wordcounts: fits in memory
	assert sys.argv[3] #name of wordprobs: fits in disk, maybe not memory
	main(sys.argv)
