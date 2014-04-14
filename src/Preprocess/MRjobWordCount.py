from mrjob.job import MRJob
import json as simplejson
import re

class MRWordFrequencyCount(MRJob):

	def mapper_get_words(self, _, line):
		tweet = simplejson.loads(line)
		if 'twitter_lang' in tweet : 
			language = tweet['twitter_lang']
			if language == 'en' :
				words = [re.sub(r'\W+', '', w.lower()) for w in tweet['body'].split()]
				if 'actor' in tweet :
					user = tweet['actor']['id'][len('id:twitter.com:') :]
					for w in words :
						if w != '':
							yield user, w
							return
							
		yield '0', 'UNK'

	def reducer_separate_words(self, user, words):
		counts = {}
		for w in words :			
			if w in counts :
				counts[w] = counts[w] + 1
			else :
				counts[w] = 1
		
		for w, c in counts.items() :
			yield user, (w, c)
	
	def reducer_count_words(self, user, word_count_pairs):
		for w, c in word_count_pairs:
			yield user, (w, c)

	def steps(self):
		return [
			self.mr(mapper=self.mapper_get_words,
					reducer=self.reducer_separate_words),
			self.mr(reducer=self.reducer_count_words)
        ]

if __name__ == '__main__':
    MRWordFrequencyCount.run()