from mrjob.job import MRJob
import json as simplejson

class MRFollowers(MRJob):

	def mapper_followers(self, _, line):
		tweet = simplejson.loads(line)
		retweetCount = 0
		language = 'es'
		if 'retweetCount' in tweet :
			retweetCount = tweet['retweetCount']
		if 'twitter_lang' in tweet :
			language = tweet['twitter_lang']

		if language == 'en' and retweetCount > 0:
			body = tweet['object']['body']
			tweeter = tweet['object']['actor']['id'][len('id:twitter.com:') :]
			user = tweet['actor']['id'][len('id:twitter.com:') :]
			yield user, tweeter

	
	def reducer_followers(self, user, follows):
		for u2 in follows:
			yield user, u2

	def steps(self):
		return [
			self.mr(mapper=self.mapper_followers,
				reducer=self.reducer_followers)
	    ]

if __name__ == '__main__':
    MRFollowers.run()