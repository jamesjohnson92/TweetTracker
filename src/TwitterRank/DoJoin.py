import sys, os
import simplejson
from collections import defaultdict

#some of this shit follows from everything else

def read_followertable(filename):
	followertable = {}
	return followertable

def read_ldaout(folder_name):
	ldaout = {}
	##read all the members of the folder
	return ldaout

def read_pregraph(filename):
	pregraph = {}
	return pregraph

def do_join(followertable, ldaout, pregraph):
	write something to disk

def main(args):
	"""
	Main method
	Rolling like it's 2006
	"""
	followertable = read_followertable(args[1])
	ldaout = read_ldaout(args[2])
	pregraph = read_pregraph(args[3])
	do_join(followertable, ldaout, pregraph)

if __name__ == "__main__":
	assert something
	"""
	We are joining:
	twittergraph
	twittergammas
	follower_gammas
	friend_gammas
	"""
	main(sys.argv)
