import sys
from math import log

tweets = {}
users = {}
labels = {}
maxvalues = [0.0 for i in range(22)]
f = open('hits', 'r')
for line in f :
	info = line.split()
	user = info[0]
	rank = info[1]
	users[user] = rank
	rank = float(rank)
	if rank > maxvalues[0] :
		maxvalues[0] = rank

f.close()


f2 = open('retweetrates', 'r')
for line in f2 :
	info = line.split()
	retweetrates = [float(t) for t in info[2:-1]]
	for i in range(10) :
		if retweetrates[i] > maxvalues[i+1] :
			maxvalues[i+1] = retweetrates[i]

f3 = open('favoriterates', 'r')
for line in f3 :
	info = line.split()
	favrates = [float(t) for t in info[1:]]
	for i in range(10) :
		if favrates[i] > maxvalues[11+i] :
			maxvalues[11+i] = favrates[i]

f2.seek(0)
f3.seek(0)
for line in f2 :
	info = line.split()
	twt = info[0]
	user = info[1]
	if user in users :
		rank = str(round(float(users[user])/maxvalues[0], 2))
	else :
		rank = "0.0"

	retweetrates = []
	for i in range(10) :
		rate = round(float(info[i+2])/maxvalues[i+1], 2)
		retweetrates.append(rate)

	lbl = "0.0"
	if int(info[-1]) > 500 :
		lbl = "1.0"

	tweets[twt] = lbl + ' ' + rank + ' ' + ' '.join(map(str, retweetrates))
f2.close()
users.clear()

for line in f3 :
	info = line.split()
	twt = info[0]
	favrates = []
	for i in range(10) :
		rate = round(float(info[1+i])/maxvalues[11+i], 2)
		favrates.append(rate)
	
	if twt in tweets :
		tweets[twt] = tweets[twt] + ' ' + ' '.join(map(str, favrates)) + 'G'
f3.close()

for t in tweets.keys():
	content = tweets[t]
	if content[-1] != 'G':
		del tweets[t]
	else:
		tweets[t] = content[:-1]

f2.close()
f3.close()

f4 = open('timefeatures', 'r')
for line in f4 :
	info = line.split()
	twt = info[0]
	hour = info[1]
	day = info[2]
	hourfeature = [0.0 for i in range(24)]
	dayfeature = [0.0 for i in range(7)]
	hourfeature[int(hour)] = 1.0
	dayfeature[int(day)] = 1.0
	if twt in tweets :
		tweets[twt] = tweets[twt] + ' ' + ' '.join(map(str, hourfeature)) + ' ' + ' '.join(map(str, dayfeature)) + 'G'

f4.close()

for t in tweets.keys():
	content = tweets[t]
	if content[-1] != 'G':
		del tweets[t]
	else:
		tweets[t] = content[:-1]


for twt in tweets :
	line = [float(l) for l in tweets[twt].split()]
	count = 0
	for num in line :
		if num < 0.01 :
			count = count + 1
	if count < 48 :
		print tweets[twt]
