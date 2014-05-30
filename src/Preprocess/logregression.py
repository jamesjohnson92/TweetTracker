import sys
from math import exp
import random

f = open('normalized', 'r')

NUM_FEATURES = 52
ETA = 0.0001

betas = [0.0 for i in range(NUM_FEATURES+1)]

testset = []
trainset = []
count = 0
for line in f :
	count = count + 1
	if count > 800 :
		testset.append(line)
	else :
		trainset.append(line)

f.close()
# "epochs" = number of passes over data during learning 
count = 0
for k in range(10000):
	gradient = [0.0 for i in range(NUM_FEATURES+1)]
 	for line in trainset:
 		x = [float(l) for l in line.split()]
 		z = betas[0]
 		for i in range(1, NUM_FEATURES+1):
 			z = z + betas[1]*x[i]
 		
 		#print y, z, exp(-z)
		gradient[0] = gradient[0] + (x[0] - (1.0/(1.0+exp(-z))))
 		for i in range(1, NUM_FEATURES+1):
 			gradient[i] = gradient[i] + x[i]*(x[0] - (1.0/(1.0+exp(-z))))
 		count = count + 1

 	for i in range(NUM_FEATURES+1):
 		betas[i] = betas[i] + ETA * gradient[i]

positives = 0
negatives = 0
correctPositives = 0
correctNegatives = 0

for line in testset:
	x = [float(l) for l in line.split()]
	prediction = betas[0]
	for i in range(1, NUM_FEATURES+1) :
		prediction = prediction + betas[i]*x[i]
	
	prediction = 1.0/(1.0 + exp(-prediction))
	
	if x[0] > 0.9 :
		positives = positives + 1
	else :
		negatives = negatives + 1

	if prediction > 0.5 and x[0] > 0.9 :
		correctPositives = correctPositives + 1
	elif prediction <= 0.5 and x[0] < 0.5 :
		correctNegatives = correctNegatives + 1
 
print 'precision', (correctPositives + correctNegatives)*100/len(testset)
print 'recall', correctPositives * 100/positives, positives