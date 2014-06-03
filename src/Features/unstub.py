
import os
import sys


num_gammas=int(sys.argv[1])

gammas_string = (', '.join(map(lambda i : "gam%d double" % i, xrange(num_gammas))))
sum_gammas = ', '.join(map(lambda i : "sum(gam%d)" % i, xrange(num_gammas)))

sed_clause = 's/__GAMMAS__/%s/g;s/__GAMMA_SUMS__/%s/g;' % (gammas_string, sum_gammas)

os.system('sed \'%s\' <collecttwitterranks_stub.q >collecttwitterranks.q' % sed_clause)
