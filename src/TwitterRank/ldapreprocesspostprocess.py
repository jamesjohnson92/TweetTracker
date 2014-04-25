import os
import sys


num_gammas=int(sys.argv[1])

gammas_string = ', '.join(map(lambda i : "gam%d double" % i, xrange(num_gammas)))
follower_gammas_string = ', '.join(map(lambda i : "follower_gam%d double" % i, xrange(num_gammas)))
friend_gammas_string = ', '.join(map(lambda i : "friend_gam%d double" % i, xrange(num_gammas)))
follower_gammas_as_string = ', '.join(map(lambda i : "follower_gammas.gam%d as follower_gam%d" % (i,i), xrange(num_gammas)))
friend_gammas_as_string = ', '.join(map(lambda i : "friend_gammas.gam%d as friend_gam%d" % (i,i), xrange(num_gammas)))
gamma_sums_string = ', '.join(map(lambda i : "sum(gam%d)" % i, xrange(num_gammas)))


sed_clause = 's/__GAMMAS__/%s/g;s/__FOLLOWER_GAMMAS__/%s/g;s/__FRIEND_GAMMAS__/%s/g;s/__FOLLOWER_GAMMAS_AS__/%s/g;s/__FRIEND_GAMMAS_AS__/%s/g;s/__GAMMA_SUMS__/%s/g;' % (
    gammas_string, follower_gammas_string, friend_gammas_string, follower_gammas_as_string, friend_gammas_as_string, gamma_sums_string)

os.system('sed \'%s\' <ldapostprocess_stub.q >ldapostprocess.q' % sed_clause)
