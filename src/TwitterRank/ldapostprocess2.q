
insert into table pregraphout
select twittergraph.follower_id as follower_id, 
       follower_gammas.gam0 as follower_gam0, follower_gammas.gam1 as follower_gam1, follower_gammas.gam2 as follower_gam2, follower_gammas.gam3 as follower_gam3, follower_gammas.gam4 as follower_gam4, follower_gammas.gam5 as follower_gam5, follower_gammas.gam6 as follower_gam6, follower_gammas.gam7 as follower_gam7, follower_gammas.gam8 as follower_gam8, follower_gammas.gam9 as follower_gam9, follower_gammas.gam10 as follower_gam10, follower_gammas.gam11 as follower_gam11, follower_gammas.gam12 as follower_gam12, follower_gammas.gam13 as follower_gam13, follower_gammas.gam14 as follower_gam14, follower_gammas.gam15 as follower_gam15, follower_gammas.gam16 as follower_gam16, follower_gammas.gam17 as follower_gam17, follower_gammas.gam18 as follower_gam18, follower_gammas.gam19 as follower_gam19, follower_gammas.gam20 as follower_gam20, follower_gammas.gam21 as follower_gam21, follower_gammas.gam22 as follower_gam22, follower_gammas.gam23 as follower_gam23, follower_gammas.gam24 as follower_gam24, follower_gammas.gam25 as follower_gam25, follower_gammas.gam26 as follower_gam26, follower_gammas.gam27 as follower_gam27, follower_gammas.gam28 as follower_gam28, follower_gammas.gam29 as follower_gam29,
       twittergraph.friend_id as friend_id, 
       twittergraph.tweet_count as tweet_count, 
       friend_gammas.gam0 as friend_gam0, friend_gammas.gam1 as friend_gam1, friend_gammas.gam2 as friend_gam2, friend_gammas.gam3 as friend_gam3, friend_gammas.gam4 as friend_gam4, friend_gammas.gam5 as friend_gam5, friend_gammas.gam6 as friend_gam6, friend_gammas.gam7 as friend_gam7, friend_gammas.gam8 as friend_gam8, friend_gammas.gam9 as friend_gam9, friend_gammas.gam10 as friend_gam10, friend_gammas.gam11 as friend_gam11, friend_gammas.gam12 as friend_gam12, friend_gammas.gam13 as friend_gam13, friend_gammas.gam14 as friend_gam14, friend_gammas.gam15 as friend_gam15, friend_gammas.gam16 as friend_gam16, friend_gammas.gam17 as friend_gam17, friend_gammas.gam18 as friend_gam18, friend_gammas.gam19 as friend_gam19, friend_gammas.gam20 as friend_gam20, friend_gammas.gam21 as friend_gam21, friend_gammas.gam22 as friend_gam22, friend_gammas.gam23 as friend_gam23, friend_gammas.gam24 as friend_gam24, friend_gammas.gam25 as friend_gam25, friend_gammas.gam26 as friend_gam26, friend_gammas.gam27 as friend_gam27, friend_gammas.gam28 as friend_gam28, friend_gammas.gam29 as friend_gam29
from twittergammas friend_gammas 
     join twittergraph
     join twittergammas follower_gammas
on twittergraph.friend_id = friend_gammas.twitter_id
   and twittergraph.follower_id = follower_gammas.twitter_id;

