drop table if exists twittergraph;
drop table if exists twittergammas;
drop table if exists pregraphout;
drop table if exists gamma_sums_out;

create external table twittergraph(
       follower_id bigint, 
       friend_id bigint,
       tweet_count bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:TROPATH}/followertable';

create external table twittergammas(
       twitter_id bigint, 
       gam0 double, gam1 double, gam2 double, gam3 double, gam4 double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:TROPATH}/ldaout';

create external table pregraphout (
       follower_id bigint,
       follower_gam0 double, follower_gam1 double, follower_gam2 double, follower_gam3 double, follower_gam4 double,
       friend_id bigint,
       tweet_count bigint,
       friend_gam0 double, friend_gam1 double, friend_gam2 double, friend_gam3 double, friend_gam4 double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:TROPATH}/pregraph';

create external table gamma_sums_out (
       gam0 double, gam1 double, gam2 double, gam3 double, gam4 double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:TROPATH}/gammasums';

insert into table gamma_sums_out
select sum(gam0), sum(gam1), sum(gam2), sum(gam3), sum(gam4)
from twittergammas;

insert into table pregraphout
select twittergraph.follower_id as follower_id, 
       follower_gammas.gam0 as follower_gam0, follower_gammas.gam1 as follower_gam1, follower_gammas.gam2 as follower_gam2, follower_gammas.gam3 as follower_gam3, follower_gammas.gam4 as follower_gam4,
       twittergraph.friend_id as friend_id, 
       twittergraph.tweet_count as tweet_count, 
       friend_gammas.gam0 as friend_gam0, friend_gammas.gam1 as friend_gam1, friend_gammas.gam2 as friend_gam2, friend_gammas.gam3 as friend_gam3, friend_gammas.gam4 as friend_gam4
from twittergammas friend_gammas 
     join twittergraph
     join twittergammas follower_gammas
on twittergraph.friend_id = friend_gammas.twitter_id
   and twittergraph.follower_id = follower_gammas.twitter_id;

