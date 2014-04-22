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
       __GAMMAS__)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:TROPATH}/ldaout';

create external table pregraphout (
       follower_id bigint,
       __FOLLOWER_GAMMAS__,
       friend_id bigint,
       tweet_count bigint,
       __FRIEND_GAMMAS__)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:TROPATH}/pregraph';

create external table gamma_sums_out (
       __GAMMAS__)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:TROPATH}/gammasums';

insert into table gamma_sums_out
select __GAMMA_SUMS__ 
from twittergammas;

insert into table pregraphout
select twittergraph.follower_id as follower_id, 
       __FOLLOWER_GAMMAS_AS__,
       twittergraph.friend_id as friend_id, 
       twittergraph.tweet_count as tweet_count, 
       __FRIEND_GAMMAS_AS__
from twittergammas friend_gammas 
     join twittergraph
     join twittergammas follower_gammas
on twittergraph.friend_id = friend_gammas.twitter_id
   and twittergraph.follower_id = follower_gammas.twitter_id;

