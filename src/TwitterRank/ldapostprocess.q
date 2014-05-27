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
       gam0 double, gam1 double, gam2 double, gam3 double, gam4 double, gam5 double, gam6 double, gam7 double, gam8 double, gam9 double, gam10 double, gam11 double, gam12 double, gam13 double, gam14 double, gam15 double, gam16 double, gam17 double, gam18 double, gam19 double, gam20 double, gam21 double, gam22 double, gam23 double, gam24 double, gam25 double, gam26 double, gam27 double, gam28 double, gam29 double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:TROPATH}/ldaout';

create external table pregraphout (
       follower_id bigint,
       follower_gam0 double, follower_gam1 double, follower_gam2 double, follower_gam3 double, follower_gam4 double, follower_gam5 double, follower_gam6 double, follower_gam7 double, follower_gam8 double, follower_gam9 double, follower_gam10 double, follower_gam11 double, follower_gam12 double, follower_gam13 double, follower_gam14 double, follower_gam15 double, follower_gam16 double, follower_gam17 double, follower_gam18 double, follower_gam19 double, follower_gam20 double, follower_gam21 double, follower_gam22 double, follower_gam23 double, follower_gam24 double, follower_gam25 double, follower_gam26 double, follower_gam27 double, follower_gam28 double, follower_gam29 double,
       friend_id bigint,
       tweet_count bigint,
       friend_gam0 double, friend_gam1 double, friend_gam2 double, friend_gam3 double, friend_gam4 double, friend_gam5 double, friend_gam6 double, friend_gam7 double, friend_gam8 double, friend_gam9 double, friend_gam10 double, friend_gam11 double, friend_gam12 double, friend_gam13 double, friend_gam14 double, friend_gam15 double, friend_gam16 double, friend_gam17 double, friend_gam18 double, friend_gam19 double, friend_gam20 double, friend_gam21 double, friend_gam22 double, friend_gam23 double, friend_gam24 double, friend_gam25 double, friend_gam26 double, friend_gam27 double, friend_gam28 double, friend_gam29 double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:TROPATH}/pregraph';

create external table gamma_sums_out (
       gam0 double, gam1 double, gam2 double, gam3 double, gam4 double, gam5 double, gam6 double, gam7 double, gam8 double, gam9 double, gam10 double, gam11 double, gam12 double, gam13 double, gam14 double, gam15 double, gam16 double, gam17 double, gam18 double, gam19 double, gam20 double, gam21 double, gam22 double, gam23 double, gam24 double, gam25 double, gam26 double, gam27 double, gam28 double, gam29 double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:TROPATH}/gammasums';

insert into table gamma_sums_out
select sum(gam0), sum(gam1), sum(gam2), sum(gam3), sum(gam4), sum(gam5), sum(gam6), sum(gam7), sum(gam8), sum(gam9), sum(gam10), sum(gam11), sum(gam12), sum(gam13), sum(gam14), sum(gam15), sum(gam16), sum(gam17), sum(gam18), sum(gam19), sum(gam20), sum(gam21), sum(gam22), sum(gam23), sum(gam24), sum(gam25), sum(gam26), sum(gam27), sum(gam28), sum(gam29) 
from twittergammas;

