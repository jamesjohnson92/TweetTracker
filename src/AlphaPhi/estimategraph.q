
drop table if exists tweettable;
drop table if exists nrt;
drop table if exists rrt;
drop table if exists twitterpregraph;

create external table tweettable
       (tweet_id bigint, creator bigint, tweeter bigint, followers bigint, timestamp bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:APPATH}/tweettable';

create external table nrt
       (creator bigint, tweeter bigint, followers bigint, ruu bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:APPATH}/nrt';

create external table rrt
       (uid bigint, ru bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:APPATH}/rrt';

create external table twitterpregraph
       (creator bigint, tweeter bigint, followers bigint, ruu bigint, ru bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:APPATH}/twitterpregraph';

insert into table nrt 
select creator, tweeter, max(followers), count(tweet_id)
from tweettable
where creator <> tweeter
group by creator, tweeter;

insert into table rrt 
select creator, sum(ruu)
from nrt
group by creator;

insert into table twitterpregraph
select nrt.creator, nrt.tweeter, nrt.followers, nrt.ruu, rrt.ru
from nrt join rrt
on nrt.creator = rrt.uid;

