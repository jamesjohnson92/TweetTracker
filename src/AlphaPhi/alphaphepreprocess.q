
drop table if exists tweettable;
drop table if exists twittergraph;
drop table if exists alphaphiin;
drop table if exists tweettopics;
drop table if exists topictweettable;

create external table tweettable
       (tweet_id bigint, creator bigint, tweeter bigint, followers bigint, dt bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:PATH}/tweettable';

create external table topictweettable
       (tweet_id bigint, creator bigint, tweeter bigint, dt bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:PATH}/topictweettable${hiveconf:TOPIC}';

create external table tweettopics
       (tweet bigint, topic int, stupid double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:PATH}/tweettopics';


insert into table topictweettable
select tweet_id, creator, tweeter, dt
from tweettable join tweettopics
on tweettopics.tweet = tweettable.tweet_id
where topic = cast('${hiveconf:TOPIC}' as int);

create external table twittergraph
       (tweet_id bigint, friend bigint, follower bigint, followers bigint, dt bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:PATH}/tweettable';


-- create external table twittergraph
--        (friend bigint, follower bigint, edgeprob double)
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
-- STORED AS TEXTFILE
-- location '${hiveconf:PATH}/twittergraph';

create external table alphaphiin
       (tweet_id bigint, tweeter bigint, retweeted bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:PATH}/alphaphiin${hiveconf:TOPIC}';

-- insert into table alphaphiin
-- select topictweettable.tweet_id, 
--        twittergraph.follower, 
--        max(twittergraph.follower = topictweettable.tweeter)
-- from twittergraph join topictweettable
-- on twittergraph.friend = topictweettable.tweeter
-- where topictweettable.creator <> twittergraph.follower
-- group by topictweettable.tweet_id, topictweettable.creator, twittergraph.follower;


