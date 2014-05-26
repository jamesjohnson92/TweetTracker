
drop table if exists tweettable;
drop table if exists twittergraph;
drop table if exists alphaphiin;
drop table if exists tweettopics;
drop table if exists topictweettable;

create external table tweettable
       (tweet_id bigint, creator bigint, tweeter bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:APPATH}/tweettable';

create external table topictweettable
       (tweet_id bigint, creator bigint, tweeter bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:APPATH}/topictweettable${hiveconf:TOPIC}';

create external table tweettopics
       (tweet bigint, topic int, stupid double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:TTPATH}/tweettopics';


insert into table topictweettable
select tweet_id, creator, tweeter
from tweettable join tweettopics
on tweettopics.tweet = tweettable.tweet_id
where topic = '${hiveconf:TOPIC}';

create external table twittergraph
       (friend bigint, follower bigint, edgeprob double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:APPATH}/twittergraph';

create external table alphaphiin
       (tweet_id bigint, tweeter bigint, retweeted bigint, seenprob double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:APPATH}/alphaphiin${hiveconf:TOPIC}';

insert into table alphaphiin
select topictweettable.tweet_id, 
       twittergraph.follower, 
       max(twittergraph.follower = topictweettable.tweeter),
       1 - exp(sum(log(1-twittergraph.edgeprob)))
from twittergraph join topictweettable
on twittergraph.friend = topictweettable.tweeter
where topictweettable.creator <> twittergraph.follower
group by topictweettable.tweet_id, topictweettable.creator, twittergraph.follower;


