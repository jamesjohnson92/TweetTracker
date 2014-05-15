
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
       (friend bigint, follower bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:APPATH}/twittergraph${hiveconf:TOPIC}';

insert into table twittergraph
select distinct creator, tweeter
from tweettable;


create external table alphaphiin
       (tweet_id bigint, tweeter bigint, retweeted bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:APPATH}/alphaphiin${hiveconf:TOPIC}';

insert into table alphaphiin
select tweettable.tweet_id, twittergraph.follower, max(twittergraph.follower = tweettable.tweeter)
from twittergraph join tweettable
on twittergraph.friend = tweettable.tweeter
where tweettable.creator <> twittergraph.follower
group by tweettable.tweet_id, tweettable.creator, twittergraph.follower;


