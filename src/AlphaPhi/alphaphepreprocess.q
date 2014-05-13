
drop table if exists tweettable;
drop table if exists twittergraph;
drop table if exists alphaphiin;


create external table tweettable
       (tweet_id bigint, creator bigint, tweeter bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:APPATH}/tweettable';


create external table twittergraph
       (friend bigint, follower bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:APPATH}/twittergraph';


create external table alphaphiin
       (tweet_id bigint, tweeter bigint, retweeted bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:APPATH}/alphaphiin';


insert into twittergraph
select distinct creator, tweeter
from tweettable

insert into alphaphiin
select tweet_id, follower, max(follower = tweeter)
from twittergraph join tweettable
on friend = creator or friend = tweeter
group by tweet_id, tweeter;

