
drop table if exists tweettable;
drop table if exists twitterranktable;
drop table if exists twitterrankfeature;


create external table tweettable
       (tweet_id bigint, creator bigint, tweeter bigint, followers bigint, dt bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:PATH}/tweettable';

create external table twitterranktable
       (user_id bigint, __GAMMAS__)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:PATH}/twitterranktable';

create external table twitterrankfeature
       (user_id bigint, __GAMMAS__)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:PATH}/twitterranktable${hiveconf:TIME}';

insert into table twitterrankfeature
select tweettable.tweet_id __GAMMA_SUMS__
from tweettable join twitterranktable
on tweettable.tweeter = twitterranktable.user_id
where tweettable.dt <= '${hiveconf:TIME}'
group by tweettable.tweet_id;
