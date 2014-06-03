
drop table if exists tweettable;
drop table if exists twitterranktable;
drop table if exists twitterrankfeature;


create external table tweettable
       (tweet_id bigint, creator bigint, tweeter bigint, followers bigint, dt bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:PATH}/tweettable';

create external table twitterranktable
       (user_id bigint, gam0 double, gam1 double, gam2 double, gam3 double, gam4 double, gam5 double, gam6 double, gam7 double, gam8 double, gam9 double, gam10 double, gam11 double, gam12 double, gam13 double, gam14 double, gam15 double, gam16 double, gam17 double, gam18 double, gam19 double, gam20 double, gam21 double, gam22 double, gam23 double, gam24 double, gam25 double, gam26 double, gam27 double, gam28 double, gam29 double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:PATH}/twitterranktable';

create external table twitterrankfeature
       (user_id bigint, gam0 double, gam1 double, gam2 double, gam3 double, gam4 double, gam5 double, gam6 double, gam7 double, gam8 double, gam9 double, gam10 double, gam11 double, gam12 double, gam13 double, gam14 double, gam15 double, gam16 double, gam17 double, gam18 double, gam19 double, gam20 double, gam21 double, gam22 double, gam23 double, gam24 double, gam25 double, gam26 double, gam27 double, gam28 double, gam29 double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:PATH}/twitterranktable${hiveconf:TIME}';

insert into table twitterrankfeature
select tweettable.tweet_id sum(gam0), sum(gam1), sum(gam2), sum(gam3), sum(gam4), sum(gam5), sum(gam6), sum(gam7), sum(gam8), sum(gam9), sum(gam10), sum(gam11), sum(gam12), sum(gam13), sum(gam14), sum(gam15), sum(gam16), sum(gam17), sum(gam18), sum(gam19), sum(gam20), sum(gam21), sum(gam22), sum(gam23), sum(gam24), sum(gam25), sum(gam26), sum(gam27), sum(gam28), sum(gam29)
from tweettable join twitterranktable
on tweettable.tweeter = twitterranktable.user_id
where tweettable.dt <= '${hiveconf:TIME}'
group by tweettable.tweet_id;
