drop table if exists wordcounts;
drop table if exists wordprobs;
drop table if exists logrelprobs;
drop table if exists loglikes;
drop table if exists priors;
drop table if exists maxlikes;
drop table if exists tweettopics;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set mapred.map.tasks = 20;
set hive.exec.parallel=true;

create external table wordcounts(
       id bigint, 
       word bigint, 
       freq bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:TTPATH}/wordcounts';


create external table wordprobs(
       word bigint,
       topic bigint,
       logrelprob double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:TTPATH}/wordprobs';

create external table priors(
       topic bigint,
       logprior double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:TTPATH}/priors';


create external table loglikes(
       id bigint,
       topic bigint,
       loglikes double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:TTPATH}/loglikes';


create external table logrelprobs(
       id bigint,
       topic bigint,
       logrelprob double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:TTPATH}/logrelprobs';

create external table tweettopics(
       id bigint,
       topic bigint,
       logrelprob double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:TTPATH}/tweettopics';

create external table maxlikes(
       id bigint,
       logrelprob double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '${hiveconf:TTPATH}/maxlikes';


insert into table loglikes	
select wordcounts.id as id, 
       wordprobs.topic as topic, 
    sum(wordprobs.logrelprob * wordcounts.freq) as logrelprob
from wordcounts join wordprobs join priors
on wordcounts.word = wordprobs.word
   and wordprobs.topic = priors.topic
group by wordcounts.id, wordprobs.topic;

insert into table logrelprobs
select ss.id, ss.topic, priors.logprior + ss.logrelprob
from priors join loglikes ss
on priors.topic = ss.topic;


insert into maxlikes
select id, max(logrelprob) as logrelprob 
from logrelprobs
group by id;

insert into table tweettopics
select lrp.id, lrp.topic, lrp.logrelprob
from logrelprobs lrp join maxlikes ss 
on lrp.id = ss.id and lrp.logrelprob = ss.logrelprob;
