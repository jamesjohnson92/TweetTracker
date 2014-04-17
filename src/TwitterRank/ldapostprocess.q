create database db if not exists;
use db;

create table twittergraph(
       follower_id  int not null, 
       friend_id int not null)
location ${hdfpath}/followertable;

create table tweetcounts(
       twitter_id int not null, 
       num_tweets int not null, 
       primary key(twitter_id))
location ${hdfspath}/tweetcounts;

create table twittergammas(
       twitter_id int not null, 
       gamma varchar(255) not null, 
       primary key (twitter_id))
location ${hdfspath}/ldaout/gamma-30
stored as SequenceFile;

select twittergraph.follower_id as follower_id, twittergraph.friend_id as friend_id, 
       twittercounts.num_tweets as num_tweets, 
       follower_gammas.gamma as follower_gamma, friend_gammas.gamma as friend_gamma
from twittergraph 
     join twittercounts 
     join twittergammas as friend_gammas 
     join twittergammas as follower_gammas
on friend_id = twittercounts.twitter_id
   and friend_id = friend_gammas.twitter_id
   and follower_id = follower_gammas.twitter_id;

