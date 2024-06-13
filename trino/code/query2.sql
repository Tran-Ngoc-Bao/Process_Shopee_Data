use iceberg.shopee;



create or replace table rate_11035478_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11035478'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11035478 and rating_count > 100
group by itemid, name;

-- create table rate_11035478_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11035478'
-- ) as
-- select *
-- from rate_11035478_today;

insert into rate_11035478_past
select * 
from rate_11035478_today;

create or replace table liked_11035478_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11035478'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11035478
group by itemid, name;

-- create table liked_11035478_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11035478'
-- ) as
-- select *
-- from liked_11035478_today;

insert into liked_11035478_past
select * 
from liked_11035478_today;



create or replace table rate_11035567_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11035567'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11035567 and rating_count > 100
group by itemid, name;

-- create table rate_11035567_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11035567'
-- ) as
-- select *
-- from rate_11035567_today;

insert into rate_11035567_past
select * 
from rate_11035567_today;

create or replace table liked_11035567_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11035567'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11035567
group by itemid, name;

-- create table liked_11035567_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11035567'
-- ) as
-- select *
-- from liked_11035567_today;

insert into liked_11035567_past
select * 
from liked_11035567_today;



create or replace table rate_11035639_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11035639'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11035639 and rating_count > 100
group by itemid, name;

-- create table rate_11035639_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11035639'
-- ) as
-- select *
-- from rate_11035639_today;

insert into rate_11035639_past
select * 
from rate_11035639_today;

create or replace table liked_11035639_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11035639'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11035639
group by itemid, name;

-- create table liked_11035639_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11035639'
-- ) as
-- select *
-- from liked_11035639_today;

insert into liked_11035639_past
select * 
from liked_11035639_today;



create or replace table rate_11035741_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11035741'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11035741 and rating_count > 100
group by itemid, name;

-- create table rate_11035741_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11035741'
-- ) as
-- select *
-- from rate_11035741_today;

insert into rate_11035741_past
select * 
from rate_11035741_today;

create or replace table liked_11035741_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11035741'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11035741
group by itemid, name;

-- create table liked_11035741_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11035741'
-- ) as
-- select *
-- from liked_11035741_today;

insert into liked_11035741_past
select * 
from liked_11035741_today;



create or replace table rate_11035761_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11035761'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11035761 and rating_count > 100
group by itemid, name;

-- create table rate_11035761_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11035761'
-- ) as
-- select *
-- from rate_11035761_today;

insert into rate_11035761_past
select * 
from rate_11035761_today;

create or replace table liked_11035761_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11035761'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11035761
group by itemid, name;

-- create table liked_11035761_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11035761'
-- ) as
-- select *
-- from liked_11035761_today;

insert into liked_11035761_past
select * 
from liked_11035761_today;



create or replace table rate_11035788_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11035788'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11035788 and rating_count > 100
group by itemid, name;

-- create table rate_11035788_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11035788'
-- ) as
-- select *
-- from rate_11035788_today;

insert into rate_11035788_past
select * 
from rate_11035788_today;

create or replace table liked_11035788_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11035788'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11035788
group by itemid, name;

-- create table liked_11035788_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11035788'
-- ) as
-- select *
-- from liked_11035788_today;

insert into liked_11035788_past
select * 
from liked_11035788_today;



create or replace table rate_11035801_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11035801'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11035801 and rating_count > 100
group by itemid, name;

-- create table rate_11035801_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11035801'
-- ) as
-- select *
-- from rate_11035801_today;

insert into rate_11035801_past
select * 
from rate_11035801_today;

create or replace table liked_11035801_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11035801'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11035801
group by itemid, name;

-- create table liked_11035801_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11035801'
-- ) as
-- select *
-- from liked_11035801_today;

insert into liked_11035801_past
select * 
from liked_11035801_today;



create or replace table rate_11035825_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11035825'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11035825 and rating_count > 100
group by itemid, name;

-- create table rate_11035825_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11035825'
-- ) as
-- select *
-- from rate_11035825_today;

insert into rate_11035825_past
select * 
from rate_11035825_today;

create or replace table liked_11035825_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11035825'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11035825
group by itemid, name;

-- create table liked_11035825_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11035825'
-- ) as
-- select *
-- from liked_11035825_today;

insert into liked_11035825_past
select * 
from liked_11035825_today;



create or replace table rate_11035853_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11035853'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11035853 and rating_count > 100
group by itemid, name;

-- create table rate_11035853_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11035853'
-- ) as
-- select *
-- from rate_11035853_today;

insert into rate_11035853_past
select * 
from rate_11035853_today;

create or replace table liked_11035853_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11035853'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11035853
group by itemid, name;

-- create table liked_11035853_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11035853'
-- ) as
-- select *
-- from liked_11035853_today;

insert into liked_11035853_past
select * 
from liked_11035853_today;