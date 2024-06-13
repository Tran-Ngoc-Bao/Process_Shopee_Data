use iceberg.shopee;



create or replace table rate_11035898_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11035898'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11035898 and rating_count > 100
group by itemid, name;

-- create table rate_11035898_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11035898'
-- ) as
-- select *
-- from rate_11035898_today;

insert into rate_11035898_past
select * 
from rate_11035898_today;

create or replace table liked_11035898_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11035898'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11035898
group by itemid, name;

-- create table liked_11035898_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11035898'
-- ) as
-- select *
-- from liked_11035898_today;

insert into liked_11035898_past
select * 
from liked_11035898_today;



create or replace table rate_11035954_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11035954'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11035954 and rating_count > 100
group by itemid, name;

-- create table rate_11035954_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11035954'
-- ) as
-- select *
-- from rate_11035954_today;

insert into rate_11035954_past
select * 
from rate_11035954_today;

create or replace table liked_11035954_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11035954'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11035954
group by itemid, name;

-- create table liked_11035954_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11035954'
-- ) as
-- select *
-- from liked_11035954_today;

insert into liked_11035954_past
select * 
from liked_11035954_today;



create or replace table rate_11036030_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11036030'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036030 and rating_count > 100
group by itemid, name;

-- create table rate_11036030_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11036030'
-- ) as
-- select *
-- from rate_11036030_today;

insert into rate_11036030_past
select * 
from rate_11036030_today;

create or replace table liked_11036030_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11036030'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036030
group by itemid, name;

-- create table liked_11036030_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11036030'
-- ) as
-- select *
-- from liked_11036030_today;

insert into liked_11036030_past
select * 
from liked_11036030_today;



create or replace table rate_11036101_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11036101'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036101 and rating_count > 100
group by itemid, name;

-- create table rate_11036101_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11036101'
-- ) as
-- select *
-- from rate_11036101_today;

insert into rate_11036101_past
select * 
from rate_11036101_today;

create or replace table liked_11036101_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11036101'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036101
group by itemid, name;

-- create table liked_11036101_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11036101'
-- ) as
-- select *
-- from liked_11036101_today;

insert into liked_11036101_past
select * 
from liked_11036101_today;



create or replace table rate_11036132_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11036132'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036132 and rating_count > 100
group by itemid, name;

-- create table rate_11036132_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11036132'
-- ) as
-- select *
-- from rate_11036132_today;

insert into rate_11036132_past
select * 
from rate_11036132_today;

create or replace table liked_11036132_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11036132'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036132
group by itemid, name;

-- create table liked_11036132_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11036132'
-- ) as
-- select *
-- from liked_11036132_today;

insert into liked_11036132_past
select * 
from liked_11036132_today;



create or replace table rate_11036194_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11036194'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036194 and rating_count > 100
group by itemid, name;

-- create table rate_11036194_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11036194'
-- ) as
-- select *
-- from rate_11036194_today;

insert into rate_11036194_past
select * 
from rate_11036194_today;

create or replace table liked_11036194_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11036194'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036194
group by itemid, name;

-- create table liked_11036194_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11036194'
-- ) as
-- select *
-- from liked_11036194_today;

insert into liked_11036194_past
select * 
from liked_11036194_today;



create or replace table rate_11036279_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11036279'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036279 and rating_count > 100
group by itemid, name;

-- create table rate_11036279_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11036279'
-- ) as
-- select *
-- from rate_11036279_today;

insert into rate_11036279_past
select * 
from rate_11036279_today;

create or replace table liked_11036279_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11036279'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036279
group by itemid, name;

-- create table liked_11036279_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11036279'
-- ) as
-- select *
-- from liked_11036279_today;

insert into liked_11036279_past
select * 
from liked_11036279_today;



create or replace table rate_11036345_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11036345'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036345 and rating_count > 100
group by itemid, name;

-- create table rate_11036345_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11036345'
-- ) as
-- select *
-- from rate_11036345_today;

insert into rate_11036345_past
select * 
from rate_11036345_today;

create or replace table liked_11036345_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11036345'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036345
group by itemid, name;

-- create table liked_11036345_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11036345'
-- ) as
-- select *
-- from liked_11036345_today;

insert into liked_11036345_past
select * 
from liked_11036345_today;



create or replace table rate_11036382_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11036382'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036382 and rating_count > 100
group by itemid, name;

-- create table rate_11036382_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11036382'
-- ) as
-- select *
-- from rate_11036382_today;

insert into rate_11036382_past
select * 
from rate_11036382_today;

create or replace table liked_11036382_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11036382'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036382
group by itemid, name;

-- create table liked_11036382_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11036382'
-- ) as
-- select *
-- from liked_11036382_today;

insert into liked_11036382_past
select * 
from liked_11036382_today;