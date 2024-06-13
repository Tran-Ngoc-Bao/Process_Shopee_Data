use iceberg.shopee;



create or replace table rate_11036478_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11036478'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036478 and rating_count > 100
group by itemid, name;

-- create table rate_11036478_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11036478'
-- ) as
-- select *
-- from rate_11036478_today;

insert into rate_11036478_past
select * 
from rate_11036478_today;

create or replace table liked_11036478_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11036478'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036478
group by itemid, name;

-- create table liked_11036478_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11036478'
-- ) as
-- select *
-- from liked_11036478_today;

insert into liked_11036478_past
select * 
from liked_11036478_today;



create or replace table rate_11036525_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11036525'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036525 and rating_count > 100
group by itemid, name;

-- create table rate_11036525_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11036525'
-- ) as
-- select *
-- from rate_11036525_today;

insert into rate_11036525_past
select * 
from rate_11036525_today;

create or replace table liked_11036525_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11036525'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036525
group by itemid, name;

-- create table liked_11036525_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11036525'
-- ) as
-- select *
-- from liked_11036525_today;

insert into liked_11036525_past
select * 
from liked_11036525_today;



create or replace table rate_11036624_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11036624'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036624 and rating_count > 100
group by itemid, name;

-- create table rate_11036624_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11036624'
-- ) as
-- select *
-- from rate_11036624_today;

insert into rate_11036624_past
select * 
from rate_11036624_today;

create or replace table liked_11036624_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11036624'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036624
group by itemid, name;

-- create table liked_11036624_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11036624'
-- ) as
-- select *
-- from liked_11036624_today;

insert into liked_11036624_past
select * 
from liked_11036624_today;



create or replace table rate_11036670_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11036670'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036670 and rating_count > 100
group by itemid, name;

-- create table rate_11036670_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11036670'
-- ) as
-- select *
-- from rate_11036670_today;

insert into rate_11036670_past
select * 
from rate_11036670_today;

create or replace table liked_11036670_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11036670'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036670
group by itemid, name;

-- create table liked_11036670_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11036670'
-- ) as
-- select *
-- from liked_11036670_today;

insert into liked_11036670_past
select * 
from liked_11036670_today;



create or replace table rate_11036793_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11036793'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036793 and rating_count > 100
group by itemid, name;

-- create table rate_11036793_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11036793'
-- ) as
-- select *
-- from rate_11036793_today;

insert into rate_11036793_past
select * 
from rate_11036793_today;

create or replace table liked_11036793_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11036793'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036793
group by itemid, name;

-- create table liked_11036793_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11036793'
-- ) as
-- select *
-- from liked_11036793_today;

insert into liked_11036793_past
select * 
from liked_11036793_today;



create or replace table rate_11036863_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11036863'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036863 and rating_count > 100
group by itemid, name;

-- create table rate_11036863_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11036863'
-- ) as
-- select *
-- from rate_11036863_today;

insert into rate_11036863_past
select * 
from rate_11036863_today;

create or replace table liked_11036863_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11036863'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036863
group by itemid, name;

-- create table liked_11036863_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11036863'
-- ) as
-- select *
-- from liked_11036863_today;

insert into liked_11036863_past
select * 
from liked_11036863_today;



create or replace table rate_11036932_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11036932'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036932 and rating_count > 100
group by itemid, name;

-- create table rate_11036932_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11036932'
-- ) as
-- select *
-- from rate_11036932_today;

insert into rate_11036932_past
select * 
from rate_11036932_today;

create or replace table liked_11036932_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11036932'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036932
group by itemid, name;

-- create table liked_11036932_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11036932'
-- ) as
-- select *
-- from liked_11036932_today;

insert into liked_11036932_past
select * 
from liked_11036932_today;



create or replace table rate_11036971_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11036971'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036971 and rating_count > 100
group by itemid, name;

-- create table rate_11036971_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11036971'
-- ) as
-- select *
-- from rate_11036971_today;

insert into rate_11036971_past
select * 
from rate_11036971_today;

create or replace table liked_11036971_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11036971'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11036971
group by itemid, name;

-- create table liked_11036971_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11036971'
-- ) as
-- select *
-- from liked_11036971_today;

insert into liked_11036971_past
select * 
from liked_11036971_today;



create or replace table rate_11116484_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate_11116484'
) as
select itemid, name, sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11116484 and rating_count > 100
group by itemid, name;

-- create table rate_11116484_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_11116484'
-- ) as
-- select *
-- from rate_11116484_today;

insert into rate_11116484_past
select * 
from rate_11116484_today;

create or replace table liked_11116484_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked_11116484'
) as
select itemid, name, sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time
from today
where grand_father_catid = 11116484
group by itemid, name;

-- create table liked_11116484_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_11116484'
-- ) as
-- select *
-- from liked_11116484_today;

insert into liked_11116484_past
select * 
from liked_11116484_today;


drop table today;