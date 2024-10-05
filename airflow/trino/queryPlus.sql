use iceberg.shopee;



create or replace table rate_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/rate'
) as
select itemid, name, grand_father_catid, father_catid,
sum(rating_star) / count(rating_star) as rating_star, sum(rating_count) / count(rating_count) as rating_count, sum(update_time) / count(update_time) as update_time,
ROW_NUMBER() OVER (PARTITION BY grand_father_catid ORDER BY rating_star DESC) as rank
from today
where rating_count > 100;

-- create table rate_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate'
-- ) as
-- select *
-- from rate_today;

insert into rate_past
select * 
from rate_today;



create or replace table liked_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/liked'
) as
select itemid, name, grand_father_catid, father_catid,
sum(liked_count) / count(liked_count) as liked_count, sum(update_time) / count(update_time) as update_time,
ROW_NUMBER() OVER (PARTITION BY grand_father_catid ORDER BY rating_star DESC) as rank
from today;

-- create table liked_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked'
-- ) as
-- select *
-- from liked_today;

insert into liked_past
select * 
from liked_today;



-- create table rate_X_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/rate_X'
-- ) as
-- select *
-- from rate_today
-- where grand_father_catid = X
-- order by rank;

insert into rate_X_past
select * 
from rate_X_today;



-- create table liked_X_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/liked_X'
-- ) as
-- select *
-- from liked_today
-- where grand_father_catid = X
-- order by rank;

insert into liked_X_past
select * 
from liked_X_today;