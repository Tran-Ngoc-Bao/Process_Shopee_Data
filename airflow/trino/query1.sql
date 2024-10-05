create schema if not exists iceberg.shopee;

use iceberg.shopee;



call iceberg.system.register_table (
    schema_name => 'shopee',
    table_name => 'today',
    table_location => 's3a://warehouse/shopee/today/'
    );

-- create table past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/past'
-- ) as
-- select *
-- from today;

insert into past
select *
from today;



create or replace table flash_sale_7h_12h_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/flash_sale_7h_12h/'
) as
select itemid, name, sum(show_discount) / count(show_discount) as show_discount, sum(price_max / 100000) / count(price_max) as price, from_unixtime(sum(update_time) / count(update_time)) as update_time 
from today 
where update_time between 1717804800 and 1717822800 
group by itemid, name;

-- create table flash_sale_7h_12h_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/flash_sale_7h_12h/'
-- ) as
-- select *
-- from flash_sale_7h_12h_today;

insert into flash_sale_7h_12h_past
select *
from flash_sale_7h_12h_today;



create or replace table flash_sale_13h_18h_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/flash_sale_13h_18h/'
) as
select itemid, name, sum(show_discount) / count(show_discount) as show_discount, sum(price_max / 100000) / count(price_max) as price, from_unixtime(sum(update_time) / count(update_time)) as update_time 
from today 
where update_time between 1717912800 and 1717930800 
group by itemid, name;

-- create table flash_sale_13h_18h_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/flash_sale_13h_18h/'
-- ) as
-- select *
-- from flash_sale_13h_18h_today;

insert into flash_sale_13h_18h_past
select *
from flash_sale_13h_18h_today;



create or replace table flash_sale_18h_24h_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/flash_sale_18h_24h/'
) as
select itemid, name, sum(show_discount) / count(show_discount) as show_discount, sum(price_max / 100000) / count(price_max) as price, from_unixtime(sum(update_time) / count(update_time)) as update_time 
from today 
where update_time between 1718017200 and 1718038800 
group by itemid, name;

-- create table flash_sale_18h_24h_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/flash_sale_18h_24h/'
-- ) as
-- select *
-- from flash_sale_18h_24h_today;

insert into flash_sale_18h_24h_past
select *
from flash_sale_18h_24h_today;



create or replace table most_comment_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/most_comment/'
) as
select itemid, name, sum(cmt_count) / count(cmt_count) as cmt_count, sum(update_time) / count(update_time) as update_time
from today 
group by itemid, name;

-- create table most_comment_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/most_comment/'
-- ) as
-- select *
-- from most_comment_today;

insert into most_comment_past
select *
from most_comment_today;



create or replace table best_seller_today
with (
    format = 'parquet',
    location = 's3a://warehouse/result_shopee/today/best_seller/'
) as
select itemid, name, sum(sold) / count(sold) as sold, sum(historical_sold) / count(historical_sold) as historical_sold, sum(update_time) / count(update_time) as update_time
from today 
group by itemid, name;

-- create table best_seller_past
-- with (
--     format = 'parquet',
--     location = 's3a://warehouse/result_shopee/past/best_seller/'
-- ) as
-- select *
-- from best_seller_today;

insert into best_seller_past
select *
from best_seller_today;