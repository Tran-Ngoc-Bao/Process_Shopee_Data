CREATE SCHEMA IF NOT EXISTS iceberg.shopee;

CALL iceberg.system.register_table (
    schema_name => 'shopee',
    table_name => 'today',
    table_location => 's3a://warehouse/shopee/today/'
    );

select itemid, name, sum(show_discount) / count(show_discount) as show_discount, sum(price_min / 10000) / count(price_min) as price_min, from_unixtime(update_time) as update_time 
from today 
where update_time between 1717804800 and 1717822800 
group by itemid, name, update_time
order by show_discount desc 
limit 100;

select itemid, name, sum(show_discount) / count(show_discount) as show_discount, sum(price_min / 10000) / count(price_min) as price_min, from_unixtime(update_time) as update_time 
from today 
where update_time between 1717912800 and 1717930800 
group by itemid, name, update_time
order by show_discount desc 
limit 100;

select itemid, name, sum(show_discount) / count(show_discount) as show_discount, sum(price_min / 10000) / count(price_min) as price_min, from_unixtime(update_time) as update_time 
from today 
where update_time between 1718017200 and 1718038800 
group by itemid, name, update_time
order by show_discount desc 
limit 100;

select itemid, name, sum(cmt_count) / count(cmt_count) as cmt_count 
from today 
group by itemid, name 
order by cmt_count desc 
limit 100;

select itemid, name, sum(sold) / count(sold) as sold, sum(historical_sold) / count(historical_sold) as historical_sold 
from today 
group by itemid, name 
order by sold desc 
limit 100;

CALL iceberg.system.register_table (
    schema_name => 'shopee',
    table_name => date_format(current_timestamp, 'day%Y%m%d'),
    table_location => 's3a://warehouse/shopee/today/'
    );

DROP TABLE iceberg.shopee.today;