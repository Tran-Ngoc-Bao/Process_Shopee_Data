CREATE SCHEMA iceberg.shopee;

CALL iceberg.system.register_table(schema_name => 'shopee', table_name => date_format(current_timestamp, 'day%Y%m%d'), table_location => 's3a://warehouse/shopee/today/');