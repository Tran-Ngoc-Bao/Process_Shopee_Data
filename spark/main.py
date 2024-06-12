from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
import redis
import json
from datetime import date

if __name__ == "__main__":
  sc = SparkContext("spark://spark-iceberg:7077", "Process Shopee Data")
  spark = SparkSession(sc)
	
  my_schema = StructType([ 
    StructField("grand_father_catid", LongType(), True), 
    StructField("grand_father_name", StringType(), True),
    StructField("grand_father_display_name", StringType(), True), 
    StructField("father_catid", LongType(), True), 
    StructField("father_name", StringType(), True), 
    StructField("father_display_name", StringType(), True), 
    StructField("update_time", LongType(), True), 
    StructField("version", StringType(), True), 
    StructField("key", StringType(), True), 

    StructField("itemid", LongType(), True),
    StructField("shopid", LongType(), True),
    StructField("name", StringType(), True),
    StructField("label_ids", ArrayType(LongType()), True),
    StructField("image", StringType(), True),
    StructField("images", ArrayType(StringType()), True),
    StructField("currency", StringType(), True),
    StructField("stock", LongType(), True),
    StructField("status", LongType(), True),
    StructField("ctime", LongType(), True),
    StructField("sold", LongType(), True),
    StructField("historical_sold", LongType(), True),
    StructField("liked", BooleanType(), True),
    StructField("liked_count", LongType(), True),
    StructField("view_count", LongType(), True),
    StructField("catid", LongType(), True),
    StructField("brand", StringType(), True),
    StructField("cmt_count", LongType(), True),
    StructField("flag", LongType(), True),
    StructField("cb_option", LongType(), True),
    StructField("item_status", StringType(), True),
    StructField("price", LongType(), True),
    StructField("price_min", LongType(), True),
    StructField("price_max", LongType(), True),
    StructField("price_min_before_discount", LongType(), True),
    StructField("price_max_before_discount", LongType(), True),
    StructField("hidden_price_display", LongType(), True),
    StructField("price_before_discount", LongType(), True),
    StructField("has_lowest_price_guarantee", BooleanType(), True),
    StructField("show_discount", LongType(), True),
    StructField("raw_discount", LongType(), True),
    StructField("discount", StringType(), True),
    StructField("is_category_failed", StringType(), True),
    StructField("size_chart", StringType(), True),
    StructField("rating_star", FloatType(), True),
    StructField("rating_count", LongType(), True),
    StructField("rating_count_1", LongType(), True),
    StructField("rating_count_2", LongType(), True),
    StructField("rating_count_3", LongType(), True),
    StructField("rating_count_4", LongType(), True),
    StructField("rating_count_5", LongType(), True),
    StructField("rcount_with_image", LongType(), True),
    StructField("rcount_with_context", LongType(), True),
    StructField("item_type", LongType(), True),
    StructField("reference_item_id", StringType(), True),
    StructField("transparent_background_image", StringType(), True),
    StructField("is_adult", BooleanType(), True),
    StructField("badge_icon_type", LongType(), True),
    StructField("shopee_verified", BooleanType(), True),
    StructField("is_official_shop", BooleanType(), True),
    StructField("show_official_shop_label", BooleanType(), True),
    StructField("show_shopee_verified_label", BooleanType(), True),
    StructField("show_official_shop_label_in_title", BooleanType(), True),
    StructField("is_cc_installment_payment_eligible", BooleanType(), True),
    StructField("is_non_cc_installment_payment_eligible", BooleanType(), True),
    StructField("show_free_shipping", BooleanType(), True),
    StructField("bundle_deal_id", LongType(), True),
    StructField("can_use_bundle_deal", BooleanType(), True),
    StructField("welcome_package_type", LongType(), True),
    StructField("welcome_package_info", StringType(), True),
    StructField("can_use_wholesale", BooleanType(), True),
    StructField("is_preferred_plus_seller", BooleanType(), True),
    StructField("shop_location", StringType(), True),
    StructField("has_model_with_available_shopee_stock", BooleanType(), True),
    StructField("is_on_flash_sale", BooleanType(), True),
    StructField("shop_name", StringType(), True),
    StructField("shop_rating", FloatType(), True),
    StructField("is_mart", BooleanType(), True),
    StructField("is_service_by_shopee", BooleanType(), True),
    StructField("free_shipping_info", StringType(), True),
    StructField("global_sold_count", LongType(), True),
    StructField("repurchase_rate", LongType(), True),
    StructField("best_selling_tag", LongType(), True),
    StructField("tp_label", StringType(), True),
    StructField("nap_ui_type", LongType(), True),
    StructField("flash_sale_stock", LongType(), True),
    StructField("info", StringType(), True),
    StructField("data_type", StringType(), True),
    StructField("count", LongType(), True),
    StructField("adsid", LongType(), True),
    StructField("campaignid", LongType(), True),
    StructField("deduction_info", StringType(), True),
    StructField("video_display_control", LongType(), True),
    StructField("can_use_cod", StringType(), True),
    StructField("pub_id", StringType(), True),
    StructField("pub_context_id", StringType(), True),
    StructField("friend_relationship_label", StringType(), True),
    StructField("show_flash_sale_label", BooleanType(), True),
    StructField("search_id", StringType(), True),
    StructField("ext_info", StringType(), True),
    StructField("need_kyc", BooleanType(), True),
    StructField("is_shopee_choice", BooleanType(), True),
    StructField("display_ad_tag", LongType(), True),
    StructField("ads_type", LongType(), True),
    StructField("traffic_source", LongType(), True),
    StructField("rank_highlight_tag", LongType(), True),
    StructField("item_card_display_sold_count", LongType(), True),
    StructField("spl_installment_discount", StringType(), True),
  ])
  
  today = date.today()
  str_today = ""
  if (today.day < 10):
    str_today = "0" + str(today.day)
    if (today.month < 10):
      str_today += "0" + str(today.month)
    else:
      str_today += str(today.month)
    str_today += str(today.year)
  else:
    str_today = str(today.day)
    if (today.month < 10):
      str_today += "0" + str(today.month)
    else:
      str_today += str(today.month)
    str_today += str(today.year)
    
  r = redis.Redis(host = "redis", port = 6379, db = 0)
  json_data = []
  list_key = []

  for i in range(1, 6):
    key = "today_list_key" + str(i)
    list_key_tmp = json.loads(r.get(key).decode("utf-8"))
    
    for j in range(1, list_key_tmp["0"]):
      list_key.append(list_key_tmp[str(j)])

  for i in range(0, len(list_key)):
    json_data.append(json.loads(r.get(list_key[i]).decode("utf-8")))

  df = spark.createDataFrame(data = json_data, schema = my_schema)

  df.writeTo("shopee.today").tableProperty("location", "s3a://warehouse/shopee/today/").createOrReplace()
  df.writeTo("shopee.day" + str_today).tableProperty("location", "s3a://warehouse/shopee/day" + str_today).createOrReplace()

  for i in range(1, 6):
    key = "today_list_key" + str(i)
    new_key = key.replace("today", str_today)
    r.rename(key, new_key)

  for key in list_key:
    new_key = key.replace("today", str_today)
    r.rename(key, new_key)