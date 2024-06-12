import json
import redis
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 5, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # "queue": "bash_queue",
    # "pool": "backfill",
    # "priority_weight": 10,
    # "end_date": datetime(2016, 1, 1),
}

dag = DAG("tutorial", default_args = default_args, schedule_interval = timedelta(30))

def create_tree_data(**kwargs):
    f = open("/usr/local/airflow/data/savedata.json", "r")
    t = json.load(f)["data"]["category_list"]
    f.close()
    tree = [[0 for i in range(150)] for j in range(50)]
    tree[49][0] = len(t)
    
    for i in range(len(t)):
        tree[i][0] = t[i]["catid"]
        tree[i][1] = t[i]["name"]
        tree[i][2] = t[i]["display_name"]
        tree[i][3] = len(t[i]["children"]) * 3 + 4

        for j in range(0, len(t[i]["children"]) * 3, 3):
            tree[i][j + 4] = t[i]["children"][int(j / 3)]["catid"]
            tree[i][j + 5] = t[i]["children"][int(j / 3)]["name"]
            tree[i][j + 6] = t[i]["children"][int(j / 3)]["display_name"]
    
    return tree

create_tree_task = PythonOperator(
    task_id = "create_tree_data",
    python_callable = create_tree_data,
    provide_context = True,
    dag = dag,
)

def format_json_schema(number_task, **kwargs):
    ti = kwargs['ti']
    tree = ti.xcom_pull(task_ids = 'create_tree_data')
    end = tree[49][0]
    lloop = int(end / 5)
    if (lloop * (number_task + 1) < end):
        end = lloop * (number_task + 1)
    output = []

    for i in range(lloop * number_task, end):
        parent_catid = tree[i][0]
        parent_name = tree[i][1]
        parent_display_name = tree[i][2]

        for j in range(4, tree[i][3], 3):
            directory_path = "/usr/local/airflow/data/" + str(parent_catid) + "/" + str(tree[i][j])
            files = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]

            for file_name in files:
                file_path = directory_path + "/" + file_name
                read_file = open(file_path, "r")
                content = json.load(read_file)
                llist = len(content["data"]["sections"][0]["index"])
                father_catid = tree[i][j]
                father_name = tree[i][j + 1]
                father_display_name = tree[i][j + 2]

                for k in range(llist):
                    result = {}
                    result["grand_father_catid"] = parent_catid
                    result["grand_father_name"] = parent_name
                    result["grand_father_display_name"] = parent_display_name
                    result["father_catid"] = father_catid
                    result["father_name"] = father_name
                    result["father_display_name"] = father_display_name
                    result["update_time"] = content["data"]["update_time"]
                    result["version"] = content["data"]["version"]
                    result["key"] = content["data"]["sections"][0]["index"][k]["key"]

                    detail = content["data"]["sections"][0]["data"]["item"][k]
                    result["itemid"] = detail["itemid"]
                    result["shopid"] = detail["shopid"]
                    result["name"] = detail["name"]
                    result["label_ids"] = detail["label_ids"]
                    result["image"] = detail["image"]
                    result["images"] = detail["images"]
                    result["currency"] = detail["currency"]
                    result["stock"] = detail["stock"]
                    result["status"] = detail["status"]
                    result["ctime"] = detail["ctime"]
                    result["sold"] = detail["sold"]
                    result["historical_sold"] = detail["historical_sold"]
                    result["liked"] = detail["liked"]
                    result["liked_count"] = detail["liked_count"]
                    result["view_count"] = detail["view_count"]
                    result["catid"] = detail["catid"]
                    result["brand"] = detail["brand"]
                    result["cmt_count"] = detail["cmt_count"]
                    result["flag"] = detail["flag"]
                    result["cb_option"] = detail["cb_option"]
                    result["item_status"] = detail["item_status"]
                    result["price"] = detail["price"]
                    result["price_min"] = detail["price_min"]
                    result["price_max"] = detail["price_max"]
                    result["price_min_before_discount"] = detail["price_min_before_discount"]
                    result["price_max_before_discount"] = detail["price_max_before_discount"]
                    result["hidden_price_display"] = detail["hidden_price_display"]
                    result["price_before_discount"] = detail["price_before_discount"]
                    result["has_lowest_price_guarantee"] = detail["has_lowest_price_guarantee"]
                    result["show_discount"] = detail["show_discount"]
                    result["raw_discount"] = detail["raw_discount"]
                    result["discount"] = detail["discount"]
                    result["is_category_failed"] = detail["is_category_failed"]
                    result["size_chart"] = detail["size_chart"]
                    result["rating_star"] = float(detail["item_rating"]["rating_star"])
                    result["rating_count"] = detail["item_rating"]["rating_count"][0]
                    result["rating_count_1"] = detail["item_rating"]["rating_count"][1]
                    result["rating_count_2"] = detail["item_rating"]["rating_count"][2]
                    result["rating_count_3"] = detail["item_rating"]["rating_count"][3]
                    result["rating_count_4"] = detail["item_rating"]["rating_count"][4]
                    result["rating_count_5"] = detail["item_rating"]["rating_count"][5]
                    result["rcount_with_image"] = detail["item_rating"]["rcount_with_image"]
                    result["rcount_with_context"] = detail["item_rating"]["rcount_with_context"]
                    result["item_type"] = detail["item_type"]
                    result["reference_item_id"] = detail["reference_item_id"]
                    result["transparent_background_image"] = detail["transparent_background_image"]
                    result["is_adult"] = detail["is_adult"]
                    result["badge_icon_type"] = detail["badge_icon_type"]
                    result["shopee_verified"] = detail["shopee_verified"]
                    result["is_official_shop"] = detail["is_official_shop"]
                    result["show_official_shop_label"] = detail["show_official_shop_label"]
                    result["show_shopee_verified_label"] = detail["show_shopee_verified_label"]
                    result["show_official_shop_label_in_title"] = detail["show_official_shop_label_in_title"]
                    result["is_cc_installment_payment_eligible"] = detail["is_cc_installment_payment_eligible"]
                    result["is_non_cc_installment_payment_eligible"] = detail["is_non_cc_installment_payment_eligible"]
                    result["show_free_shipping"] = detail["show_free_shipping"]
                    result["bundle_deal_id"] = detail["bundle_deal_id"]
                    result["can_use_bundle_deal"] = detail["can_use_bundle_deal"]
                    result["welcome_package_type"] = detail["welcome_package_type"]
                    result["welcome_package_info"] = detail["welcome_package_info"]
                    result["can_use_wholesale"] = detail["can_use_wholesale"]
                    result["is_preferred_plus_seller"] = detail["is_preferred_plus_seller"]
                    result["shop_location"] = detail["shop_location"]
                    result["has_model_with_available_shopee_stock"] = detail["has_model_with_available_shopee_stock"]
                    result["is_on_flash_sale"] = detail["is_on_flash_sale"]
                    result["shop_name"] = detail["shop_name"]
                    if detail["shop_rating"] is None:
                        result["shop_rating"] = 0.0
                    else:
                        result["shop_rating"] = float(detail["shop_rating"])
                    result["is_mart"] = detail["is_mart"]
                    result["is_service_by_shopee"] = detail["is_service_by_shopee"]
                    result["free_shipping_info"] = detail["free_shipping_info"]
                    result["global_sold_count"] = detail["global_sold_count"]
                    result["repurchase_rate"] = detail["repurchase_rate"]
                    result["best_selling_tag"] = detail["best_selling_tag"]
                    result["tp_label"] = detail["tp_label"]
                    result["nap_ui_type"] = detail["nap_ui_type"]
                    result["flash_sale_stock"] = detail["flash_sale_stock"]
                    result["info"] = detail["info"]
                    result["data_type"] = detail["data_type"]
                    result["count"] = detail["count"]
                    result["adsid"] = detail["adsid"]
                    result["campaignid"] = detail["campaignid"]
                    result["deduction_info"] = detail["deduction_info"]
                    result["video_display_control"] = detail["video_display_control"]
                    result["can_use_cod"] = detail["can_use_cod"]
                    result["pub_id"] = detail["pub_id"]
                    result["pub_context_id"] = detail["pub_context_id"]
                    result["friend_relationship_label"] = detail["friend_relationship_label"]
                    result["show_flash_sale_label"] = detail["show_flash_sale_label"]
                    result["search_id"] = detail["search_id"]
                    result["ext_info"] = detail["ext_info"]
                    result["need_kyc"] = detail["need_kyc"]
                    result["is_shopee_choice"] = detail["is_shopee_choice"]
                    result["display_ad_tag"] = detail["display_ad_tag"]
                    result["ads_type"] = detail["ads_type"]
                    result["traffic_source"] = detail["traffic_source"]
                    result["rank_highlight_tag"] = detail["rank_highlight_tag"]
                    result["item_card_display_sold_count"] = detail["item_card_display_sold_count"]["display_sold_count"]
                    result["spl_installment_discount"] = detail["spl_installment_discount"]

                    output.append(result)

                read_file.close()
    
    return output

format_schema_task1 = PythonOperator(
    task_id = "format_json_schema1",
    python_callable = format_json_schema,
    op_kwargs = {"number_task": 0},
    provide_context = True,
    dag = dag,
)

format_schema_task2 = PythonOperator(
    task_id = "format_json_schema2",
    python_callable = format_json_schema,
    op_kwargs = {"number_task": 1},
    provide_context = True,
    dag = dag,
)

format_schema_task3 = PythonOperator(
    task_id = "format_json_schema3",
    python_callable = format_json_schema,
    op_kwargs = {"number_task": 2},
    provide_context = True,
    dag = dag,
)

format_schema_task4 = PythonOperator(
    task_id = "format_json_schema4",
    python_callable = format_json_schema,
    op_kwargs = {"number_task": 3},
    provide_context = True,
    dag = dag,
)

format_schema_task5 = PythonOperator(
    task_id = "format_json_schema5",
    python_callable = format_json_schema,
    op_kwargs = {"number_task": 4},
    provide_context = True,
    dag = dag,
)
 
def store_data_in_redis(number_task, **kwargs):
    ti = kwargs['ti']
    output = ti.xcom_pull(task_ids = 'format_json_schema' + number_task)
    r = redis.Redis(host = "redis", port = 6379, db = 0)
    list_key = {}
    list_key["0"] = len(output) + 1

    for i in range(len(output)):
        r.set("today" + number_task + str(i), json.dumps(output[i]))
        list_key[str(i + 1)] = "today" + number_task + str(i)

    r.set("today_list_key" + number_task, json.dumps(list_key))

store_data_task1 = PythonOperator(
    task_id = "store_data_in_redis1",
    python_callable = store_data_in_redis,
    op_kwargs = {"number_task": "1"},
    provide_context = True,
    dag = dag,
)

store_data_task2 = PythonOperator(
    task_id = "store_data_in_redis2",
    python_callable = store_data_in_redis,
    op_kwargs = {"number_task": "2"},
    provide_context = True,
    dag = dag,
)

store_data_task3 = PythonOperator(
    task_id = "store_data_in_redis3",
    python_callable = store_data_in_redis,
    op_kwargs = {"number_task": "3"},
    provide_context = True,
    dag = dag,
)

store_data_task4 = PythonOperator(
    task_id = "store_data_in_redis4",
    python_callable = store_data_in_redis,
    op_kwargs = {"number_task": "4"},
    provide_context = True,
    dag = dag,
)

store_data_task5 = PythonOperator(
    task_id = "store_data_in_redis5",
    python_callable = store_data_in_redis,
    op_kwargs = {"number_task": "5"},
    provide_context = True,
    dag = dag,
)

create_tree_task >> [format_schema_task1, format_schema_task2, format_schema_task3, format_schema_task4, format_schema_task5]
format_schema_task1 >> store_data_task1
format_schema_task2 >> store_data_task2
format_schema_task3 >> store_data_task3
format_schema_task4 >> store_data_task4
format_schema_task5 >> store_data_task5