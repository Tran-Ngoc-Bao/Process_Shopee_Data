# Mini project - Data Engineering - Viettel Digital Talent 2024

## Project introduction
<ul>
  <li>Name of project: Analyze e-commerce information on Shopee</li>
  <li>Project objective:
    <ul>
      <li>Top well-rated products by item</li>
      <li>Top liked products by item</li>
      <li>Top products on strong sale</li>
      <li>Top products with the most comments</li>
      <li>Top best selling products</li>
    </ul>
  </li>
</ul>

## System overview
### Data ingestion
<img src="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/illustration/diagram/ThuThapDuLieu.png">
  
### Data storage
<img src="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/illustration/diagram/LuuTruDuLieu.png">
  
### Process data
<img src="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/illustration/diagram/XuLyDuLieu.png">
  
### Visualize data
<img src="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/illustration/diagram/TrucQuanHoaDuLieu.png">


## Deploy system
<ul>
  <li>You should download images before</li>
  
```sh
docker pull { ... }
```

  <li>Move to clone project and Start system</li>
  
```sh
docker compose up -d
```

  <li>After start system, all port website of containers in <a href="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/port.txt">here</a></li>
  <li>Start DAG in Airflow cluster</li>
  <img style="width:75%" src="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/illustration/system/airflow.png">
  <li>Move to spark-iceberg container</li>

```sh
docker exec -it spark-iceberg bash
```

  <li>Run code pyspark in spark-iceberg</li>

```sh
bin/spark-submit /home/iceberg/code/main.py
```

  <img style="width:75%" src="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/illustration/system/spark.png">
  <li>Move to trino container</li>

```sh
docker exec -it trino bash
```

  <li>Run code sql in trino</li>

```sh
trino -f { /etc/trino/code/query1.sql, /etc/trino/code/query2.sql, /etc/trino/code/query3.sql, /etc/trino/code/query4.sql }
```

  <img style="width:75%" src="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/illustration/system/trino.png">

  <li>Visualize data in Superset website on local</li>
</ul>

## Demo
### Store data in Redis
<img style="width:75%" src="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/illustration/system/redis.png">
  
### Store data in MinIO
<img style="width:75%" src="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/illustration/system/minio.png">
  
### Data warehouse on PostgreSQL & Hive
<img style="width:75%" src="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/illustration/system/hive_postgresql.png">

### Top well-rated products by item
<img src="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/illustration/output/top_rate_Balo_%26_Tui_Vi_Nam.jpg">

### Top liked products by item
<img src="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/illustration/output/top_like_Thoi_Trang_Nam.jpg">
  
### Top products on strong sale
<div>
  <img style="width:33%" src="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/illustration/output/sale_7h_12h_8_6.jpg">
  <img style="width:33%" src="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/illustration/output/sale_13h_18h_9_6.jpg">
  <img style="width:33%" src="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/illustration/output/sale_18h_24h_10_6.jpg">
</div>

### Top products with the most comments
<img src="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/illustration/output/top_comment.jpg">

### Top best selling products
<img src="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/illustration/output/top_sold.jpg">

## Report
<ul>
  <li><a href="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/report/report.pdf">Report</a></li>
  <li><a href="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/report/slide.pptx">Slide</a></li>
</ul>
