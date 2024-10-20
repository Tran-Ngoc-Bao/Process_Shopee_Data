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

## Data flow
  <img src="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/illustration/DataFlow.png">

## Deploy system
#### 1. Should pull and build images before
```sh
docker pull postgres tabulario/spark-iceberg tabulario/iceberg-rest minio/minio minio/mc trinodb/trino:457 starburstdata/hive:3.1.2-e.18
```
```sh
docker build ./airflow -t airflow
```
```sh
docker build ./superset -t superset
```

#### 2. Start system
```sh
docker compose up -d
```

#### 3. Install java on airflow-webserver similar with airflow-scheduler
```sh
docker exec -u root -it airflow-webserver bash
```
```sh
apt update && apt install default-jdk
```

#### 4. Start DAG on Airflow cluster

#### 5. Build enviroment Superset
```sh
./superset/bootstrap-superset.sh
```
  
#### 6. Visualize data in Superset with SQLalchemy uri
```
trino://hive@trino:8080/iceberg
```

## Output
### Top well-rated products by item
<img style="width:70%" src="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/illustration/output/top_rate_Balo_%26_Tui_Vi_Nam.jpg">

### Top liked products by item
<img style="width:70%" src="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/illustration/output/top_like_Thoi_Trang_Nam.jpg">

### Top products with the most comments
<img style="width:70%" src="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/illustration/output/top_comment.jpg">

## Report
<ul>
  <li><a href="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/report/report.pdf">Report</a></li>
  <li><a href="https://github.com/Tran-Ngoc-Bao/ProcessShopeeData/blob/master/report/slide.pptx">Slide</a></li>
</ul>
