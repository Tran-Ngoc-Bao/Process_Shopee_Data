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
#### 1. You should pull and build images in file docker-compose.yaml before
  
```sh
docker pull { ... }
```

#### 2. Move to clone project and Start system
  
```sh
docker compose up -d
```

#### 3. Build enviroment on airflow-webserve and airflow-scheduler

```sh
docker exec -u root -it [airflow-webserver/airflow-scheduler] bash 
source /opt/airflow/trino/build-env.sh
```

#### 4. After start system, all port website of containers in <a href="https://github.com/Tran-Ngoc-Bao/Process_Shopee_Data/blob/master/port.txt">here</a>

#### 5. Start DAG in Airflow cluster

#### 6. Build enviroment Superset
```sh
./superset/bootstrap-superset.sh
```
  
#### 7. Visualize data in Superset with SQLalchemy uri
```sh
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
