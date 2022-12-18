# Capstone Project - Chin Lertvipada 64199130039

## Data model (Datalake)
![DataModelDL](document/chin-capstone-datalake.png)
<br>

## Data model (Datawarehouse)
![DataModelDWH](document/chin-capstone-dwh.png)
<br>
__________
<br>

## Project Information Documentation
[Information Documentation](https://github.com/chin-lertvipada/swu-ds525/blob/b19f682d79c748e7f674b3a47c82b4dc80f9f9c5/chin-capstone/document/Capstone%20-%20Summary.pdf)
<br>
__________
<br>

## Project Overview Presentation
[Presentation Documentation](https://github.com/chin-lertvipada/swu-ds525/blob/4e44777ae66f79edc1d15d498d4558e11dc0f619/chin-capstone/document/Capstone%20-%20Presentation.pdf)

[![Presentation VDO](document/YoutubeCover.png)](https://youtu.be/7uj3f0Q5t0I)
<br>
__________
<br>

## Project implementation instruction
<br>

### 1. Change directory to project **"chin-capstone"**:
```sh
$ cd chin-capstone
```
<br>

### 2. Prepare Cloud access (AWS):
- Retrieve credential thru AWS terminal
```sh
$ cat ~/.aws/credentials
```
![awsAccessKey](document/aws_access_key.png)

- Copy 3 following values to update the source codes<br>

*Values to copy:*
> - aws_access_key_id
> - aws_secret_access_key
> - aws_session_token

*Source code to update (1):*
> - /code/etl_datalake_s3.ipynb
![awsAccessKeyDL](document/aws_access_key_datalake.png)

*Source code to update (2):*
> - /dags/etl_dwh_airflow.py
![awsAccessKeyDWH](document/aws_access_key_dwh.png)

<br>

### 3. Prepare Datalake storage (AWS S3):
- Create below S3 bucket with *"All public access"*
> - **jaochin-dataset-fifa**

![s3Bucket](document/s3_bucket.png)

- Create below repositories in the bucket
> - **landing** (store raw data)<br>
> - **cleaned** (store datalake or cleaned data)

![s3Folder](document/s3_folder.png)

- Place the raw data in **"landing/"**

![rawData](document/rawData.png)

<br>

### 4. Prepare Datawarehouse storage (AWS RedShift):
- Create Redshift cluster with following information

![redshift1](document/redshift1.png)
![redshift2](document/redshift2.png)
![redshift3](document/redshift3.png)

- Copy "**Endpoint**" and Cluster information to update Redshift credential

![redshiftEndpoint](document/redshiftEndpoint.png)


- Update the source code with Redshift credential
> - **dags/etl_dwh_airflow.py**
![redshiftCredential](document/redshiftCredential.png)

<br>

### 5. Create virtual environment named **"ENV"** (only 1st time):
```sh
$ python -m venv ENV
```
<br>

### 6. Activate the visual environment:
```sh
$ source ENV/bin/activate
```
<br>

### 7. Install needful libraries from configuration file (only 1st time):
```sh
$ pip install -r prerequisite/requirements.txt
```
<br>

### 8. Prepare environment workspace thru Docker:
- If Linux system, run 2 following commands (for Airflow usage)

```sh
mkdir -p ./dags ./logs ./plugins
```
```sh
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

- After that, run below command to start Docker

```sh
docker-compose up
```
<br>

### 9. Execute the **"Datalake"** process thru Web service:
- Access PySpark Notebook UI by port 8888 (localhost:8888)

![pyspark](document/pyspark.png)

- Run PySpark Notebook **"/code/etl_datalake_s3.ipynb"**

![runSpark](document/runSpark.png)

- The cleaned data will be stored in S3 for each entity
> - jaochin-dataset-fifa/cleaned/clubs/<br>
> - jaochin-dataset-fifa/cleaned/leagues/<br>
> - jaochin-dataset-fifa/cleaned/nationalities/<br>
> - jaochin-dataset-fifa/cleaned/players/<br>
> - jaochin-dataset-fifa/cleaned/positions/<br>

![cleanedData](document/cleanedData.png)


- Each entity is partitioned by **"date_oprt"** (execution date)

![cleanedDataPart](document/cleanedDataPart.png)
<br><br>

### 10. Execute the **"Datawarehouse"** process thru Airflow:
- Access Airflow UI by port 8080 (localhost:8080) with below credential
> - Username: "airflow"<br>
> - Password: "airflow"<br>

- The Datawarehouse script will be run follow the schedule configuration
> - Schedule: "Monthly" (1st of each month)<br>
> - Start date: "1st December 2022"

![airflowDWH](document/airflowDWH.png)

- The Datawarehouse data will be loaded into Redshift (check by Query editor)
```sh
select * from player_value_wage;
```
![redshiftOutput1](document/redshiftOutput1.png)
![redshiftOutput2](document/redshiftOutput2.png)

<br>

### 11. Dashboard creation thru Tableau:
- Connect Tableau Desktop to Redshift by following information

![redshiftCredential](document/redshiftCredential.png)
![tbConnect](document/tbConnect.png)

- Load the data from Redshift to Tableau

![tbLoadData](document/tbLoadData.png)

- Create Dashboard to visualize the insight!

https://public.tableau.com/app/profile/chin.lertvipada/viz/Capstone_csv/FootballMarketValue
![dashboard](document/dashboard.png)
<br>
__________
<br>

## Shutdown steps
##### 1. Stop services by shutdown Docker:
```sh
$ docker-compose down
```

##### 2. Deactivate the virtual environment:
```sh
$ deactivate
```