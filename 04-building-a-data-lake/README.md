# Building a Data Warehouse

## Data model
![DataModel](Doc/04-datalake.png)
<br>

## Documentation
[Documentation](https://github.com/chin-lertvipada/swu-ds525/blob/5ed4d388bca49709e39da5a2a8a7864a63d9e166/04-building-a-data-lake/Doc/Lab4%20-%20Building%20datalake%20-%20Summary.pdf)
<br>
__________
<br>

## Project implementation instruction
<br>


### 1. change directory to project 04-building-a-data-lake:
```sh
$ cd 04-building-a-data-lake
```

### 2. prepare environment workspace by Docker:
```sh
$ docker-compose up
```

### 3. Open JupyterLab URL:
<br>

![JupyterLab](Doc/JupyterLab_URL.png)
<br>

### 4. Execute the Notebook 'etl_local_64199130039.ipynb' step by step: 
<br>

![Notebook](Doc/NoteBook.png)
<br>

### 5. Chcek the cleaned output data in folders, partition by 'date_oprt':
#### actors : [actors](https://github.com/chin-lertvipada/swu-ds525/tree/main/04-building-a-data-lake/actors)

#### repos : [repos](https://github.com/chin-lertvipada/swu-ds525/tree/main/04-building-a-data-lake/repos)

#### orgs : [orgs](https://github.com/chin-lertvipada/swu-ds525/tree/main/04-building-a-data-lake/orgs)

#### events : [events](https://github.com/chin-lertvipada/swu-ds525/tree/main/04-building-a-data-lake/events)
<br>


## Shutdown steps

### 11. Shutdown environment workspace:
```sh
$ docker-compose down
```
