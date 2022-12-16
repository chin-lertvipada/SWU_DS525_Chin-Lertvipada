import papermill as pm


pm.execute_notebook(
    'Code/datalake_etl_s3.ipynb',
    'Code/datalake_etl_s3_output.ipynb',
)