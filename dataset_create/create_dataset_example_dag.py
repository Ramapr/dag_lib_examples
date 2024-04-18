from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from clickhouse_driver import Client
from datetime import datetime, timedelta
import requests
import json
import pandas as pd
import os
import logging
import boto3


log = logging.getLogger(__name__)


# dags/temp
param_file = f"./dags/ae_dag_conf/test.json"
USERNAME=""
PASS = ""
BUCKET_NAME="source"
end_point="http://192.168."
df_name = "test_dset.parquet"
dag_id = "example-dset"


def make_df(req, names):
    tm = pd.to_datetime([int(t) * 1e6 for t in req['res']['time']])
    df = pd.DataFrame(data=[], index=tm, columns=names)
    for p ,v in zip(names, req['res']['data']) :
        df[p] = v['values']
    return df

def get_data(url: str,
             start_date: int,
             end_date: int,
             prms,
             num_points=10_000) -> dict :

    headers = {'content-type': 'application/json'}
    data = {"date_type": "opc-time", 
            "params": prms, 
            "step": num_points, 
            "start_date": start_date,
            "end_date": end_date,
            "filters": []
           }
    r = requests.post(url, data=json.dumps(data).replace('\\' , ''), headers=headers)
    return json.loads(r.text)


with DAG(dag_id=dag_id,
         default_args={'depends_on_past': False,
                       'wait_for_downstream': False,
                       },
         #tags = tags,
         #schedule="@weekly",
         schedule_interval=timedelta(hours=1),
         start_date=datetime(2023, 11, 16),
         max_active_runs=1,
         catchup=False
         ) as dag:

    def _update_dataset(**kwargs):
        print('start')
        with open(param_file) as json_file:
            cfg = json.load(json_file)

        print(cfg)

        # FIX HERE
        #end = kwargs.get('data_interval_start')
        #start = end - timedelta(hours=int(cfg["data"]["timerange_hours"]))
        
        start = '2023-11-20 12:24:06'
        end =   '2023-11-21 13:24:06'
        print(start, end)

        start_ms = (pd.to_datetime(start)  - pd.Timestamp("1970-01-01")) // pd.Timedelta('1ms')
        end_ms = (pd.to_datetime(end)  - pd.Timestamp("1970-01-01")) // pd.Timedelta('1ms')

        out = get_data(cfg["internal"]["data_api"],
                        start_ms,
                        end_ms,
                        cfg["data"]["params"])

        if not isinstance(out, dict):
            print("INFO: type not equal to dict")
            print(out)
            return None

        try:
            df = make_df(out, cfg["data"]["params"])
        except Exception as e:
            print('e2', e)
            return None

        print(df.shape)
        df.to_parquet(f"./dags/temp/{df_name}")

        try:
            # connect to minio
            s3 = boto3.resource('s3',
                 endpoint_url=end_point,
                 aws_access_key_id=USERNAME,
                 aws_secret_access_key=PASS,
                            )
            print(s3)
            #print(BUCKET_NAME)
            s6 = s3.Bucket(BUCKET_NAME).upload_file(f"./dags/temp/{df_name}",
                                         f"/{cfg['data']['station_name']}/{df_name}")

        except Exception as e:
            print(e)
            return None
        os.remove(f"./dags/temp/{df_name}")
        
        print('success ended job')
        return None



    upd_dset = PythonOperator(
        task_id='update_dataset',
        python_callable=_update_dataset,
        dag=dag
    )

    upd_dset
