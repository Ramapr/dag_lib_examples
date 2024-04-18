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
import numpy as np
import os
import logging



with DAG(dag_id=dag_id,
         default_args={'depends_on_past': True,
                       'wait_for_downstream': True,
                       'owner': owner 
                       },
         tags = tags,    
         schedule_interval=timedelta(hours=time_delta_h),
         start_date=datetime(year=2023, month=3, day=1, hour=6, minute=0),
         max_active_runs=1,
         catchup=True
         ) as dag:


    def _health_check(**kwargs):
        try:
            mdl_list = requests.post("http://localhost:8080/v2/repository/index", 
                                    json={}, timeout=5.).json()
            if check_model(mdl_list, 
                           model_name,
                           model_version):
                return "main"
                
            print(f"ERROR: model with name={model_name} and ver={model_version} not found")
            return None
        except Exception as e:
            print(e)
            return None

    def _main(**kwargs):
        """
        # 1. load data and check all 
        # 2. request to mlserver  
        # 3. get responce 
        # 4. process responce 
        # 5. push responce to xcomm
        """
        with open(param_file) as json_file:
            cfg = json.load(json_file)
        
        end = kwargs.get('data_interval_start')
        start = end - timedelta(hours=cfg["data"]["timerange_hours"])

        try:
            fact_data_raw = get_data(cfg["internal"]["data_api"],
                                     cfg["data"], 
                                     ','.join(cfg["data"]["params"]), 
                                     cfg["data"]["period"], 
                                     start,  
                                     end) 
        except Exceptions as e:
            print(e)
            return None 

        try:
            df = format_data2df(fact_data_raw)
        except Exceptions as e:
            print(e)
            return None

        reordered = df[cfg["data"]["params"]]
        if reordered.shape[1] != len(cfg["data"]["params"]): 
            print(f"ERROR:  {cfg['data']['params']} & {reordered.columns}")
            return None

        #################
        # PREPROCESSING #
        #################
        payld = preprocess(reordered.values, cfg)

        responce = inf_req(payld, 
                           mlserver_endpoint, 
                           model_name, 
                           model_version)

        if isinstance(responce, tuple):
            print(responce.json())
            return None 

        #######
        ## calc_features
        resp_data = responce.json()
        
        data = np.array(resp_data['outputs'][0]['data'])
        #data = data_out.reshape(payld.shape)
        if data.shape != payld.shape:
            print("ERROR in shapes:" + str(data.shape) + str(payld.shape)
            return None

        df_output = pd.DataFrame(data=(payld - data), 
                        index=reordered.index, 
                        columns=reordered.columns)

        #outs = {'df' : df_output}
        # convert to df
        if cfg['postprocess']["mean"]:
            df_output['mean'] = df_output.mean(axis=1)
            #outs['mean_e'] = mean_e
        if cfg['postprocess']["var"]:
            df_output['var'] = df_output.var(axis=1)
            # outs['var_e'] = var_e
      
        ##############
        if cfg['outputs']['type'] not in ['db', 'kafka']:
            print(outs)
            return None
        else:
            # push to xcomm 
            kwargs['ti'].xcom_push(key='res', 
                                   value=df_output.to_json())
            if cfg['output']['type'] == 'db':
                return "write2db"
            elif cfg['output']['type'] == 'kafka':
                return "write2kafka" 
            
    def _write2kafka(**kwargs):
        pass  

    def _write2db(**kwargs):
        conn = ClickHouseConnection.get_connection(db)
        ress = kwargs['ti'].xcom_pull(key='res')
        ress = json.loads(json.loads(ress))

        params = kwargs['ti'].xcom_pull(
            task_ids='prepare_data', key='params_order')

        time = ress['time']
        t2 = ress['result']['t2']
        mae = ress['result']['mae']
        errors = np.array(ress['errors'])
        dtime = pd.to_datetime(time, format="%Y-%m-%d %H:%M:%S.%f").tolist()

        sel = 'INSERT INTO %s (datetime, Val) VALUES'
        conn.execute(sel % 'mae', [{"datetime": v[0], "Val":v[1]} for v in zip(dtime, mae)])
        conn.execute(sel % 't2', [{"datetime": v[0], "Val":v[1]} for v in zip(dtime, t2)])

        for i, param in enumerate(params):
            table_name = add_prefix+param
            conn.execute(sel % table_name, [{"datetime": v[0], "Val":v[1]} for v in zip(dtime, errors[:,i])])
        # print("SUCESS")


    health_check = BranchPythonOperator(
        task_id='health_check',
        python_callable=_health_check,
        dag=dag
    )


    main = BranchPythonOperator(
        task_id='main',
        python_callable=_main,
        dag=dag
    )


    write2db = PythonOperator(
        task_id='write2db',
        python_callable=_write2db,
        dag=dag
    )

    # write2kafka = PythonOperator( 
    #     task_id='write2kafka', 
    #     python_callable=_write2kafka, 
    #     dag=dag
    # )

    health_check >> main >> write2db

