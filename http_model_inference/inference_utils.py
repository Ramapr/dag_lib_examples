

class ClickHouseConnection:
    connection = None
    def get_connection(db_name, connection_name='clickhouse_conn'):
        if ClickHouseConnection.connection:
            return connection
        db_props = BaseHook.get_connection(connection_name)
        ClickHouseConnection.connection = Client(
            host=db_props.host, database=db_name)
        return ClickHouseConnection.connection


def check_model(mdllist, name, ver):
    total = [m for m in mdllist if m['name'] == name]
    if len(total):
        total = [m for m in total if m['version'] == ver] 
        if len(total) == 1:
            if total[0]['state'] == 'READY':
                return True 
    return False


def format_data2df(data: dict) -> pd.DataFrame:
    """ 
    convert data 
    """
    df_mod = pd.DataFrame.from_dict(data=data)
    df_mod['time'] = pd.to_datetime(df_mod['time'] * 1e6)
    #model1 = df_mod
    df_mod.set_index('time', inplace=True)
    df_mod.drop(columns=['station'], inplace=True)
    return df_mod


def model_request(payload: np.ndarray, 
                  mlserv: str, 
                  mdl_name: str , 
                  ver: str ):    
    """
    mlserv = 'http://localhost:8080'
    mdl_name = 'onx'
    ver = '0.0.1'
    """
    inference_request = {
        "inputs": [{
              "name": "predict",
              "shape": payload.shape,
              "datatype": "FP32",
              "data": payload.tolist()}
               ]
    }
    endpoint = f"{mlserv}/v2/models/{mdl_name}/versions/{ver}/infer"
    try:
        response = requests.post(endpoint,
                                 json=inference_request, 
                                 timeout=25.)
        if response.status_code == 200:
            return response #.content
        else: 
            return False, responce
    except Exception as e: # timeout exception 
        return False, e


