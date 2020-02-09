import json

from catcher.utils import file_utils
from catcher.utils.logger import debug
from catcher_modules.utils import db_utils


def get_failed_task(dag_id, execution_time, conf, driver):
    engine = db_utils.get_engine(conf, driver)
    with engine.connect() as connection:
        result = connection.execute('''select * from task_instance ti
                                       where ti.dag_id = '{}' and ti.execution_date = '{}'
                                    '''.format(dag_id, execution_time))
        rows = [dict(r) for r in result.fetchall()]
        if rows:
            return list(filter(lambda x: x['state'] == 'failed', rows))[0]['task_id']
        else:
            return None


def get_execution_date_by_run_ud(run_id, conf, driver):
    engine = db_utils.get_engine(conf, driver)
    with engine.connect() as connection:
        result = connection.execute('''select execution_date from dag_run where run_id = '{}' '''.format(run_id))
        rows = [dict(r) for r in result.fetchall()]
        if not rows:
            raise Exception('Can\'t get execution_date by run_id {}'.format(run_id))
        return rows[0]['execution_date']


def get_xcom(task_id, execution_time, conf, driver):
    engine = db_utils.get_engine(conf, driver)
    with engine.connect() as connection:
        dag_id = _get_dag_id_by_task_id(task_id, connection)
        result = connection.execute('''select * from xcom 
                                       where execution_date = '{}' and task_id = '{}' and dag_id = '{}'
                                    '''.format(execution_time, task_id, dag_id))
        return [dict(r) for r in result.fetchall()][0]


def fill_connections(inventory, conf, driver):
    """
    Populate airflow connections based on catcher's inventory file
    :param inventory: path to inventory file
    :param conf: db configuration
    :param driver: dialect
    :return:
    """
    inv_dict = file_utils.read_source_file(inventory)
    engine = db_utils.get_engine(conf, driver)
    with engine.connect() as connection:
        for name, value in inv_dict.items():
            try:
                if _check_conn_id_exists(name, connection):
                    raise Exception('Already exists')
                if isinstance(value, str):  # conn_id: 'drivername://username:password@host:port/database'
                    connection.execute(_prepare_url(name, value))
                else:  # conn_id: extras object (aws)
                    connection.execute(_prepare_extras(name, value))
                debug('{} added'.format(name))
            except Exception as e:
                debug('Can\'t add {}:{} - {}'.format(name, value, e))


def _get_dag_id_by_task_id(task_id, connection):
    result = connection.execute('''
    select dag_id from task_instance ti where ti.task_id = '{}' order by execution_date desc limit 1
    '''.format(task_id))
    rows = [dict(r) for r in result.fetchall()]
    if not rows:
        raise Exception('Can\'t get dag_id by task_id {}'.format(task_id))
    return rows[0]['dag_id']


def _check_conn_id_exists(conn_id, connection) -> bool:
    result = connection.execute('select count(*) from connection where conn_id = \'{}\''.format(conn_id)).fetchall()
    return result != [(0,)]


def _prepare_url(name, value) -> str:
    from sqlalchemy.engine.url import make_url
    url = make_url(value)
    return '''insert into connection(conn_id,conn_type,host,schema,login,password,port)
                         values('{}','{}','{}','{}','{}','{}',{})'''.format(name, url.drivername.split('+')[0],
                                                                            url.host, 'default', url.username,
                                                                            url.password_original, url.port)


def _prepare_extras(name, value) -> str:
    if name.lower().startswith('s3') and 'key_id' in value and 'secret_key' in value:  # s3/minio
        # rename key_id & secret_key
        return '''insert into connection(conn_id,conn_type,extra)
                  values('{}','{}','{}')'''.format(name, 'aws',
                                                   json.dumps({'host': value.get('url'),
                                                               'aws_access_key_id': value.get('key_id'),
                                                               'aws_secret_access_key': value.get('secret_key'),
                                                               'region_name': value.get('region')}))
    else:  # some other extras
        return '''insert into connection(conn_id,conn_type,extra)
                    values('{}','{}','{}')'''.format(name, value.get('type'), json.dumps(value))
