import json

from catcher.utils import file_utils
from catcher.utils.logger import debug

from catcher_modules.utils import db_utils


def get_failed_task(dag_id, execution_time, conf, dialect):
    engine = db_utils.get_engine(conf, dialect)
    with engine.connect() as connection:
        result = connection.execute('''select * from task_instance ti
                                       where ti.dag_id = '{}' and ti.execution_date = '{}'
                                    '''.format(dag_id, execution_time))
        rows = [dict(r) for r in result.fetchall()]
        if rows:
            return list(filter(lambda x: x['state'] == 'failed', rows))[0]['task_id']
        else:
            return None


def get_execution_date_by_run_ud(run_id, conf, dialect):
    engine = db_utils.get_engine(conf, dialect)
    with engine.connect() as connection:
        result = connection.execute('''select execution_date from dag_run where run_id = '{}' '''.format(run_id))
        rows = [dict(r) for r in result.fetchall()]
        if not rows:
            raise Exception('Can\'t get execution_date by run_id {}'.format(run_id))
        return rows[0]['execution_date']


def get_xcom(task_id, execution_time, conf, dialect):
    engine = db_utils.get_engine(conf, dialect)
    with engine.connect() as connection:
        dag_id = _get_dag_id_by_task_id(task_id, connection)
        result = connection.execute('''select * from xcom 
                                       where execution_date = '{}' and task_id = '{}' and dag_id = '{}'
                                    '''.format(execution_time, task_id, dag_id))
        return [dict(r) for r in result.fetchall()][0]


def check_dag_exists(dag_id, conf, dialect) -> bool:
    """
    Check if dag exists in airflow
    """
    engine = db_utils.get_engine(conf, dialect)
    with engine.connect() as connection:
        result = connection.execute('''select * from dag 
                                           where dag_id = '{}'
                                        '''.format(dag_id))
        found = [dict(r) for r in result.fetchall()]
        return found != []


def unpause_dag(dag_id, conf, dialect) -> bool:
    """
    Unpause dag directly in the database. 
    Please use airflow_client.unpause_dag (if your version of airflow supports it instead.
    """
    engine = db_utils.get_engine(conf, dialect)
    with engine.connect() as connection:
        result = connection.execute('''update dag set is_paused=false 
                                       where dag_id = '{}'
                                            '''.format(dag_id))
        return result.rowcount == 1


def check_import_errors(dag_id, conf, dialect):
    """
    Check if there was import error for the dag
    """
    engine = db_utils.get_engine(conf, dialect)
    with engine.connect() as connection:
        result = connection.execute('''select * from import_error 
                                               where filename like '%%{}%%'
                                            '''.format(dag_id))
        return [dict(r)['stacktrace'] for r in result.fetchall()]


def fill_connections(inventory, conf, dialect, fernet_key):
    """
    Populate airflow connections based on catcher's inventory file
    :param inventory: path to inventory file
    :param conf: db configuration
    :param dialect: dialect
    :param fernet_key: is used in passwords encryption
    :return:
    """
    inv_dict = file_utils.read_source_file(inventory)
    engine = db_utils.get_engine(conf, dialect)
    with engine.connect() as connection:
        for name, value in inv_dict.items():
            try:
                if _check_conn_id_exists(name, connection):
                    raise Exception('Already exists')
                if isinstance(value, str):  # conn_id: 'drivername://username:password@host:port/database'
                    connection.execute(_prepare_url(name, value, fernet_key))
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


def _prepare_url(name, value, fernet_key) -> str:
    from sqlalchemy.engine.url import make_url
    from cryptography.fernet import Fernet
    f = Fernet(fernet_key.encode())
    url = make_url(value)
    if url.password_original is not None:
        password = f.encrypt(url.password_original.encode()).decode()
    else:
        password = None
    return '''insert into connection(conn_id,conn_type,host,schema,login,password,port,is_encrypted)
                         values('{}','{}','{}','{}','{}','{}',{},true)'''.format(name,
                                                                                 url.drivername.split('+')[0],
                                                                                 url.host,
                                                                                 url.database or None,
                                                                                 url.username,
                                                                                 password,
                                                                                 url.port)


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
