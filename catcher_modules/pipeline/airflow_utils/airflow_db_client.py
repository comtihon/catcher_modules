import json
from typing import Tuple

from catcher.utils import file_utils
from catcher.utils.logger import debug, warning
from sqlalchemy.exc import ArgumentError

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


def get_dag_run_by_run_ud(run_id, conf, dialect):
    engine = db_utils.get_engine(conf, dialect)
    with engine.connect() as connection:
        result = connection.execute("""
        SELECT dag_id, execution_date, state, run_id, external_trigger, conf, end_date, start_date
        FROM dag_run 
        WHERE run_id = '{}'
        """.format(run_id))
        rows = [dict(r) for r in result.fetchall()]

        if not rows:
            raise Exception('Can\'t get dag_run info by run_id {}'.format(run_id))

        return rows[0]


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
                if isinstance(value, dict) and 'type' in value:
                    if _check_conn_id_exists(name, connection):
                        debug('{} already exists'.format(name))
                        continue
                    query, params = _prepare_connection(value, fernet_key)
                    from sqlalchemy.sql import text
                    connection.execute(text(query), name=name, **params)
                    debug('{} added'.format(name))
                else:
                    debug('{} ignored. No type specified'.format(name))
            except Exception as e:
                warning('Can\'t add {}:{} - {}'.format(name, value, e))


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


def _prepare_connection(value, fernet_key) -> Tuple[str, dict]:
    conn_type = value['type']
    from cryptography.fernet import Fernet
    f = Fernet(fernet_key.encode())
    db_config = value
    if 'url' in value and conn_type not in ['http', 'https', 'ftp', 'sftp']:  # database url based configuration
        db_config = {**db_config, **_string_url_to_object(conn_type, value['url'], f)}
    else:  # object based configuration (without url) or http/ftp
        if 'password' in db_config:
            db_config['password'] = f.encrypt(db_config['password'].encode()).decode()
        if 'url' in value and conn_type in ['http', 'https', 'ftp', 'sftp']:  # http/ftp based connection
            db_config['host'] = db_config['url']
    _unify_config(conn_type, db_config)
    if 'extra' in db_config and db_config['extra'] is not None:  # encode extra if exists
        db_config['extra'] = f.encrypt(db_config['extra'].encode()).decode()
    return '''insert into 
              connection(conn_id,conn_type,host,schema,login,password,port,extra,is_encrypted,is_extra_encrypted)
              values(:name,:type,:host,:dbname,:user,:password,:port,:extra,true,true)''', db_config


def _unify_config(conn_type: str, config: dict):
    if conn_type == 'mongo':
        config['dbname'] = config.pop('database', config.get('dbname', 'test'))
        config['user'] = config.pop('username', config.get('user'))
        if not config['dbname']:
            config['dbname'] = 'test'  # test if default database for Mongo
    if conn_type == 'aws' and 'extra' not in config:  # construct aws extra based on conf if extra not specified
        config.pop('host')  # minio host is encrypted in extra, shouldn't be taken from url
        config['extra'] = json.dumps({'host': config.get('url'),
                                      'aws_access_key_id': config.get('key_id'),
                                      'aws_secret_access_key': config.get('secret_key'),
                                      'region_name': config.get('region')})
    for field in ['host', 'dbname', 'user', 'password', 'port', 'extra']:  # to make sqlalchemy matcher happy
        if field not in config:
            config[field] = None


def _string_url_to_object(conn_type: str, url_str: str, fernet) -> dict:
    from sqlalchemy.engine.url import make_url
    try:
        url = make_url(url_str)
    except ArgumentError:
        debug('Can\'t parse {}. Will try {}'.format(url_str, conn_type + '://' + url_str))
        return _string_url_to_object(conn_type, conn_type + '://' + url_str, fernet)
    if url.password_original is not None:
        password = fernet.encrypt(url.password_original.encode()).decode()
    else:
        password = None
    return dict(host=url.host,
                dbname=url.database or None,
                user=url.username,
                password=password,
                port=url.port)
