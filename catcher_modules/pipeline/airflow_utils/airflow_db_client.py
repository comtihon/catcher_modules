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


def _get_dag_id_by_task_id(task_id, connection):
    result = connection.execute('''
    select dag_id from task_instance ti where ti.task_id = '{}' order by execution_date desc limit 1
    '''.format(task_id))
    rows = [dict(r) for r in result.fetchall()]
    if not rows:
        raise Exception('Can\'t get dag_id by task_id {}'.format(task_id))
    return rows[0]['dag_id']
