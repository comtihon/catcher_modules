from time import sleep

from catcher.steps.external_step import ExternalStep
from catcher.steps.step import update_variables, Step
from catcher.utils.logger import debug
from catcher_modules.exceptions.airflow_exceptions import OldAirflowVersionException

from catcher_modules.pipeline.airflow_utils import airflow_client, airflow_db_client


class Airflow(ExternalStep):
    """
    Allows you to run dag sync/async, get xcom and populate connections in Apache `Airflow <https://airflow.apache.org/>`_
    workflow management platform.

    :Input:

    :config: airflow config object

    - backend: airflow db backend. *Optional* (default is 'postgresql').
      See `db_utils https://github.com/comtihon/catcher_modules/blob/master/catcher_modules/utils/db_utils.py#L3`_
    - db_conf: see db_conf object. **Required**.
    - url: airflow url. **Required**.
    - populate_connections: fill in airflow connections from current inventory file. **Optional** (default is false)
    - fernet_key: fernet key. Used with populate_connections. **Optional**

    :db_conf: airflow db backend configuration.

    - dbname: name of the database to connect to
    - user: database user
    - host: database host
    - password: user's password
    - port: database port

    :run: Run dag. Return it's run id in case of sync false or wait till it is done.

    - config: airflow config object. **Required**.
    - dag_id: dag's id to run. **Required**.
    - dag_config: a dict of optional dag-config. *Optional*
    - sync: if true - will wait till the dag_run finished. *Optional* (default is false)
    - wait_timeout: wait timeout in seconds for sync=true. *Optional* (default is 5 sec)

    :run_status: Get a dag run object for run id

    - config: airflow config object. **Required**.
    - dag_id: dag's id to run. **Required**.
    - run_id: run id of a dag. Is returned from run with sync=false

    :get_xcom: Get xcom value

    - config: airflow config object. **Required**.
    - task_id: task id which pushed data to xcom. **Required**.
    - execution_date: dag's run execution date. Can be obtained via run_status.
      *Optional* Either execution_date or run_id must be set.
    - run_id: run id of a dag. *Optional* Either execution_date or run_id must be set.

    Run dag async and check it's status later manually.
    ::

        variables:
                db_conf: 'airflow:airflow@localhost:5433/airflow'
                airflow: 'http://127.0.0.1:8080'
        steps:
            - airflow:
                run:
                    config:
                        db_conf: '{{ db_conf }}'
                        url: '{{ airflow }}'
                    dag_id: 'init_data_sync'
                register: {run_id: '{{ OUTPUT }}'}
            - wait:
                seconds: 50
                for:
                    - airflow:
                        run_status:
                            config:
                                db_conf: '{{ db_conf }}'
                                url: '{{ airflow }}'
                            dag_id: 'init_data_sync'
                            run_id: '{{ run_id }}'
                        register: {status: '{{ OUTPUT }}'}
                    - check: {the: {{ status }}, is: 'failed'}

    Run dag and wait for it to be completed successfully.
    ::

        variables:
                db_conf: 'airflow:airflow@localhost:5433/airflow'
                airflow: 'http://127.0.0.1:8080'
        steps:
            - airflow:
                run:
                    config:
                        db_conf: '{{ db_conf }}'
                        url: '{{ airflow }}'
                    dag_id: 'init_data_sync'
                    sync: true
                    wait_timeout: 50

    Run dag, wait for it and get task's xcom.
    ::

        variables:
                db_conf: 'airflow:airflow@localhost:5433/airflow'
                airflow: 'http://127.0.0.1:8080'
        steps:
            - airflow:
                run:
                    config:
                        db_conf: '{{ db_conf }}'
                        url: '{{ airflow }}'
                    dag_id: 'execute_batch'
                    sync: true
                    wait_timeout: 50
                register: {run_id: '{{ OUTPUT }}'}
            - airflow:
                get_xcom:
                    config:
                        db_conf: '{{ db_conf }}'
                        url: '{{ airflow }}'
                    task_id: fill_data
                    run_id:  '{{ run_id }}'

    """

    @update_variables
    def action(self, includes: dict, variables: dict) -> dict or tuple:
        body = self.simple_input(variables)
        method = Step.filter_predefined_keys(body)  # run/run_status
        oper = body[method]
        if method == 'run':
            return variables, self._run_dag(oper, variables.get('INVENTORY_FILE'))
        if method == 'run_status':
            return variables, self._run_status(oper)
        if method == 'get_xcom':
            return variables, self._get_xcom(oper)
        raise Exception('Unknown method: {}'.format(method))

    def _run_dag(self, oper, inventory):
        dag_id = oper['dag_id']
        config = oper['config']
        db_conf = config['db_conf']
        backend = config.get('backend', 'postgresql')
        url = config['url']
        self._prepare_dag_run(dag_id, config, inventory)
        dag_config = oper.get('dag_config', {})
        run_id = airflow_client.trigger_dag(url, dag_id, dag_config)
        sync = oper.get('sync', False)
        if not sync:
            return run_id
        else:
            wait_timeout = oper.get('wait_timeout', 5)
            execution_date = airflow_client.get_dag_run(url, dag_id, run_id)['execution_date']
            state = self._wait_for_running(url, dag_id, execution_date, wait_timeout)
            if state != 'success':
                failed_task = airflow_db_client.get_failed_task(dag_id,
                                                                execution_date,
                                                                db_conf,
                                                                backend)
                raise Exception('Dag {} failed task {} with state {}'.format(dag_id, failed_task, state))
            return run_id

    @staticmethod
    def _prepare_dag_run(dag_id, config, inventory):
        db_conf = config['db_conf']
        backend = config.get('backend', 'postgresql')
        url = config['url']
        if not airflow_db_client.check_dag_exists(dag_id, db_conf, backend):
            errors = airflow_db_client.check_import_errors(dag_id, db_conf, backend)
            msg = 'No dag {} found.'.format(dag_id)
            if errors:
                msg = msg + ' Possible import errors: {}'.format(str(errors))
            raise Exception(msg)
        if inventory is not None and config.get('populate_connections', False):
            # fill connections from inventory to airflow
            airflow_db_client.fill_connections(inventory, db_conf, backend, config['fernet_key'])
        try:
            airflow_client.unpause_dag(url, dag_id)
        except OldAirflowVersionException:
            airflow_db_client.unpause_dag(dag_id, db_conf, backend)


    @staticmethod
    def _run_status(oper):
        dag_id = oper['dag_id']
        run_id = oper['run_id']
        aiflow_url = oper['config']['url']
        return airflow_client.get_dag_run(aiflow_url, dag_id, run_id)

    @staticmethod
    def _get_xcom(oper):
        task_id = oper['task_id']
        run_id = oper.get('run_id')
        execution_date = oper.get('execution_date')
        if run_id is None and execution_date is None:
            raise ValueError('Both run_id and execution_date are not specified!')
        config = oper['config']
        db_conf = config['db_conf']
        dialect = config.get('backend', 'postgresql')
        if execution_date is None:
            execution_date = airflow_db_client.get_execution_date_by_run_ud(run_id, db_conf, dialect)
        return airflow_db_client.get_xcom(task_id, execution_date, db_conf, dialect)

    @staticmethod
    def _wait_for_running(url, dag_id, execution_date, timeout):
        while True:
            state = airflow_client.get_run_status(url, dag_id, execution_date)
            debug(state)
            if state.lower() != 'running':
                return state.lower()
            if timeout > 0:
                sleep(1)
                timeout -= 1
            else:
                raise Exception('Dag {} still running'.format(dag_id))
