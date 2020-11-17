import posixpath

from catcher.utils.logger import debug
from catcher_modules.exceptions.airflow_exceptions import OldAirflowVersionException
from requests import request


def trigger_dag(aiflow_url, dag_id, dag_config):
    url = posixpath.join(aiflow_url, 'api/experimental/dags/{}/dag_runs'.format(dag_id))
    r = request('POST', url, json=dag_config, headers={'content-type': 'application/json'})
    if r.status_code != 200:
        raise Exception('Can\'t trigger dag: {}'.format(r.json()))
    debug(r.json())
    message = r.json()['message']
    if not message.startswith('Created'):
        raise Exception('Dag run was not created: ' + message)
    # message text expected:
    # 'Created <DagRun init_data_sync @ 2020-01-04 13:22:16+00:00: manual__2020-01-04T13:22:16+00:00,
    # externally triggered: True>'
    return message.split(' ')[6].strip(',')


def unpause_dag(aiflow_url, dag_id):
    url = posixpath.join(aiflow_url, 'api/experimental/dags/{}/paused/false'.format(dag_id))
    r = request('GET', url)
    if r.status_code == 404:  # old airflow, rest api is not suported
        debug('Endpoint not found: ' + r.text)
        raise OldAirflowVersionException('Can\'t unpause the dag {}'.format(dag_id))
    if r.status_code != 200:
        debug(r.text)
        raise Exception('Can\'t unpause dag: {}'.format(dag_id))


def get_dag_run(aiflow_url: str, dag_id: str, run_id: str) -> dict:
    url = posixpath.join(aiflow_url, 'api/experimental/dags/{}/dag_runs'.format(dag_id))
    r = request('GET', url)
    if r.status_code != 200:
        raise Exception('Can\'t get list of run: {}'.format(r.json()))
    debug(r.json())
    dag_run = filter(lambda x: x['run_id'] == run_id, r.json())
    if not dag_run:
        raise Exception('No run_id {} found in runs for dag {}'.format(run_id, dag_id))
    return list(dag_run)[0]


def get_run_status(aiflow_url: str, dag_id: str, execution_date: str) -> str:
    url = posixpath.join(aiflow_url, 'api/experimental/dags/{}/dag_runs/{}'.format(dag_id, execution_date))
    r = request('GET', url)
    if r.status_code != 200:
        raise Exception('Can\'t get run status for {}:{}  {}'.format(dag_id, execution_date, r.json()))
    return r.json()['state']
