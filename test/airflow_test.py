import json
import os
from shutil import copyfile

import pytest
from catcher.utils.file_utils import ensure_empty
from catcher.utils.logger import error
from cryptography.fernet import Fernet
from datetime import datetime
import docker

import test
from os.path import join, dirname
from catcher.core.runner import Runner
from test.abs_test_class import TestClass


class AirflowTest(TestClass):
    def __init__(self, method_name):
        super().__init__('airflow', method_name)

    @property
    def conf(self):
        return "postgresql://test:test@localhost:5433/test"

    @property
    def fernet(self):
        return "zp8kV516l9tKzqq9pJ2Y6cXbM3bgEWIapGwzQs6jio4="

    def setUp(self):
        super().setUp()
        ensure_empty(join(test.get_test_dir(self.test_name), 'resources'))
        self.add_dag('airflow_hello_world.py')

    def tearDown(self):
        super().tearDown()
        # we shouldn't recreate directory itself, as it will unlink docker volume
        path = join(dirname(test.__file__), '../dags')
        if os.path.isdir(path):
            for f in os.listdir(path):
                path_in_dir = join(path, f)
                if not os.path.isdir(path_in_dir):
                    os.remove(path_in_dir)
        self.delete_dag(self.dag_id)


    @pytest.mark.skip(reason="too heavy for travis")
    def test_populate_connections(self):
        """
        Catcher's Airflow step should populate Airflow connections based on it's inventory
        """
        self.dag_id = 'hello_world'
        self.populate_file('test_inventory.yml', '''
                postgres_conf_1: 'test:test@localhost:5433/test'
                postgres_conf_2:
                    url: 'test:test@localhost:5433/test'
                    type: postgres
                postgres_conf_3:
                    dbname: 'test'
                    user: 'test'
                    password: 'test'
                    host: 'localhost'
                    port: 5433
                    type: 'postgres'
                postgres_conf_4:
                    url: 'test:test@localhost:5433/test'
                    type: postgres
                    extras: '{"key":"value"}'
                mongodb_conf_1:
                    url: 'mongodb://username:password@host:27017'
                    type: mongo
                mongodb_conf_2:
                    database: test
                    username: username
                    password: password
                    host: localhost
                    port: 27017
                    type: mongo
                    extras: '{"key":"value"}'
                minio_conf_1:
                    type: 'aws'
                    url: http://minio:9000
                    key_id: minio
                    secret_key: minio123
                minio_conf_2:
                    type: 'aws'
                    extra: '{"host":"http://minio:9000","aws_access_key_id":"minio","aws_secret_access_key":"minio123"}'
                airflow:
                    url: 'http://127.0.0.1:8080'
                    type: 'http'
                ''')

        self.populate_file('main.yaml', f'''---
                        steps:
                            - airflow:
                                run:
                                    config:
                                        db_conf: '{{{{ postgres_conf_2 }}}}'
                                        url: '{{{{ airflow.url }}}}'
                                        populate_connections: true
                                        fernet_key: zp8kV516l9tKzqq9pJ2Y6cXbM3bgEWIapGwzQs6jio4=
                                    dag_id: '{self.dag_id}'
                                    sync: true
                                    wait_timeout: 50
                        ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), join(self.test_dir, 'test_inventory.yml'))
        self.assertTrue(runner.run_tests())
        self._check_connection('postgres_conf_1', None, None)  # wasn't imported as type is missing
        self._check_connection('postgres_conf_2',
                               {'host': 'localhost', 'schema': 'test', 'login': 'test', 'port': 5433,
                                'password': 'test'}, None)
        self._check_connection('postgres_conf_3',
                               {'host': 'localhost', 'schema': 'test', 'login': 'test', 'port': 5433,
                                'password': 'test'}, None)
        self._check_connection('postgres_conf_4',
                               {'host': 'localhost', 'schema': 'test', 'login': 'test', 'port': 5433,
                                'password': 'test'}, '{"key":"value"}')
        self._check_connection('mongodb_conf_1',
                               {'host': 'host', 'schema': 'test', 'login': 'username', 'port': 27017,
                                'password': 'password'}, None)
        self._check_connection('mongodb_conf_2',
                               {'host': 'localhost', 'schema': 'test', 'login': 'username', 'port': 27017,
                                'password': 'password'}, '{"key":"value"}')
        self._check_connection('minio_conf_1',
                               {},
                               '{"host":"http://minio:9000","aws_access_key_id":"minio","aws_secret_access_key":"minio123"}')
        self._check_connection('minio_conf_2',
                               {}, json.dumps({'host': 'http://minio:9000',
                                               'aws_access_key_id': 'minio',
                                               'aws_secret_access_key': 'minio123',
                                               'region_name': None}))
        self._check_connection('airflow', {'host': '127.0.0.1', 'port': 8080}, None)

    @pytest.mark.skip(reason="too heavy for travis")
    def test_trigger_dag_with_dag_config(self):
        """
        Catcher's Airflow step should be able to trigger dag with a specific execution date
        """
        execution_date = datetime(2020, 3, 18, 0, 59, 59)
        airflow_url = 'http://127.0.0.1:8080'
        self.dag_id = 'hello_world'

        self.populate_file('test_inventory_2.yml', f'''
                postgres_conf_2:
                    url: 'test:test@localhost:5433/test'
                    type: postgres
                airflow:
                    url: '{airflow_url}'
                    type: 'http'
                ''')

        self.populate_file('main_2.yaml', f'''---
                        steps:
                            - airflow:
                                run:
                                    config:
                                        db_conf: '{{{{ postgres_conf_2 }}}}'
                                        url: '{{{{ airflow.url }}}}'
                                        populate_connections: true
                                        fernet_key: zp8kV516l9tKzqq9pJ2Y6cXbM3bgEWIapGwzQs6jio4=
                                    dag_config:
                                        execution_date: '{str(execution_date)}'
                                    dag_id: '{self.dag_id}'
                                    sync: true
                                    wait_timeout: 50
                        ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main_2.yaml'), join(self.test_dir, 'test_inventory_2.yml'))
        self.assertTrue(runner.run_tests())


    def delete_dag(self, dag_id):
        client = docker.from_env()
        airflow = client.containers.get('catcher_modules_webserver_1')
        res = airflow.exec_run(cmd=f'/entrypoint.sh airflow delete_dag -y {dag_id}')
        if res.exit_code != 0:
            error("Can't delete dag {}: {}".format(dag_id, res.output.decode()))
            raise Exception(f"Can't delete dag {dag_id}")

    def add_dag(self, dag_name: str):
        copyfile(join(self.global_resource_dir, dag_name), join(dirname(test.__file__), '../dags', dag_name))
        client = docker.from_env()
        airflow = client.containers.get('catcher_modules_webserver_1')
        res = airflow.exec_run(cmd='/entrypoint.sh python -c "from airflow.models import DagBag; d = DagBag();"')
        if res.exit_code != 0:
            error("Can't trigger airflow dagbag refresh: {}".format(res.output.decode()))
            raise Exception("Can't trigger airflow dagbag refresh")

    def _check_connection(self, conn_id, expected, expected_extra):
        from sqlalchemy import create_engine
        engine = create_engine(self.conf)
        with engine.connect() as connection:
            res = connection.execute('select * from connection where conn_id = \'{}\''.format(conn_id))
            if res.returns_rows:
                result = [dict(r) for r in res]
                if expected is None:
                    self.assertEqual([], result)
                else:
                    self.assertEqual(1, len(result))
                    result = result[0]
                    if expected_extra is None:
                        self.assertIsNone(result['extra'])
                    for key, value in result.items():
                        if key not in expected:
                            continue
                        if key == 'password':
                            self.assertEqual(self._decode_value(value), expected[key])
                        elif key == 'extra':
                            self.assertEqual(self._decode_value(value), expected_extra)
                        else:
                            self.assertEqual(value, expected[key])

    def _decode_value(self, value):
        f = Fernet(self.fernet.encode())
        return f.decrypt(value.encode()).decode()
