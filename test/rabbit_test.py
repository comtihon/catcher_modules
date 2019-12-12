import json
from os.path import join

import ssl
import test
import numbers
from catcher.core.runner import Runner
from catcher.utils.file_utils import ensure_empty

from test.abs_test_class import TestClass


class RabbitTest(TestClass):
    config = {
        "username": "catcher",
        "password": "catcher",
        "server": "127.0.0.1:5672",
        "virtualhost": "catcher.virtual.host",
        "exchange": "catcher.test.exchange",
        "routingKey": "test",
        "queue": "catcher.test"
    }

    def __init__(self, method_name):
        super().__init__('rabbit', method_name)
        if self.config.get('virtualhost') is None:
            self.config['virtualhost'] = ''
        self.connectionParameters = self._get_connection_parameters(self.config)

    def _get_connection_parameters(self, config):
        import pika
        amqpURL = 'amqp{}://{}:{}@{}/{}'
        sslOptions = config.get('sslOptions')
        parameters = pika.URLParameters(
            amqpURL.format('s' if sslOptions else '', config['username'], config['password'], config['server'],
                           config['virtualhost']))
        if sslOptions is not None:
            parameters.ssl = True
            parameters.ssl_options = self._get_ssl_options(sslOptions)
        return parameters

    def _get_ssl_options(self, sslOptions):
        # PROTOCOL_TLSv1, PROTOCOL_TLSv1_1 or PROTOCOL_TLSv1_2
        sslVersion = {
            'PROTOCOL_TLSv1': ssl.PROTOCOL_TLSv1,
            'PROTOCOL_TLSv1_1': ssl.PROTOCOL_TLSv1_1,
            'PROTOCOL_TLSv1_2': ssl.PROTOCOL_TLSv1_2
        }
        # CERT_NONE, CERT_OPTIONAL or CERT_REQUIRED
        certReqs = {
            'CERT_NONE': ssl.CERT_NONE,
            'CERT_OPTIONAL': ssl.CERT_OPTIONAL,
            'CERT_REQUIRED': ssl.CERT_REQUIRED,
        }

        return {
            'ssl_version': sslVersion.get(sslOptions.get('ssl_version'), ssl.PROTOCOL_TLSv1_2),
            'ca_certs': sslOptions.get('ca_certs'),
            'keyfile': sslOptions.get('keyfile'),
            'certfile': sslOptions.get('certfile'),
            'cert_reqs': certReqs.get(sslOptions.get('cert_reqs'), 'CERT_NONE')
        }

    def setUp(self):
        super().setUp()
        ensure_empty(join(test.get_test_dir(self.test_name), 'resources'))
        import pika
        with pika.BlockingConnection(self.connectionParameters) as connection:
            channel = connection.channel()
            channel.exchange_declare(exchange=self.config['exchange'], exchange_type='topic')
            channel.queue_declare(queue=self.config['queue'])
            channel.queue_bind(exchange=self.config['exchange'], queue=self.config['queue'],
                               routing_key=self.config['routingKey'])

    def tearDown(self):
        super().tearDown()
        import pika
        with pika.BlockingConnection(self.connectionParameters) as connection:
            channel = connection.channel()
            channel.queue_unbind(self.config['queue'], self.config['exchange'], self.config['routingKey'])
            channel.queue_delete(self.config['queue'])
            channel.exchange_delete(self.config['exchange'])

    def test_publish_message(self):
        self.populate_file('main.yaml', '''---
            variables:
                rabbit_config:
                    server: 127.0.0.1:5671
                    virtualhost: catcher.virtual.host
                    sslOptions: {'ssl_version': 'PROTOCOL_TLSv1_2', 'cert_reqs': 'CERT_NONE'}
                    username: catcher
                    password: catcher
            steps:
                - rabbit:
                    publish:
                        config: '{{ rabbit_config }}'
                        exchange: 'catcher.test.exchange'
                        routing_key: 'test'
                        data: 'Catcher test message'
                        headers: {'test.header.1': 'header1', 'test.header.2': 'header1'}
                    name: 'publish message'
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

        import pika
        with pika.BlockingConnection(self.connectionParameters) as connection:
            channel = connection.channel()
            method_frame, header_frame, body = channel.basic_get(self.config['queue'])
            if method_frame:
                self.assertEqual('Catcher test message', body.decode('UTF-8'))
                self.assertEqual({'test.header.1': 'header1', 'test.header.2': 'header1'}, header_frame.headers)
                channel.basic_ack(method_frame.delivery_tag)

    def test_publish_message_from_file(self):
        self.populate_file('main.yaml', '''---
            variables:
                rabbit_config:
                    server: 127.0.0.1:5672
                    virtualhost: catcher.virtual.host
                    username: catcher
                    password: catcher
            steps:
                - rabbit:
                    publish:
                        config: '{{ rabbit_config }}'
                        exchange: 'catcher.test.exchange'
                        routing_key: 'test'
                        data_from_file: 'test-data.json'
                    name: 'From file: publish message'
            ''')
        self.populate_file('resources/test-data.json', '''
        {
            "testtool": "catcher", 
            "rating": 10
        }
        ''')

        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

        import pika
        with pika.BlockingConnection(self.connectionParameters) as connection:
            channel = connection.channel()
            method_frame, header_frame, body = channel.basic_get(self.config['queue'])
            if method_frame:
                actual_test_data = json.loads(body.decode('UTF-8'))
                self.assertEqual('catcher', actual_test_data["testtool"])
                self.assertEqual(10, actual_test_data["rating"])
                channel.basic_ack(method_frame.delivery_tag)

    def test_publish_json(self):
        self.populate_file('main.yaml', '''---
            variables:
                rabbit_config:
                    server: 127.0.0.1:5671
                    virtualhost: catcher.virtual.host
                    sslOptions: {'ssl_version': 'PROTOCOL_TLSv1_2', 'cert_reqs': 'CERT_NONE'}
                    username: catcher
                    password: catcher
            steps:
                - rabbit:
                    publish:
                        config: '{{ rabbit_config }}'
                        exchange: 'catcher.test.exchange'
                        routing_key: 'test'
                        data: "{{ {'key': RANDOM_INT}|tojson }}"
                        headers: {'test.header.1': 'header1', 'test.header.2': 'header1'}
                    name: 'publish message'
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

        import pika
        with pika.BlockingConnection(self.connectionParameters) as connection:
            channel = connection.channel()
            method_frame, header_frame, body = channel.basic_get(self.config['queue'])
            if method_frame:
                actual_test_data = json.loads(body.decode('UTF-8'))
                if not isinstance(actual_test_data["key"], numbers.Number):
                    raise Exception('incorrect message format ' + str(actual_test_data))
                channel.basic_ack(method_frame.delivery_tag)

    def test_consume_message(self):
        # publish a message to the exchange so that it can be read by the queue
        import pika
        with pika.BlockingConnection(self.connectionParameters) as connection:
            channel = connection.channel()
            channel.basic_publish(exchange=self.config['exchange'],
                                  routing_key=self.config['routingKey'],
                                  properties=None,
                                  body='Test queue message')

        self.populate_file('main.yaml', '''---
            variables:
                rabbit_config:
                    server: 127.0.0.1:5672
                    virtualhost: catcher.virtual.host
                    username: catcher
                    password: catcher
            steps:
                - rabbit:
                    consume:
                        config: '{{ rabbit_config }}'
                        queue: 'catcher.test'
                    register: {qMessage: '{{ OUTPUT }}'}
                    name: 'consume message'
                - check:
                    equals: {the: '{{ qMessage }}', is: 'Test queue message'}
                    name: 'Check: consume message'
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
