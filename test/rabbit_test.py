import json
import logging
from os.path import join

import pika

from catcher.core.runner import Runner
from test.abs_test_class import TestClass

logging.getLogger("pika").setLevel(logging.INFO)


class RabbitTest(TestClass):

    config = {
        "username": "catcher",
        "password": "catcher",
        "server": "localhost:5672",
        "exchange": "catcher.test.exchange",
        "routingKey": "test",
        "queue": "catcher.test"
    }

    rabbitChannel = None

    def __init__(self, method_name):
        super().__init__('rabbit', method_name)

    def setUp(self):
        super().setUp()
        import pika
        amqpURL = 'amqp://{}:{}@{}/'
        parameters = pika.URLParameters(amqpURL.format(self.config['username'], self.config['password'], self.config['server']))
        connection = pika.BlockingConnection(parameters)
        self.rabbitChannel = connection.channel()
        self.rabbitChannel.exchange_declare(exchange=self.config['exchange'],exchange_type='topic',durable=True)
        self.rabbitChannel.queue_declare(queue=self.config['queue'], auto_delete=False, exclusive=False)
        self.rabbitChannel.queue_bind(exchange=self.config['exchange'],queue=self.config['queue'],routing_key=self.config['routingKey'])
        

    def tearDown(self):
        super().tearDown()
        self.rabbitChannel.queue_unbind(self.config['queue'],self.config['exchange'],self.config['routingKey'],None)
        self.rabbitChannel.queue_delete(self.config['queue'])
        self.rabbitChannel.exchange_delete(self.config['exchange'])
        self.rabbitChannel.close()
        

    def test_publish_message(self):
        self.populate_file('main.yaml', '''---
            variables:
                rabbit_config:
                    server: localhost:5672
                    username: catcher
                    password: catcher
            steps:
                - rabbit:
                    publish:
                        config: '{{ rabbit_config }}'
                        exchange: 'catcher.test.exchange'
                        routing_key: 'test'
                        data: 'Catcher test message'
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        method_frame, header_frame, body = self.rabbitChannel.basic_get(self.config['queue'])
        if method_frame:
            self.assertEqual('Catcher test message', body.decode('UTF-8'))
            self.rabbitChannel.basic_ack(method_frame.delivery_tag)

    def test_consume_message(self):
        # publish a message to the exchange so that it can be read by the queue
        self.rabbitChannel.basic_publish(exchange=self.config['exchange'],
                             routing_key=self.config['routingKey'],
                             properties=None,body='Test queue message')
        self.populate_file('main.yaml', '''---
            variables:
                rabbit_config:
                    server: localhost:5672
                    username: catcher
                    password: catcher
            steps:
                - rabbit:
                    consume:
                        config: '{{ rabbit_config }}'
                        queue: 'catcher.test'
                    register: {qMessage: '{{ OUTPUT }}'}
                - check:
                    equals: {the: '{{ qMessage }}', is: 'Test queue message'}
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
           