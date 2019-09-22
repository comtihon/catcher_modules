import json
from os.path import join

import pika

from catcher.core.runner import Runner
from test.abs_test_class import TestClass


class RabbitTest(TestClass):
    def __init__(self, method_name):
        super().__init__('rabbit', method_name)

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
                        exchange: 'catcher.tests.exchange'
                        routing_key: 'test'
                        data: 'Catcher test message'
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
