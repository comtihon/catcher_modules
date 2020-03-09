from os.path import join

import mock
from catcher.core.runner import Runner

from test.abs_test_class import TestClass


class MarketoTest(TestClass):
    def __init__(self, method_name):
        super().__init__('marketo', method_name)

    @mock.patch('marketorestpython.client.MarketoClient', autospec=True)
    def test_read_lead(self, marketo_mock):
        self.populate_file('main.yaml', '''---
            variables:
                marketo_config:
                    munchkin_id: test
                    client_id: test
                    client_secret: test
            steps:
                - marketo:
                    read:
                        conf:
                            munchkin_id: '{{ marketo_munchkin_id }}'
                            client_id: '{{ marketo_client_id }}'
                            client_secret: '{{ marketo_client_secret }}'
                        fields: ['id', 'email', 'custom_field_1']
                        filter_key: 'custom_id'
                        filter_value: ['my_value_1', 'my_value_2']
                    register: {leads: '{{ OUTPUT }}'}
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
