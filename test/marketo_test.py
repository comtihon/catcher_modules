from os.path import join

import mock
from catcher.core.runner import Runner

from test.abs_test_class import TestClass


class MarketoTest(TestClass):
    def __init__(self, method_name):
        super().__init__('marketo', method_name)

    @mock.patch('marketorestpython.client.MarketoClient', autospec=True)
    def test_read_lead(self, marketo_mock):
        instance = marketo_mock.return_value
        instance.execute.return_value = [{'id': 1, 'email': 'foo@baz.bar', 'custom_id': 'my_value_1'},
                                         {'id': 2, 'email': 'foo2@baz.bar', 'custom_id': 'my_value_2'}]
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
                - check: 
                    equals:
                      the: '{{ leads }}'
                      is: [{'id': 1, 'email': 'foo@baz.bar', 'custom_id': 'my_value_1'},
                           {'id': 2, 'email': 'foo2@baz.bar', 'custom_id': 'my_value_2'}] 
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        instance.execute.assert_called_with(method='get_multiple_leads_by_filter_type',
                                            filterType='custom_id',
                                            filterValues=['my_value_1', 'my_value_2'],
                                            fields=['id', 'email', 'custom_field_1'],
                                            batchSize=None)

    @mock.patch('marketorestpython.client.MarketoClient', autospec=True)
    def test_write_lead(self, marketo_mock):
        instance = marketo_mock.return_value
        instance.execute.return_value = [{'status': 'updated'}, {'status': 'updated'}]
        self.populate_file('main.yaml', '''---
            variables:
                marketo_config:
                    munchkin_id: test
                    client_id: test
                    client_secret: test
                email_1: 'foo@baz.bar'
                email_2: 'foo2@baz.bar'
            steps:
                - marketo:
                    write:
                        conf:
                            munchkin_id: '{{ marketo_munchkin_id }}'
                            client_id: '{{ marketo_client_id }}'
                            client_secret: '{{ marketo_client_secret }}'
                        action: 'updateOnly'
                        lookupField: 'custom_id'
                        leads:
                            - custom_id: 14
                              email: '{{ email_1 }}'
                              custom_field_1: 'some value'
                            - custom_id: 15
                              email: '{{ email_2 }}'
                              custom_field_1: 'some other value'
                    register: {response: '{{ OUTPUT }}'}
                - check: 
                    all:
                        of: '{{ response }}'
                        equals: {the: '{{ ITEM.status }}', is: 'updated'}
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        instance.execute.assert_called_with(method='create_update_leads',
                                            leads=[{'custom_id': 14, 'email': 'foo@baz.bar',
                                                    'custom_field_1': 'some value'},
                                                   {'custom_id': 15, 'email': 'foo2@baz.bar',
                                                    'custom_field_1': 'some other value'}],
                                            action='updateOnly',
                                            lookupField='custom_id',
                                            asyncProcessing='false',
                                            partitionName='Default')
