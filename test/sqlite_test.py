from os.path import join

import sqlite3
from catcher.core.runner import Runner

from test.abs_test_class import TestClass


class SQLiteTest(TestClass):
    def __init__(self, method_name):
        super().__init__('sqlite', method_name)

    @property
    def conf(self):
        return join(self.test_dir, "test.db")

    def setUp(self):
        super().setUp()
        print(self.conf)
        conn = sqlite3.connect(self.conf)
        cur = conn.cursor()
        cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer);")
        cur.execute("insert into test(id, num) values(1, 1);")
        cur.execute("insert into test(id, num) values(2, 2);")
        conn.commit()
        cur.close()
        conn.close()

    def tearDown(self):
        super().tearDown()

    def test_read_simple_query(self):
        self.populate_file('test_inventory.yml', '''
        sqlite: '/{}'
        '''.format(join(self.test_dir, "test.db")))

        self.populate_file('main.yaml', '''---
            steps:
                - sqlite:
                    request:
                        conf: '{{ sqlite }}'
                        query: 'select count(*) from test'
                    register: {documents: '{{ OUTPUT }}'}
                - check:
                    equals: {the: '{{ documents }}', is: 2}
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), join(self.test_dir, 'test_inventory.yml'))
        self.assertTrue(runner.run_tests())

    def test_write_simple_query(self):
        self.populate_file('main.yaml', '''---
                steps:
                    - sqlite:
                        request:
                            conf: '/{}'
                            query: 'insert into test(id, num) values(3, 3);'
                '''.format(join(self.test_dir, "test.db")))
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        conn = sqlite3.connect(self.conf)
        cur = conn.cursor()
        cur.execute("select count(*) from test")
        response = cur.fetchall()
        conn.commit()
        cur.close()
        conn.close()
        self.assertEqual([(3,)], response)

    def test_read_with_variables(self):
        self.populate_file('main.yaml', '''---
                variables:
                    db_conf: '/''' + join(self.test_dir, "test.db") + ''''\n
                steps:
                   - echo: {from: '{{ RANDOM_INT }}', register: {num: '{{ OUTPUT }}'}} 
                   - echo: {from: '{{ RANDOM_INT }}', register: {id: '{{ OUTPUT }}'}} 
                   - sqlite:
                        actions: 
                            - request:
                                conf: '{{ db_conf }}'
                                query: 'insert into test(id, num) values({{ id }}, {{ num }});'
                            - request:
                                conf: '{{ db_conf }}'
                                query: select * from test where id={{ id }}
                              register: {document: '{{ OUTPUT }}'}
                   - check: 
                        equals: {the: '{{ document[1] }}', is: '{{ num }}'} 
                ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
