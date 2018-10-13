from os.path import join

import pymysql
from catcher.core.runner import Runner

from test.abs_test_class import TestClass


class MySqlTest(TestClass):
    def __init__(self, method_name):
        super().__init__('mysql', method_name)

    @property
    def conf(self):
        return {'host': 'localhost',
                'user': 'root',
                'password': 'test',
                'port': 3307,
                'db': 'test'}

    def setUp(self):
        super().setUp()
        conn = pymysql.connect(**self.conf)
        cur = conn.cursor()
        cur.execute("CREATE TABLE test (id INT NOT NULL, num INT, PRIMARY KEY (id));")
        cur.execute("insert into test(id, num) values(1, 1);")
        cur.execute("insert into test(id, num) values(2, 2);")
        conn.commit()
        cur.close()
        conn.close()

    def tearDown(self):
        super().tearDown()
        conn = pymysql.connect(**self.conf)
        cur = conn.cursor()
        cur.execute("DROP TABLE test;")
        conn.commit()
        cur.close()
        conn.close()

    def test_read_simple_query(self):
        self.populate_file('test_inventory.yml', '''
        mysql:
            dbname: test
            user: root
            password: test
            host: localhost
            port: 3307
        ''')

        self.populate_file('main.yaml', '''---
            steps:
                - mysql:
                    request:
                        conf: '{{ mysql }}'
                        query: 'select count(*) from test'
                    register: {documents: '{{ OUTPUT }}'}
                - check:
                    equals: {the: '{{ documents }}', is: 2}
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), join(self.test_dir, 'test_inventory.yml'))
        self.assertTrue(runner.run_tests())

    def test_str_conf(self):
        self.populate_file('test_inventory.yml', '''
        mssql: 'root:test@localhost:3307/test'
        ''')

        self.populate_file('main.yaml', '''---
                steps:
                    - mysql:
                        request:
                            conf: '{{ mssql }}'
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
                    - mysql:
                        request:
                            conf: 'root:test@localhost:3307/test'
                            query: 'insert into test(id, num) values(3, 3);'
                ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        conn = pymysql.connect(**self.conf)
        cur = conn.cursor()
        cur.execute("select count(*) from test")
        response = cur.fetchall()
        conn.commit()
        cur.close()
        conn.close()
        self.assertEqual(((3,),), response)

    def test_read_with_variables(self):
        self.populate_file('main.yaml', '''---
                variables:
                    db_conf: 'root:test@localhost:3307/test'
                steps:
                   - echo: {from: '{{ RANDOM_INT }}', register: {num: '{{ OUTPUT }}'}} 
                   - echo: {from: '{{ RANDOM_INT }}', register: {id: '{{ OUTPUT }}'}} 
                   - mysql:
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
