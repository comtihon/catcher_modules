from os.path import join

import pymysql
import pytest
from catcher.core.runner import Runner
from catcher.utils.file_utils import ensure_empty

import test
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

    @property
    def connection(self):
        return pymysql.connect(**self.conf)

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
        ensure_empty(join(test.get_test_dir(self.test_name), 'resources'))

    def tearDown(self):
        super().tearDown()
        conn = pymysql.connect(**self.conf)
        cur = conn.cursor()
        cur.execute("DROP TABLE test;")
        cur.execute("DROP TABLE if exists foo;")
        cur.execute("DROP TABLE if exists bar;")
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
                        query: 'select count(*) as count from test'
                    register: {documents: '{{ OUTPUT.count }}'}
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
                            query: 'select count(*) as count from test'
                        register: {documents: '{{ OUTPUT.count }}'}
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
        response = self.get_values('test')
        self.assertEqual(((1, 1), (2, 2), (3, 3)), response)

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
                              register: {document: '{{ OUTPUT.num }}'}
                   - check: 
                        equals: {the: '{{ document }}', is: '{{ num }}'} 
                ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

    def test_populate(self):
        self.populate_file('resources/schema.sql', '''
                        CREATE TABLE foo(
                            user_id      integer    primary key,
                            email        varchar(36)    NOT NULL
                        );
                        ''')

        self.populate_file('resources/foo.csv', "user_id,email\n"
                                                "1,test1@test.com\n"
                                                "2,test2@test.com\n"
                           )

        self.populate_file('main.yaml', '''---
                            steps:
                                - prepare:
                                    populate:
                                        mysql:
                                            conf: 'root:test@localhost:3307/test'
                                            schema: schema.sql
                                            data:
                                                foo: foo.csv
                            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        response = self.get_values('foo')
        self.assertEqual(2, len(response))
        self.assertEqual(1, response[0][0])
        self.assertEqual('test1@test.com', response[0][1])
        self.assertEqual(2, response[1][0])
        self.assertEqual('test2@test.com', response[1][1])

    # TODO fixme. https://stackoverflow.com/questions/60138719/multiple-queries-with-mysqlpymysql
    @pytest.mark.skip(reason="Need fix for implementation")
    def test_populate_multiple_ddl(self):
        self.populate_file('resources/schema.sql', '''
                                        CREATE TABLE if not exists foo(
                                            user_id      integer    primary key,
                                            email        varchar(36)    NOT NULL
                                        );
                                        insert into foo values (1, \'test1@test.org\');
                                        truncate table foo;
                                        insert into foo values (2, \'test2@test.org\');
                                        ''')
        self.populate_file('main.yaml', '''---
                                    steps:
                                        - prepare:
                                            populate:
                                                mysql:
                                                    conf: 'root:test@localhost:3307/test'
                                                    schema: schema.sql
                                    ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        response = self.get_values('foo')
        self.assertEqual([(2, 'test2@test.org')], response)

    def test_expect_strict(self):
        self.populate_schema_file()
        self.populate_data_file()
        self.populate_file('main.yaml', '''---
                                    steps:
                                        - prepare:
                                            populate:
                                                mysql:
                                                    conf: 'root:test@localhost:3307/test'
                                                    schema: schema.sql
                                                    data:
                                                        foo: foo.csv
                                        - expect:
                                            compare:
                                                mysql:
                                                    conf: 'root:test@localhost:3307/test'
                                                    data:
                                                        foo: foo.csv
                                                    strict: true
                                    ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

    def test_expect(self):
        self.populate_schema_file()
        self.populate_data_file()
        self.populate_file('main.yaml', '''---
                                    steps:
                                        - prepare:
                                            populate:
                                                mysql:
                                                    conf: 'root:test@localhost:3307/test'
                                                    schema: schema.sql
                                                    data:
                                                        foo: foo.csv
                                        - expect:
                                            compare:
                                                mysql:
                                                    conf: 'root:test@localhost:3307/test'
                                                    data:
                                                        foo: foo.csv
                                    ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

    def get_values(self, table):
        conn = pymysql.connect(**self.conf)
        cur = conn.cursor()
        cur.execute('select * from ' + table)
        response = cur.fetchall()
        conn.commit()
        cur.close()
        conn.close()
        return response
