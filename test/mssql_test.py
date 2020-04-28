from os.path import join

from sqlalchemy import create_engine

import test
from catcher.core.runner import Runner
from catcher.utils.file_utils import ensure_empty
from test.abs_test_class import TestClass


class MSSqlTest(TestClass):
    def __init__(self, method_name):
        super().__init__('mssql', method_name)

    @property
    def conf(self):
        return 'localhost', 'sa', 'Test1234', 'tempdb'

    @property
    def connection(self):
        engine = create_engine("mssql+pyodbc://sa:Test1234@localhost:1433/tempdb?driver=ODBC+Driver+17+for+SQL+Server")
        return engine.connect()

    def setUp(self):
        super().setUp()
        with self.connection as conn:
            conn.execute("CREATE TABLE test (id INT NOT NULL, num INT, PRIMARY KEY (id));")
            conn.execute("insert into test(id, num) values(1, 1);")
            conn.execute("insert into test(id, num) values(2, 2);")
        ensure_empty(join(test.get_test_dir(self.test_name), 'resources'))

    def tearDown(self):
        super().tearDown()
        with self.connection as conn:
            conn.execute("DROP TABLE test;")
            conn.execute("DROP TABLE if exists foo;")
            conn.execute("DROP TABLE if exists bar;")

    def test_read_simple_query(self):
        self.populate_file('test_inventory.yml', '''
        mssql:
            dbname: tempdb
            user: sa
            password: Test1234
            host: localhost
            port: 1433
            driver: ODBC Driver 17 for SQL Server
        ''')

        self.populate_file('main.yaml', '''---
            steps:
                - mssql:
                    request:
                        conf: '{{ mssql }}'
                        query: 'select count(*) as count from test'
                    register: {documents: '{{ OUTPUT.count }}'}
                - check:
                    equals: {the: '{{ documents }}', is: 2}
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), join(self.test_dir, 'test_inventory.yml'))
        self.assertTrue(runner.run_tests())

    def test_str_conf(self):
        self.populate_file('test_inventory.yml', '''
        mssql: 'sa:Test1234@localhost:1433/tempdb?driver=ODBC+Driver+17+for+SQL+Server'
        ''')

        self.populate_file('main.yaml', '''---
                steps:
                    - mssql:
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
                    - mssql:
                        request:
                            conf: 'sa:Test1234@localhost:1433/tempdb?driver=ODBC+Driver+17+for+SQL+Server'
                            query: 'insert into test(id, num) values(3, 3);'
                ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        response = self.get_values('test')
        self.assertEqual([(1, 1), (2, 2), (3, 3)], response)

    def test_read_with_variables(self):
        self.populate_file('main.yaml', '''---
                variables:
                    db_conf: 'sa:Test1234@localhost:1433/tempdb?driver=ODBC+Driver+17+for+SQL+Server'
                steps:
                   - echo: {from: '{{ RANDOM_INT }}', register: {num: '{{ OUTPUT }}'}} 
                   - echo: {from: '{{ RANDOM_INT }}', register: {id: '{{ OUTPUT }}'}} 
                   - mssql:
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

    def test_populate_int(self):
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
                                mssql:
                                    conf: 'sa:Test1234@localhost:1433/tempdb?driver=ODBC+Driver+17+for+SQL+Server'
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

    def test_expect_strict(self):
        self.populate_schema_file()
        self.populate_data_file()
        self.populate_file('main.yaml', '''---
                        steps:
                            - prepare:
                                populate:
                                    mssql:
                                        conf: 'sa:Test1234@localhost:1433/tempdb?driver=ODBC+Driver+17+for+SQL+Server'
                                        schema: schema.sql
                                        data:
                                            foo: foo.csv
                            - expect:
                                compare:
                                    mssql:
                                        conf: 'sa:Test1234@localhost:1433/tempdb?driver=ODBC+Driver+17+for+SQL+Server'
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
                                    mssql:
                                        conf: 'sa:Test1234@localhost:1433/tempdb?driver=ODBC+Driver+17+for+SQL+Server'
                                        schema: schema.sql
                                        data:
                                            foo: foo.csv
                            - expect:
                                compare:
                                    mssql:
                                        conf: 'sa:Test1234@localhost:1433/tempdb?driver=ODBC+Driver+17+for+SQL+Server'
                                        data:
                                            foo: foo.csv
                                    ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

    def get_values(self, table):
        with self.connection as conn:
            return conn.execute('select * from ' + table).fetchall()

