from os.path import join

import psycopg2
import test
from catcher.core.runner import Runner
from catcher.utils.file_utils import ensure_empty

from test.abs_test_class import TestClass


class PrepareTest(TestClass):
    def __init__(self, method_name):
        super().__init__('prepare', method_name)

    @property
    def conf(self):
        return "dbname=test user=test host=localhost password=test port=5433"

    @property
    def connection(self):
        return psycopg2.connect(self.conf)

    def setUp(self):
        super().setUp()
        ensure_empty(join(test.get_test_dir(self.test_name), 'resources'))

    def tearDown(self):
        super().tearDown()
        with self.connection as conn:
            cur = conn.cursor()
            cur.execute("DROP TABLE if exists foo;")
            cur.execute("DROP TABLE if exists bar;")
            conn.commit()
            cur.close()

    def test_populate_from_scratch(self):
        self.populate_file('resources/pg_schema.sql', '''
       
        CREATE TABLE foo(
            user_id      varchar(36)    primary key,
            email        varchar(36)    NOT NULL
        );
        
        CREATE TABLE bar(
            key           varchar(36)    primary key,
            value         varchar(36)    NOT NULL
        );
        ''')

        self.populate_file('resources/foo.csv', "user_id,email\n"
                                                "1,test1@test.com\n"
                                                "2,test2@test.com\n"
                           )
        self.populate_file('resources/bar.csv', "key,value\n"
                                                "k1,v1\n"
                                                "k2,v2\n"
                           )

        self.populate_file('test_inventory.yml', '''
        postgres:
            dbname: test
            user: test
            password: test
            host: localhost
            port: 5433
        ''')

        self.populate_file('main.yaml', '''---
            steps:
                - prepare:
                    populate:
                        postgres:
                            conf: '{{ postgres }}'
                            schema: pg_schema.sql
                            data:
                                foo: foo.csv
                                bar: bar.csv
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), join(self.test_dir, 'test_inventory.yml'))
        self.assertTrue(runner.run_tests())
        response = self.get_values('foo')
        self.assertEqual(2, len(response))
        self.assertEqual('1', response[0][0])
        self.assertEqual('test1@test.com', response[0][1])
        self.assertEqual('2', response[1][0])
        self.assertEqual('test2@test.com', response[1][1])
        response = self.get_values('bar')
        self.assertEqual(2, len(response))
        self.assertEqual('k1', response[0][0])
        self.assertEqual('v1', response[0][1])
        self.assertEqual('k2', response[1][0])
        self.assertEqual('v2', response[1][1])

    def test_populate_int(self):
        self.populate_file('resources/pg_schema.sql', '''
                CREATE TABLE foo(
                    user_id      integer    primary key,
                    email        varchar(36)    NOT NULL
                );
                ''')

        self.populate_file('resources/foo.csv', "user_id,email\n"
                                                "1,test1@test.com\n"
                                                "2,test2@test.com\n"
                           )

        self.populate_file('test_inventory.yml', '''
                        postgres: 'test:test@localhost:5433/test'
                ''')

        self.populate_file('main.yaml', '''---
                    steps:
                        - prepare:
                            populate:
                                postgres:
                                    conf: '{{ postgres }}'
                                    schema: pg_schema.sql
                                    data:
                                        foo: foo.csv
                    ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), join(self.test_dir, 'test_inventory.yml'))
        self.assertTrue(runner.run_tests())
        response = self.get_values('foo')
        self.assertEqual(2, len(response))
        self.assertEqual(1, response[0][0])
        self.assertEqual('test1@test.com', response[0][1])
        self.assertEqual(2, response[1][0])
        self.assertEqual('test2@test.com', response[1][1])

    def test_populate_templates(self):
        self.populate_file('resources/pg_schema.sql', '''
                CREATE TABLE foo(
                    user_id      integer    primary key,
                    email        varchar(36)    NOT NULL
                );
                ''')
        self.populate_file('resources/foo.csv', "user_id,email\n"
                                                "1,{{ email }}\n"
                           )
        self.populate_file('main.yaml', '''---
                    variables:
                        postgres: 'test:test@localhost:5433/test'
                        email: 'test@test.com'
                    steps:
                        - prepare:
                            populate:
                                postgres:
                                    conf: '{{ postgres }}'
                                    schema: pg_schema.sql
                                    data:
                                        foo: foo.csv
                    ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        response = self.get_values('foo')
        self.assertEqual(1, len(response))
        self.assertEqual(1, response[0][0])
        self.assertEqual('test@test.com', response[0][1])

    def test_populate_generate(self):
        self.populate_file('resources/pg_schema.sql', '''
                CREATE TABLE foo(
                    user_id      integer    primary key,
                    email        varchar(36)    NOT NULL
                );
                ''')
        self.populate_file('resources/foo.csv', "user_id,email\n"
                                                "{% for user in users %}"
                                                "{{ loop.index }},{{ user }}\n"
                                                "{% endfor %}"
                                                "4,other_email\n"
                           )
        self.populate_file('main.yaml', '''---
                    variables:
                        postgres: 'test:test@localhost:5433/test'
                        users: ['test_1', 'test_2', 'test_3']
                    steps:
                        - prepare:
                            populate:
                                postgres:
                                    conf: '{{ postgres }}'
                                    schema: pg_schema.sql
                                    data:
                                        foo: foo.csv
                    ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        response = self.get_values('foo')
        self.assertEqual(4, len(response))
        self.assertEqual(1, response[0][0])
        self.assertEqual('test_1', response[0][1])
        self.assertEqual('test_2', response[1][1])
        self.assertEqual('test_3', response[2][1])
        self.assertEqual('other_email', response[3][1])

    def test_prepare_json(self):
        self.populate_file('resources/pg_schema.sql', '''
                CREATE TABLE foo(
                    user_id      integer    primary key,
                    payload      json       NOT NULL
                );
                ''')
        self.populate_file('resources/foo.csv', "user_id,payload\n"
                                                "1,{\"date\": \"1990-07-20\"}"
                           )
        self.populate_file('main.yaml', '''---
                    variables:
                        postgres: 'test:test@localhost:5433/test'
                        users: ['test_1', 'test_2', 'test_3']
                    steps:
                        - prepare:
                            populate:
                                postgres:
                                    conf: '{{ postgres }}'
                                    schema: pg_schema.sql
                                    data:
                                        foo: foo.csv
                                    use_json: true
                        - postgres:
                            request:
                                conf: '{{ postgres }}'
                                query: "select payload ->> 'date' AS date from foo where user_id = 1"
                            register: {date: '{{ OUTPUT.date }}' }
                        - check: {equals: {the: '1990-07-20', is: '{{ date }}'}}
                    ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

    def test_prepare_variables_override(self):
        self.populate_file('resources/pg_schema.sql', '''
                CREATE TABLE foo(
                    user_id      integer    primary key,
                    email        varchar(36)    NOT NULL
                );
                ''')
        self.populate_file('resources/foo.csv', "user_id,email\n"
                                                "{% for user in users %}"
                                                "{{ loop.index }},{{ user }}\n"
                                                "{% endfor %}"
                                                "4,other_email\n"
                           )
        self.populate_file('main.yaml', '''---
                    variables:
                        postgres: 'test:test@localhost:5433/test'
                        users: ['test_1', 'test_2', 'test_3']
                    steps:
                        - prepare:
                            populate:
                                postgres:
                                    conf: '{{ postgres }}'
                                    schema: pg_schema.sql
                                    data:
                                        foo: foo.csv
                                variables:
                                    users: ['u_1', 'u_2', 'u_3']
                    ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        response = self.get_values('foo')
        self.assertEqual(4, len(response))
        self.assertEqual(1, response[0][0])
        self.assertEqual('u_1', response[0][1])
        self.assertEqual('u_2', response[1][1])
        self.assertEqual('u_3', response[2][1])
        self.assertEqual('other_email', response[3][1])
