from catcher.utils.file_utils import ensure_empty

import test
from os.path import join

import psycopg2
from catcher.core.runner import Runner

from test.abs_test_class import TestClass


class PostgresTest(TestClass):
    def __init__(self, method_name):
        super().__init__('postgres', method_name)

    @property
    def conf(self):
        return "dbname=test user=test host=localhost password=test port=5433"

    @property
    def connection(self):
        return psycopg2.connect(self.conf)

    def setUp(self):
        super().setUp()
        ensure_empty(join(test.get_test_dir(self.test_name), 'resources'))
        with self.connection as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE if not exists test (id serial PRIMARY KEY, num integer);")
            cur.execute("insert into test(id, num) values(1, 1) on conflict do nothing;")
            cur.execute("insert into test(id, num) values(2, 2) on conflict do nothing;")
            conn.commit()
            cur.close()

    def tearDown(self):
        super().tearDown()
        with self.connection as conn:
            cur = conn.cursor()
            cur.execute("DROP TABLE test;")
            cur.execute("DROP TABLE IF exists foo;")
            conn.commit()
            cur.close()

    def test_read_simple_query(self):
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
                - postgres:
                    request:
                        conf: '{{ postgres }}'
                        query: 'select count(*) from test'
                    register: {documents: '{{ OUTPUT }}'}
                - check:
                    equals: {the: '{{ documents.count }}', is: 2}
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), join(self.test_dir, 'test_inventory.yml'))
        self.assertTrue(runner.run_tests())

    def test_str_conf(self):
        self.populate_file('test_inventory.yml', '''
        postgres: 'test:test@localhost:5433/test'
        ''')

        self.populate_file('main.yaml', '''---
                steps:
                    - postgres:
                        request:
                            conf: '{{ postgres }}'
                            query: 'select count(*) from test'
                        register: {documents: '{{ OUTPUT }}'}
                    - check:
                        equals: {the: '{{ documents.count }}', is: 2}
                ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), join(self.test_dir, 'test_inventory.yml'))
        self.assertTrue(runner.run_tests())

    def test_conf_with_dialect(self):
        self.populate_file('test_inventory.yml', '''
                postgres: 'postgresql://test:test@localhost:5433/test'
                ''')

        self.populate_file('main.yaml', '''---
                        steps:
                            - postgres:
                                request:
                                    conf: '{{ postgres }}'
                                    query: 'select count(*) from test'
                                register: {documents: '{{ OUTPUT }}'}
                            - check:
                                equals: {the: '{{ documents.count }}', is: 2}
                        ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), join(self.test_dir, 'test_inventory.yml'))
        self.assertTrue(runner.run_tests())

    def test_write_simple_query(self):
        self.populate_file('main.yaml', '''---
                steps:
                    - postgres:
                        request:
                            conf: 'test:test@localhost:5433/test'
                            query: 'insert into test(id, num) values(3, 3);'
                ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        response = self.get_values('test')
        self.assertEqual([(1, 1), (2, 2), (3, 3)], response)

    def test_read_with_variables(self):
        self.populate_file('main.yaml', '''---
                variables:
                    pg_conf: 'test:test@localhost:5433/test'
                    num: '{{ RANDOM_INT }}'
                    id: '{{ RANDOM_INT }}'
                steps:
                   - postgres:
                        actions: 
                            - request:
                                conf: '{{ pg_conf }}'
                                query: 'insert into test(id, num) values({{ id }}, {{ num }});'
                            - request:
                                conf: '{{ pg_conf }}'
                                query: select * from test where id={{ id }}
                              register: {document: '{{ OUTPUT }}'}
                   - check: 
                        equals: {the: '{{ document.id }}', is: '{{ num }}'} 
                ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

    def test_read_date(self):
        with self.connection as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE if not exists foo (id serial PRIMARY KEY, payload json);")
            cur.execute("insert into foo values(1, '{ \"date\": \"1973-12-15\"}') on conflict do nothing;")

        self.populate_file('main.yaml', '''---
                variables:
                    pg_conf: 'test:test@localhost:5433/test'
                steps:
                    - postgres:
                         request:
                             conf: '{{ pg_conf }}'
                             query: "select payload ->> 'date' AS date from foo where id = 1"
                         register: {date: '{{ OUTPUT }}' }
                    - check: {equals: {the: '1973-12-15', is: '{{ date.date }}'}}
                ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

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
                                                postgres:
                                                    conf: 'test:test@localhost:5433/test'
                                                    schema: schema.sql
                                    ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        response = self.get_values('foo')
        self.assertEqual([(2, 'test2@test.org')], response)

    def test_populate_sql(self):
        self.populate_file('resources/schema.sql', '''
                                        CREATE TABLE if not exists foo(
                                            user_id      integer    primary key,
                                            email        varchar(36)    NOT NULL
                                        );
                                        {%- for user in users %}
                                        insert into foo values ({{user.num}}, '{{user.email}}');
                                        {%- endfor -%}
                                        ''')
        self.populate_file('main.yaml', '''---
                                            variables:
                                                users:
                                                    - num: 1
                                                      email: test1@test.org
                                                    - num: 2
                                                      email: test2@test.org
                                            steps:
                                                - postgres:
                                                    request:
                                                        conf: 'test:test@localhost:5433/test'
                                                        sql: schema.sql
                                            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        response = self.get_values('foo')
        self.assertEqual([(1, 'test1@test.org'), (2, 'test2@test.org')], response)
