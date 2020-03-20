from os.path import join
import test

import sqlite3
from catcher.core.runner import Runner
from catcher.utils.file_utils import ensure_empty

from test.abs_test_class import TestClass


class SQLiteTest(TestClass):
    def __init__(self, method_name):
        super().__init__('sqlite', method_name)

    @property
    def conf(self):
        return join(self.test_dir, "test.db")

    @property
    def connection(self):
        return sqlite3.connect(self.conf)

    def setUp(self):
        super().setUp()
        print(self.conf)
        with self.connection as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer);")
            cur.execute("insert into test(id, num) values(1, 1);")
            cur.execute("insert into test(id, num) values(2, 2);")
            conn.commit()
            cur.close()
        ensure_empty(join(test.get_test_dir(self.test_name), 'resources'))

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
                    - sqlite:
                        request:
                            conf: '/{}'
                            query: 'insert into test(id, num) values(3, 3);'
                '''.format(join(self.test_dir, "test.db")))
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        response = self.get_values('test')
        self.assertEqual([(1, 1), (2, 2), (3, 3)], response)

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
                              register: {document: '{{ OUTPUT.num }}'}
                   - check: 
                        equals: {the: '{{ document }}', is: '{{ num }}'} 
                ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

    def test_populate(self):
        self.populate_schema_file()
        self.populate_data_file()

        self.populate_file('main.yaml', '''---
                            steps:
                                - prepare:
                                    populate:
                                        sqlite:
                                            conf: '/{}'
                                            schema: schema.sql
                                            data:
                                                foo: foo.csv
                            '''.format(join(self.test_dir, "test.db")))
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
        db_file = join(self.test_dir, "test.db")
        self.populate_file('main.yaml', '''---
                                    steps:
                                        - prepare:
                                            populate:
                                                sqlite:
                                                    conf: '/{}'
                                                    schema: schema.sql
                                                    data:
                                                        foo: foo.csv
                                        - expect:
                                            compare:
                                                sqlite:
                                                    conf: '/{}'
                                                    data:
                                                        foo: foo.csv
                                                    strict: true
                                    '''.format(db_file, db_file))
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

    def test_expect(self):
        self.populate_schema_file()
        self.populate_data_file()
        db_file = join(self.test_dir, "test.db")
        self.populate_file('main.yaml', '''---
                                    steps:
                                        - prepare:
                                            populate:
                                                sqlite:
                                                    conf: '/{}'
                                                    schema: schema.sql
                                                    data:
                                                        foo: foo.csv
                                        - expect:
                                            compare:
                                                sqlite:
                                                    conf: '/{}'
                                                    data:
                                                        foo: foo.csv
                                    '''.format(db_file, db_file))
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

