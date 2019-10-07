from os.path import join

import psycopg2
import test
from catcher.core.runner import Runner
from catcher.utils.file_utils import ensure_empty

from test.abs_test_class import TestClass


class ExpectTest(TestClass):
    def __init__(self, method_name):
        super().__init__('expect', method_name)

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
            cur.execute('''
            CREATE TABLE if not exists foo(
            user_id      integer    primary key,
            email        varchar(36)    NOT NULL
            );

            CREATE TABLE if not exists bar(
                key           varchar(36)    primary key,
                value         varchar(36)    NOT NULL
            );

            insert into foo values(1, 'test1@test.com'),(2, 'test2@test.com') ON CONFLICT DO NOTHING;
            insert into bar values('k1','v1'),('k2', 'v2') ON CONFLICT DO NOTHING;
            ''')
            conn.commit()
            cur.close()

    def tearDown(self):
        super().tearDown()
        with self.connection as conn:
            cur = conn.cursor()
            cur.execute("DROP TABLE if exists foo;")
            cur.execute("DROP TABLE if exists bar;")
            conn.commit()
            cur.close()

    def test_check_schema(self):
        self.populate_file('resources/check_schema.json', '''
                {
                    "foo": {
                        "columns": [
                            {"user_id": "integer"},
                            {"email": "varchar(36)}"
                        ],
                        "keys": ["user_id"]
                    },
                    "bar": {
                        "columns": [
                            {"key": "varchar(36)"},
                            {"value": "varchar(36)"}
                        ],
                        "keys": ["key"]
                    }
                }''')  # TODO add index on value and check it.
        pass

    def test_expect(self):
        self.populate_file('resources/foo.csv', "user_id,email\n"
                                                "1,test1@test.com\n"
                                                "2,test2@test.com\n"
                           )
        self.populate_file('resources/bar.csv', "key,value\n"
                                                "k1,v1\n"
                                                "k2,v2\n"
                           )

        self.populate_file('main.yaml', '''---
            steps:
                - expect:
                    compare:
                        postgres:
                            conf: 'test:test@localhost:5433/test'
                            data:
                                foo: foo.csv
                                bar: bar.csv
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

    def test_expect_false_positive(self):
        self.populate_file('resources/foo.csv', "user_id,email\n"
                                                "2,test2@test.com\n"
                                                "3,test3@test.com\n"
                           )

        self.populate_file('main.yaml', '''---
                    steps:
                        - expect:
                            compare:
                                postgres:
                                    conf: 'test:test@localhost:5433/test'
                                    data: {foo: foo.csv}
                    ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertFalse(runner.run_tests())

    def test_expect_strict(self):
        self.populate_file('resources/foo.csv', "user_id,email\n"
                                                "1,test1@test.com\n"
                                                "2,test2@test.com\n"
                           )
        self.populate_file('resources/bar.csv', "key,value\n"
                                                "k1,v1\n"
                                                "k2,v2\n"
                           )

        self.populate_file('main.yaml', '''---
                    steps:
                        - expect:
                            compare:
                                postgres:
                                    conf: 'test:test@localhost:5433/test'
                                    data:
                                        foo: foo.csv
                                        bar: bar.csv
                                    strict: true
                    ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

    def test_expect_strict_false_positive_mismatch(self):
        self.populate_file('resources/foo.csv', "user_id,email\n"
                                                "1,test1@test.com\n"
                                                "2,WRONG_DATA\n"
                           )

        self.populate_file('main.yaml', '''---
                            steps:
                                - expect:
                                    compare:
                                        postgres:
                                            conf: 'test:test@localhost:5433/test'
                                            data:
                                                foo: foo.csv
                                            strict: true
                            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertFalse(runner.run_tests())

    def test_expect_strict_false_positive_less_expected(self):
        self.populate_file('resources/foo.csv', "user_id,email\n"
                                                "1,test1@test.com\n"
                           )

        self.populate_file('main.yaml', '''---
                            steps:
                                - expect:
                                    compare:
                                        postgres:
                                            conf: 'test:test@localhost:5433/test'
                                            data:
                                                foo: foo.csv
                                            strict: true
                            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertFalse(runner.run_tests())

    def test_expect_strict_false_positive_more_expected(self):
        self.populate_file('resources/foo.csv', "user_id,email\n"
                                                "1,test1@test.com\n"
                                                "2,test2@test.com\n"
                                                "3,test2@test.com\n"
                           )

        self.populate_file('main.yaml', '''---
                            steps:
                                - expect:
                                    compare:
                                        postgres:
                                            conf: 'test:test@localhost:5433/test'
                                            data:
                                                foo: foo.csv
                                            strict: true
                            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertFalse(runner.run_tests())

    def test_expect_template(self):
        self.populate_file('resources/foo.csv', "user_id,email\n"
                                                "1,{{name}}1@test.com\n"
                                                "2,{{name}}2@test.com\n"
                           )
        self.populate_file('main.yaml', '''---
            variables:
                name: 'test'
            steps:
                - expect:
                    compare:
                        postgres:
                            conf: 'test:test@localhost:5433/test'
                            data:
                                foo: foo.csv
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

    def test_expect_template_strict(self):
        self.populate_file('resources/foo.csv', "user_id,email\n"
                                                "1,{{name}}1@test.com\n"
                                                "2,{{name}}2@test.com\n"
                           )
        self.populate_file('main.yaml', '''---
            variables:
                name: 'test'
            steps:
                - expect:
                    compare:
                        postgres:
                            conf: 'test:test@localhost:5433/test'
                            data:
                                foo: foo.csv
                            strict: true
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

