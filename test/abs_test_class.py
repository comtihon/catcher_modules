import os
import unittest
from os.path import join

import test
from catcher.utils import logger
from catcher.utils.logger import get_logger
from catcher.utils.file_utils import ensure_empty, remove_dir


class TestClass(unittest.TestCase):
    def __init__(self, test_name, method_name):
        super().__init__(method_name)
        self._test_name = test_name
        self._test_dir = test.get_test_dir(test_name)
        logger.configure('debug')
        get_logger().setLevel('DEBUG')

    @property
    def test_name(self):
        return self._test_name

    @property
    def test_dir(self):
        return join(os.getcwd(), self._test_dir)

    @property
    def connection(self):
        return None

    def setUp(self):
        ensure_empty(test.get_test_dir(self.test_name))

    def tearDown(self):
        remove_dir(test.get_test_dir(self.test_name))

    def populate_file(self, file: str, content: str):
        with open(join(self.test_dir, file), 'w') as f:
            f.write(content)

    def get_values(self, table):
        with self.connection as conn:
            cur = conn.cursor()
            cur.execute('select * from ' + table)
            response = cur.fetchall()
            conn.commit()
            cur.close()
            return response

    def populate_schema_file(self):
        self.populate_file('resources/schema.sql', '''
        CREATE TABLE foo(
            user_id      integer    primary key,
            email        varchar(36)    NOT NULL
        );
        ''')

    def populate_data_file(self):
        self.populate_file('resources/foo.csv', "user_id,email\n"
                                                "1,test1@test.com\n"
                                                "2,test2@test.com\n"
                           )
