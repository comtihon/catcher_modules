import csv
import json
import os
from io import StringIO
from abc import abstractmethod
from itertools import zip_longest
from typing import List

from catcher.utils.logger import debug
from catcher.utils.misc import fill_template_str

from catcher_modules.utils import generator_utils
from catcher_modules.utils import db_utils


class EmptyRow:
    pass


class SqlAlchemyDb:

    @property
    @abstractmethod
    def driver(self) -> str:
        pass

    @abstractmethod
    def table_info(self, table_name):
        pass

    def execute(self, body: dict):
        in_data = body['request']
        conf = in_data['conf']
        query = in_data['query']
        return self.__execute(conf, query)

    def populate(self, variables, conf=None, schema=None, data: dict = None, **kwargs):
        """
        :Input:  Populate database with prepared scripts (DDL or CSV with data).

        :populate: - populate a database with the data and/or run DDL to create schema.

        :conf:  postgres configuration. Can be a single line string or object. **Required**.

        - dbname: name of the database to connect to
        - user: database user
        - host: database host
        - password: user's password
        - port: database port

        :schema: path to the schema file. *Optional*

        :data: dictionary with keys = tables and values - paths to csv files with data. *Optional*

        :F.e.:
        populate postgres
        ::
            variables:
                pg_schema: schema.sql
                pg_data:
                    foo: foo.csv
                    bar: bar.csv
            steps:
                - prepare:
                    populate:
                        postgres:
                            conf: {{ pg_conf }}
                            schema: {{ pg_schema }}
                            data: {{ pg_data }}

        """
        resources = variables['RESOURCES_DIR']
        if schema is not None:
            with open(resources + '/' + schema) as fd:
                ddl_sql = fd.read()
                self.__execute(conf, ddl_sql)
        if data is not None and data:
            for table_name, path_to_csv in data.items():
                self.__populate_csv(conf, table_name, resources + '/' + path_to_csv, variables)

    def expect(self, variables, conf=None, schema=None, data: dict = None, strict=False, **kwargs):
        """
        :Input: Check database schema and data.

        :compare: - compare the data in the database with the expected data.

        :conf:  postgres configuration. Can be a single line string or object. **Required**.

        - dbname: name of the database to connect to
        - user: database user
        - host: database host
        - password: user's password
        - port: database port

        :schema: path to the schema file. *Optional*

        :data: dictionary with keys = tables and values - paths to csv files with data.
               Jinja2 templates supported. *Optional*

        :strict: Strictly check the data. Will pass only if no other data exists and the data is in the
         same order, as in the csv. *Optional* (default is false)

        :F.e.:
        `schema`
        ::
            {
                "foo": {
                    "columns": {
                        "user_id": "integer",
                        "email": "varchar(36)"
                    },
                    "keys": ["user_id"]
                },
                "bar": {
                    "columns": {
                        "key": "varchar(36)",
                        "value": "varchar(36)"
                    },
                    "keys": ["key"]
                }
            }

        `test`
        ::
            steps:
                - expect:
                    compare:
                        postgres:
                            conf: 'test:test@localhost:5433/test'
                            schema: check_schema.json
                            data:
                                foo: foo.csv
                                bar: bar.csv
                            strict: true

        """
        resources = variables['RESOURCES_DIR']
        if schema is not None:
            self.__check_schema(conf, os.path.join(resources, schema))
        if data is not None:
            for table_name, path_to_csv in data.items():
                csv_file = os.path.join(resources, path_to_csv)
                csv_stream = self.__read_n_fill_csv(csv_file, variables)
                if strict:
                    self._check_data_strict(conf, table_name, csv_stream)
                else:
                    self._check_data(conf, table_name, csv_stream)

    def __populate_csv(self, conf, table_name, path_to_csv, variables):
        csv_reader = csv.reader(self.__read_n_fill_csv(path_to_csv, variables), delimiter=',')
        line_count = 0
        engine = db_utils.get_engine(conf, self.driver)

        row_table = self.__automap_table(table_name, engine)
        from sqlalchemy.orm import Session
        session = Session(engine)
        names = None
        for row in csv_reader:
            if line_count == 0:
                names = row
                line_count += 1
            else:
                session.add(row_table(**dict(zip(names, row))))
        session.commit()

    def __check_schema(self, conf, schema_file):
        with open(schema_file) as fd:
            data = json.load(fd)
            for table, meta in data.items():
                info = self.table_info(table)
        return True

    def _check_data(self, conf, table_name, csv_stream):
        iter_csv = iter(generator_utils.csv_to_generator(csv_stream))
        keys = next(iter_csv)

        engine = db_utils.get_engine(conf, self.driver)
        row_table = self.__automap_table(table_name, engine)
        from sqlalchemy.orm import Session
        session = Session(engine)
        has_error = False
        try:
            for row in iter_csv:
                found = session.query(row_table).filter_by(**dict(zip(keys, row))).first()
                if found is None:
                    debug('No ' + str(row) + ' found')
                    has_error = True
            if has_error:
                raise Exception('Data check failed')
        finally:
            session.close()

    def _check_data_strict(self, conf, table_name, csv_stream):
        engine = db_utils.get_engine(conf, self.driver)
        table = self.__automap_table(table_name, engine)
        csv_generator = generator_utils.csv_to_generator(csv_stream)
        keys = next(iter(csv_generator))
        db_generator = generator_utils.table_to_generator(table, engine)
        sentinel = EmptyRow()
        res = all(self.compare_result_set(keys, a, b)
                  for a, b in zip_longest(csv_generator, db_generator, fillvalue=sentinel))
        if not res:
            raise Exception('Data check failed')

    def __execute(self, conf: str, query: str):
        engine = db_utils.get_engine(conf, self.driver)
        with engine.connect() as connection:
            result = connection.execute(query)
            return SqlAlchemyDb.gather_response(result)

    @classmethod
    def __automap_table(cls, table_name: str, engine):
        from sqlalchemy.ext.automap import automap_base

        Base = automap_base()
        Base.prepare(engine, reflect=True)
        try:
            return Base.classes[table_name]
        except KeyError:
            raise Exception('Can\'t map table without primary key.')

    @classmethod
    def __read_n_fill_csv(cls, csv_path, variables):
        with open(csv_path) as csv_file:
            csv_content = fill_template_str(csv_file.read(), variables)
        return StringIO(csv_content)

    @staticmethod
    def gather_response(cursor):
        try:
            response = cursor.fetchall()
            if len(response) == 1:  # for only one value select * from .. where id = 1 -> [('a', 1, 2)]
                response = response[0]
                if len(response) == 1:  # for only one value select count(*) from ... -> (2,)
                    response = response[0]
            return response
        except Exception as e:
            debug('Execution error {}'.format(e))
            return None

    @staticmethod
    def compare_result_set(keys: List[str], values: List[str], db_row):
        if isinstance(values, EmptyRow):
            debug('Got more data than expected: ' +
                  str({key: value for (key, value) in db_row.__dict__.items() if key != '_sa_instance_state'}))
            return False
        expected = dict(zip(keys, values))
        if isinstance(db_row, EmptyRow):
            debug('Missing data: ' + str(expected))
            return False
        for key, value in expected.items():
            if not hasattr(db_row, key):
                debug('No ' + str(key) + ' found in ' +
                      str({key: value for (key, value) in db_row.__dict__.items() if key != '_sa_instance_state'}))
                return False
            if str(getattr(db_row, key)) != value:
                debug('Value mismatch for ' + str(key) + ': got '
                      + str(getattr(db_row, key)) + ', expect: ' + str(value))
                return False
        return True
