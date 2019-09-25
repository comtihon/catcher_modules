import csv
import json
from abc import abstractmethod
from itertools import zip_longest
from typing import List

from catcher.utils.logger import debug

from catcher_modules.utils import generator_utils


class EmptyRow:
    pass


class SqlAlchemyDb:

    @property
    @abstractmethod
    def driver(self) -> str:
        pass

    @property
    @abstractmethod
    def default_port(self) -> int:
        pass

    @abstractmethod
    def table_info(self, table_name):
        pass

    def execute(self, body: dict):
        in_data = body['request']
        conf = in_data['conf']
        query = in_data['query']
        return self.__execute(conf, query)

    def populate(self, resources, conf=None, schema=None, data: dict = None, **kwargs):
        """ Populate database with prepared scripts (DDL or CSV with data).

        :Input:

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
        if schema is not None:
            with open(resources + '/' + schema) as fd:
                ddl_sql = fd.read()
                self.__execute(conf, ddl_sql)
        if data is not None and data:
            for table_name, path_to_csv in data.items():
                self.__populate_csv(conf, table_name, resources + '/' + path_to_csv)

    def check(self, resources, conf=None, schema=None, data: dict = None, strict=False, **kwargs):
        """ Check database schema and data.

        :Input:

        :compare: - compare the data in the database with the expected data.

        :conf:  postgres configuration. Can be a single line string or object. **Required**.

        - dbname: name of the database to connect to
        - user: database user
        - host: database host
        - password: user's password
        - port: database port

        :schema: path to the schema file. *Optional*

        :data: dictionary with keys = tables and values - paths to csv files with data. *Optional*

        :strict: Strictly check the data. Will pass only if no other data exists and the data is in the
         same order, as in the csv. *Optional* (default is false)

        :F.e.:
        - schema:
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

        - test:
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
        if schema is not None:
            self.__check_schema(conf, resources + '/' + schema)
        if data is not None:
            for table_name, path_to_csv in data.items():
                csv_file = resources + '/' + path_to_csv
                if strict:
                    self._check_data_strict(conf, table_name, csv_file)
                else:
                    self._check_data(conf, table_name, csv_file)

    def __populate_csv(self, conf, table_name, path_to_csv):
        with open(path_to_csv) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            line_count = 0
            from sqlalchemy import create_engine
            engine = create_engine(self.__form_conf(conf))

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

    def __automap_table(self, table_name: str, engine):
        from sqlalchemy.ext.automap import automap_base

        Base = automap_base()
        Base.prepare(engine, reflect=True)
        try:
            return Base.classes[table_name]
        except KeyError:
            raise Exception('Can\'t map table without primary key.')

    def _check_data(self, conf, table_name, path_to_csv):
        # TODO templates in csv?
        iter_csv = iter(generator_utils.csv_to_generator(path_to_csv))
        keys = next(iter_csv)

        from sqlalchemy import create_engine
        engine = create_engine(self.__form_conf(conf))
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

    def _check_data_strict(self, conf, table_name, path_to_csv):
        from sqlalchemy import create_engine
        engine = create_engine(self.__form_conf(conf))
        table = self.__automap_table(table_name, engine)
        csv_generator = generator_utils.csv_to_generator(path_to_csv)
        keys = next(iter(csv_generator))
        db_generator = generator_utils.table_to_generator(table, engine)
        sentinel = EmptyRow()
        res = all(self.compare_result_set(keys, a, b)
                  for a, b in zip_longest(csv_generator, db_generator, fillvalue=sentinel))
        if not res:
            raise Exception('Data check failed')

    def __execute(self, conf: str, query: str):
        from sqlalchemy import create_engine
        engine = create_engine(self.__form_conf(conf))
        # TODO use with
        connection = engine.connect()  # TODO pool pre-ping
        try:
            result = connection.execute(query)
            return SqlAlchemyDb.gather_response(result)
        finally:
            connection.close()

    def __form_conf(self, conf):
        if not isinstance(conf, str):
            return '{}://{}:{}@{}:{}/{}'.format(self.driver,
                                                conf['user'],
                                                conf['password'],
                                                conf['host'],
                                                conf.get('port', self.default_port),
                                                conf['dbname'])
        else:
            return self.driver + '://' + conf

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
