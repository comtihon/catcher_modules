from abc import abstractmethod

from catcher.utils.logger import debug


class SqlAlchemyDb:

    @property
    @abstractmethod
    def driver(self) -> str:
        pass

    @property
    @abstractmethod
    def default_port(self) -> int:
        pass

    def execute(self, body: dict):
        from sqlalchemy import create_engine
        in_data = body['request']
        conf = in_data['conf']
        query = in_data['query']
        if isinstance(conf, str):
            engine = create_engine(self.driver + '://' + conf)
        else:
            engine = create_engine('{}://{}:{}@{}:{}/{}'.format(self.driver,
                                                                conf['user'],
                                                                conf['password'],
                                                                conf['host'],
                                                                conf.get('port', self.default_port),
                                                                conf['dbname']))
        connection = engine.connect()
        try:
            result = connection.execute(query)
            return SqlAlchemyDb.gather_response(result)
        finally:
            connection.close()

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
