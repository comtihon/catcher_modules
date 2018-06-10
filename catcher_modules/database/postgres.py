import psycopg2
from catcher.steps.external_step import ExternalStep
from psycopg2._psycopg import ProgrammingError

DOCUMENTATION = '''
---
module: postgres
description: Postgres Catcher module
options:
    conf:
        description:
            - postgres configuration. Can be a single line string or object.
        required: true
        values:
            dbname: name of the database to connect to
            user: database user
            host: database host
            password: user's password
            port: database port
    query:
        description:
            - query to run.
        required: true
'''
EXAMPLES = '''
# select all from test, use object configuration
postgres:
    request:
        conf: 
            dbname: test
            user: test
            password: test
            host: localhost
            port: 5433
        query: 'select count(*) from test'
    register: {documents: '{{ OUTPUT }}'}
    
# insert into test, using string configuration
postgres:
    request:
        conf: 'dbname=test user=test host=localhost password=test port=5433'
        query: 'insert into test(id, num) values(3, 3);'
'''


class Postgres(ExternalStep):
    def action(self, request: dict) -> any:
        in_data = request['request']
        conf = in_data['conf']
        query = in_data['query']
        if isinstance(conf, str):
            conn = psycopg2.connect(conf)
        else:
            conn = psycopg2.connect(**conf)
        cur = conn.cursor()
        try:
            cur.execute(query)
            response = Postgres.gather_response(cur)
            conn.commit()
            return response
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def gather_response(cursor):
        try:
            response = cursor.fetchall()
            if len(response) == 1:  # for only one value select * from .. where id = 1 -> [('a', 1, 2)]
                response = response[0]
                if len(response) == 1:  # for only one value select count(*) from ... -> (2,)
                    response = response[0]
            return response
        except ProgrammingError:
            return None
