from catcher.steps.external_step import ExternalStep
from catcher.steps.step import update_variables

from catcher_modules.database import SqlAlchemyDb


class Postgres(ExternalStep, SqlAlchemyDb):
    """
    Allows you to run queries in `Postgres <https://www.postgresql.org/>`_

    :Input:

    :conf:  postgres configuration. Can be a single line string or object. Dialect is not mandatory. **Required**.

    - dbname: name of the database to connect to
    - user: database user
    - host: database host
    - password: user's password
    - port: database port

    :query: query to run. **Deprecated since 5.2**
    :sql: query or sql file from resources to run. **Required**

    :Examples:

    Select all from test, use object configuration
    ::

        postgres:
          request:
              conf:
                  dbname: test
                  user: user
                  password: password
                  host: localhost
                  port: 5433
              sql: 'select count(*) from test'
          register: {documents: '{{ OUTPUT }}'}

    Run all commands from **resources/my_ddl.sql**, using string configuration
    ::

        postgres:
          request:
              conf: 'user:password@localhost:5432/test'
              sql: 'my_ddl.sql'

    Insert into test, using string configuration with dialect
    ::

        postgres:
          request:
              conf: 'postgresql://user:password@localhost:5432/test'
              sql: 'insert into test(id, num) values(3, 3);'


      """

    @property
    def dialect(self) -> str:
        return "postgresql"

    @update_variables
    def action(self, includes: dict, variables: dict) -> any:
        body = self.simple_input(variables)
        return variables, self.execute(body, variables)
