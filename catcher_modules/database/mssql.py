from catcher.steps.external_step import ExternalStep
from catcher.steps.step import update_variables
from catcher_modules.utils import db_utils
from catcher_modules.database import SqlAlchemyDb


class MSSql(ExternalStep, SqlAlchemyDb):
    """
    Allows you to run queries on Microsoft SQL `Server <https://www.microsoft.com/en-us/sql-server/>`_.

    :Input:

    :conf:  mssql configuration. Can be a single line string or object. Dialect is not mandatory. **Required**.

    - dbname: name of the database to connect to
    - user: database user
    - host: database host
    - password: user's password
    - port: database port
    - driver: odbc driver name you've installed. *Optional* If not specified, the default driver, which comes with
              catcher-modules Dockerfile will be used.

    :query: query to run. **Deprecated since 5.2**
    :sql: query or sql file from resources to run. **Required**

    :Examples:

    Select all from test, use object configuration
    ::

        mssql:
          request:
              conf:
                  dbname: test
                  user: user
                  password: password
                  host: localhost
                  port: 1433
                  driver: ODBC Driver 17 for SQL Server
              sql: 'select count(*) as count from test'
          register: {documents: '{{ OUTPUT }}'}

    **Note** that we alias count. For some reason sqlalchemy for mssql will return `count(*)` as a column name
    instead of `count`.

    Insert into test, using string configuration
    ::

        mssql:
          request:
              conf: 'user:password@localhost:5432/test'
              sql: 'insert into test(id, num) values(3, 3);'

    Insert into test, using string configuration with pymssql (pymssql should be installed)
    ::

        mssql:
          request:
              conf: 'mssql+pymssql://user:password@localhost:5432/test'
              sql: 'insert into test(id, num) values(3, 3);'

      """

    @property
    def dialect(self) -> str:
        return "mssql+pyodbc"

    def get_engine(self, conf):
        if isinstance(conf, dict):
            driver = 'ODBC Driver 17 for SQL Server' if 'driver' not in conf else conf['driver']
        else:
            driver = None
        return db_utils.get_engine(conf, self.dialect, driver)

    @update_variables
    def action(self, includes: dict, variables: dict) -> any:
        body = self.simple_input(variables)
        return variables, self.execute(body, variables)
