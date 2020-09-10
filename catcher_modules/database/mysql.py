from catcher.steps.external_step import ExternalStep
from catcher.steps.step import update_variables

from catcher_modules.database import SqlAlchemyDb


class MySql(ExternalStep, SqlAlchemyDb):
    """
    Allows you to run queries on `MySQL <https://www.mysql.com/>`_
    (and all mysql compatible databases like `MariaDB <https://mariadb.org/>`_).

    :Input:

    :conf:  mysql configuration. Can be a single line string or object. Dialect is not mandatory. **Required**.

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

        mysql:
          request:
              conf:
                  dbname: test
                  user: user
                  password: password
                  host: localhost
                  port: 3306
              sql: 'select count(*) as count from test'
          register: {documents: '{{ OUTPUT }}'}

    **Note** that we alias count. For some reason sqlalchemy for mysql will return `count(*)` as a column name
    instead of `count`.

    Insert into test, using string configuration
    ::

        mysql:
          request:
              conf: 'user:password@localhost:3306/test'
              sql: 'insert into test(id, num) values(3, 3);'

    Insert into test, using string configuration with dialect
    ::

        mysql:
          request:
              conf: 'mysql+pymysql://user:password@localhost:3306/test'
              sql: 'insert into test(id, num) values(3, 3);'

      """

    @property
    def dialect(self) -> str:
        return "mysql+pymysql"

    @update_variables
    def action(self, includes: dict, variables: dict) -> any:
        body = self.simple_input(variables)
        return variables, self.execute(body, variables)
