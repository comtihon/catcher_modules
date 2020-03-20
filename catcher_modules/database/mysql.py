from catcher.steps.external_step import ExternalStep
from catcher.steps.step import update_variables

from catcher_modules.database import SqlAlchemyDb


class MySql(ExternalStep, SqlAlchemyDb):
    """
    :Input:

    :conf:  mysql configuration. Can be a single line string or object. **Required**.

    - dbname: name of the database to connect to
    - user: database user
    - host: database host
    - password: user's password
    - port: database port
    :query: query to run. **Required**

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
              query: 'select count(*) as count from test'
          register: {documents: '{{ OUTPUT }}'}

    **Note** that we alias count. For some reason sqlalchemy for mysql will return `count(*)` as a column name
    instead of `count`.

    Insert into test, using string configuration
    ::
        mysql:
          request:
              conf: 'user:password@localhost:5432/test'
              query: 'insert into test(id, num) values(3, 3);'

      """

    @property
    def driver(self) -> str:
        return "mysql+pymysql"

    @update_variables
    def action(self, includes: dict, variables: dict) -> any:
        body = self.simple_input(variables)
        return variables, self.execute(body)
