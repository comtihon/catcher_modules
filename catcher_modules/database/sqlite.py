from catcher.steps.external_step import ExternalStep
from catcher.steps.step import update_variables

from catcher_modules.database import SqlAlchemyDb


class SQLite(ExternalStep, SqlAlchemyDb):
    """
    :Input:

    :conf:  sqlite path string. **Required**.
    :query: query to run. **Required**

    :Examples:

    Select all from test, use relative path
    ::
        sqlite:
          request:
              conf: '/foo.db'
              query: 'select count(*) as count from test'
          register: {documents: '{{ OUTPUT }}'}

    **Note** that we alias count. For some reason sqlalchemy for sqlite will return `count(*)` as a column name
    instead of `count`.

    Insert into test, using string absolute path (with 2 slashes)
        mssql:
          request:
              conf: '//absolute/path/to/foo.db'
              query: 'insert into test(id, num) values(3, 3);'

      """

    @property
    def driver(self) -> str:
        return "sqlite"

    @update_variables
    def action(self, includes: dict, variables: dict) -> any:
        body = self.simple_input(variables)
        return variables, self.execute(body)
