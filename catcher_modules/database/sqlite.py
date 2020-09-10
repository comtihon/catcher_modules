from catcher.steps.external_step import ExternalStep
from catcher.steps.step import update_variables

from catcher_modules.database import SqlAlchemyDb


class SQLite(ExternalStep, SqlAlchemyDb):
    """
    Allows you to create `SQLite <https://www.sqlite.org/index.html>`_ database on your local filesystem and work with
    it.
    **Important** - for relative path use one slash `/`. For absolute slash - two `//`.

    :Input:

    :conf:  sqlite path string. Dialect is not mandatory. **Required**.
    :query: query to run. **Deprecated since 5.2**
    :sql: query or sql file from resources to run. **Required**

    :Examples:

    Select all from test, use relative path
    ::

        sqlite:
          request:
              conf: '/foo.db'
              sql: 'select count(*) as count from test'
          register: {documents: '{{ OUTPUT }}'}

    **Note** that we alias count. For some reason sqlalchemy for sqlite will return `count(*)` as a column name
    instead of `count`.

    Insert into test, using string absolute path (with 2 slashes)
    ::

        sqlite:
          request:
              conf: '//absolute/path/to/foo.db'
              sql: 'insert into test(id, num) values(3, 3);'

      """

    @property
    def dialect(self) -> str:
        return "sqlite"

    @update_variables
    def action(self, includes: dict, variables: dict) -> any:
        body = self.simple_input(variables)
        return variables, self.execute(body, variables)
