from catcher.steps.external_step import ExternalStep
from catcher.steps.step import update_variables


class Mongo(ExternalStep):
    """
    Allows you to interact with `MongoDB <https://www.mongodb.com/>`_ NoSQL database.
    :Input:

    :conf:  mongodb configuration. Can be a single line

    `string <https://docs.mongodb.com/manual/reference/connection-string/>`_ url or kv object. **Required**.

    - database: name of the database to connect to
    - username: database user. Must be RFC 2396 encoded when in URI.
    - host: database host
    - password: user's password. Must be RFC 2396 encoded when in URI.
    - port: database port
    - authSource: The database to authenticate on. Default is database.

    See `pymongo <http://api.mongodb.com/python/current/api/pymongo/mongo_client.html>`_ for more options.
    :collection: collection to use. **Required**

    :command: String. Use this if you have to run command without any parameters.
              Where command's value is your command to run, like `command: find_one`.  `Optional`
    :<command>: Object. Use this when you have command with parameters.
                Where <command> key is your command name and it's value is parameter object (list or dict). `Optional`
                Either <command> or command should exist.
    :next: Run other operation just after your operation. Can be string like `next: count`
           or object with params `next: {'sort': 'author'}. You can chain multiple next (see example). `Optional`
    :list_params: Pass command params as different arguments. Useful when pymongo command takes several arguments
                  (both `*args` and `**kwargs`). `*args` will be set in case of params in list while `**kwargs` will be sent in
                  case of dict. See examples for more info.

    :Examples:

    Find one document. Use **command** key when no params.
    ::

        mongo:
          request:
              conf:
                  database: test
                  username: test
                  password: test
                  host: localhost
                  port: 27017
              collection: 'your_collection'
              command: 'find_one'
          register: {document: '{{ OUTPUT }}'}

    Insert into test, using string configuration
    ::

        mongo:
          request:
              conf: 'mongodb://username:password@host'
              collection: 'your_collection'
              insert_one:
                'author': 'Mike'
                'text': 'My first blog post!'
                'tags': ['mongodb', 'python', 'pymongo']
                'date': '{{ NOW_DT }}'

    Find specific document
    ::

        mongo:
          request:
              conf:
                  database: test
                  username: test
                  password: test
                  host: localhost
                  port: 27017
              collection: 'your_collection'
              find_one: {'author': 'Mike'}
          register: {document: '{{ OUTPUT }}'}

    To find multiple documents just use **find** instead of **find_one**.

    Bulk insert
    ::

        mongo:
          request:
              conf: '{{ mongo_conf }}'
              collection: 'your_collection'
              insert_many:
                - {'foo': 'baz'}
                - {'foo': 'bar'}

    Chaining operations: db.collection.find().sort().count()
    ::

        mongo:
          request:
              conf:
                  database: test
                  username: test
                  password: test
                  host: localhost
                  port: 27017
              collection: 'your_collection'
              find: {'author': 'Mike'}
              next:
                sort: 'author'
                next: 'count'
          register: {document: '{{ OUTPUT }}'}

    Will run every next operation on previous one. You can chain more than one operation.

    Run operation with list parameters (`**kwargs`). Is useful when calling commands with additional arguments.
    ::

        mongo:
          request:
              conf:
                  database: test
                  username: test
                  password: test
                  host: localhost
                  port: 27017
              collection: 'your_collection'
              find:
                filter: {'author': 'Mike'}
                projection: {'_id': False}
              list_params: true  # pass list arguments as separate params
          register: {document: '{{ OUTPUT }}'}

    Run operation with list parameters (`*args`). Run map-reduce.
    ::

        mongo:
          request:
              conf:
                  database: test
                  username: test
                  password: test
                  host: localhost
                  port: 27017
              collection: 'your_collection'
              map_reduce:
                - 'function () {
                        this.tags.forEach(function(z) {
                            emit(z, 1);
                        });
                   }'
                - 'function (key, values) {
                     var total = 0;
                      for (var i = 0; i < values.length; i++) {
                        total += values[i];
                      }
                      return total;
                    }'
                - 'myresults'
              list_params: true  # pass list arguments as separate params
          register: {document: '{{ OUTPUT }}'}

    """

    @update_variables
    def action(self, includes: dict, variables: dict) -> any:
        from pymongo import MongoClient
        body = self.simple_input(variables)
        in_data = body['request']
        conf = in_data['conf']
        collection = in_data['collection']
        if isinstance(conf, str):  # url
            client = MongoClient(conf)
            database = client.get_database('test')
        else:
            database = conf.pop('database', 'test')
            client = MongoClient(**conf)
            database = client.get_database(database)
            if database is None and conf['database'] is not None:
                database = client.get_database(conf['database'])
        action = Action(in_data)
        result = action(database[collection])
        return variables, result


class Action:
    def __init__(self, in_data: dict or str) -> None:
        super().__init__()
        if isinstance(in_data, str):  # next: count
            self.action = in_data
        else:
            if 'command' in in_data:  # action with no params
                self.action = in_data['command']
            else:  # action with params
                [action] = [k for k in in_data.keys()
                            if k != 'conf' and k != 'next' and k != 'collection' and k != 'list_params']
                self.action = action
                self.params = in_data[action]
            if 'next' in in_data:
                self.next = Action(in_data['next'])
            self.list_params = in_data.get('list_params', False)

    def __call__(self, collection):
        from pymongo.cursor import Cursor
        if hasattr(self, 'params'):
            if self.list_params and isinstance(self.params, list):
                res = getattr(collection, self.action)(*self.params)
            elif self.list_params and isinstance(self.params, dict):
                res = getattr(collection, self.action)(**self.params)
            else:
                res = getattr(collection, self.action)(self.params)
        else:
            res = getattr(collection, self.action)()
        if hasattr(self, 'next'):
            return self.next(res)
        if isinstance(res, Cursor):
            return drop_ids(list(res))
        try:
            if '_id' in res:
                res.pop('_id')
                return res
        except TypeError:
            pass
        if hasattr(res, 'modified_count'):
            return res.modified_count
        if hasattr(res, 'deleted_count'):
            return res.deleted_count
        if hasattr(res, 'inserted_id'):
            return res.inserted_id
        if hasattr(res, 'inserted_ids'):
            return res.inserted_ids
        return res


def drop_ids(results):
    for res in results:
        res.pop('_id')
    return results
