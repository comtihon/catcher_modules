#!/usr/bin/python
from catcher.steps.external_step import ExternalStep
from couchbase.cluster import Cluster, PasswordAuthenticator


class Couchbase(ExternalStep):
    """
    :Input:

    :conf:  couchbase configuration. Is an object.. **Required**.

    - bucket: bucket to work with
    - user: database user (optional)
    - host: database host (optional)
    - password: user's password
    :put: put value in the database by the key.
    :get: get object by key.
    :delete: delete object by key.
    :query: query to run.

    :Examples:

    Put value by key
    ::
        couchbase:
            request:
                conf:
                    bucket: test
                    host: localhost
                put:
                    key: my_key
                    value: {foo: bar, baz: [1,2,3,4]}

    Get value by key
    ::
        couchbase:
            request:
                conf:
                    bucket: test
                    user: test
                    password: test
                    host: localhost
                get:
                    key: my_key

    Delete value by key
    ::
        couchbase:
            request:
                conf:
                    bucket: test
                    user: test
                    password: test
                    host: localhost
                delete:
                    key: my_key

    Query by foo
    ::
        couchbase:
            request:
                conf:
                    bucket: test
                    user: test
                    password: test
                    host: localhost
                query: "select `baz` from test where `foo` = 'bar'"

    """

    def action(self, request: dict) -> any:
        in_data = request['request']
        conf = in_data['conf']
        cluster = Cluster('couchbase://' + conf['host'])
        if 'user' in conf and 'password' in conf:
            cluster.authenticate(PasswordAuthenticator(conf['user'], conf['password']))
        bucket = cluster.open_bucket(conf['bucket'])
        if 'put' in in_data:
            put = in_data['put']
            result = bucket.upsert(put['key'], put['value'])
            if result.success:
                return {}
            else:
                raise RuntimeError(result.errstr)
        if 'get' in in_data:
            get = in_data['get']
            result = bucket.get(get['key'], quiet=True)
            if result is None:
                raise RuntimeError('no data found for key ' + get['key'])
            return result.value
        if 'delete' in in_data:
            delete = in_data['delete']
            bucket.remove(delete['key'], quiet=True)
            return {}
        if 'query' in in_data:
            query = in_data['query']
            res = [row for row in bucket.n1ql_query(query)]
            if len(res) == 1:
                return res[0]
            return res
        raise RuntimeError('nothing to do: ' + str(in_data))
