#!/usr/bin/python
from catcher.steps.external_step import ExternalStep
from couchbase.cluster import Cluster, PasswordAuthenticator

DOCUMENTATION = '''
---
module: couchbase
description: Couchbase Catcher module
options:
    conf:
        description:
            - couchbase configuration. Is an object.
        required: true
        values:
            bucket: bucket to work with
            user: database user (optional)
            host: database host (optional)
            password: user's password
    put:
        description:
            - put value in the database by the key
        required: false
        values:
            key: key
            value: value. Any object.
    get:
        description:
            - get object by key
        required: false
        values:
            key: key
    delete:
        description:
            - delete object by key
        required: false
        values:
            key: key
    query:
        description:
            - query to run.
        required: false
'''
EXAMPLES = '''
# put value by key
couchbase:
    request:
        conf: 
            bucket: test
            host: localhost
        put: 
            key: my_key
            value: {foo: bar, baz: [1,2,3,4]}

# get value by key
couchbase:
    request:
        conf: 
            bucket: test
            user: test
            password: test
            host: localhost
        get: 
            key: my_key

# delete value by key
couchbase:
    request:
        conf: 
            bucket: test
            user: test
            password: test
            host: localhost
        delete: 
            key: my_key
            
# query by foo
couchbase:
    request:
        conf: 
            bucket: test
            user: test
            password: test
            host: localhost
        query: "select `baz` from test where `foo` = 'bar'"
'''


class Couchbase(ExternalStep):
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
