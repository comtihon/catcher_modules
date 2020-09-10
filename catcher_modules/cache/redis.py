from catcher.steps.external_step import ExternalStep
from catcher.steps.step import update_variables


class Redis(ExternalStep):
    """
    Work with Redis cache.
    Put value to cache or get it, increment/decrement or delete.

    :Input:

    :conf:  redis configuration. Is an object.

    - host: redis host. Default is localhost
    - port: redis port. Default is 6379
    - db: redis database number. Default is 0

    :<command>: - command to run. Every command can have a list of arguments.

    Refer to `Redis <https://redis.io/commands>`_ and `Redis-Py <https://redis-py.readthedocs.io/en/latest/>`_

    :Examples:

    Set value (default configuration)
    ::

        variables:
                complex:
                    a: 1
                    b: 'c'
                    d: [1,2,4]

        redis:
            request:
                set:
                    key: '{{ complex }}'

    Get value by key 'key' and register in variable 'var'
    ::

        redis:
            request:
                get: 'key'
            register: {var: '{{ OUTPUT }}'}

    Decrement, increment by 5 and delete
    ::

        redis:
            actions:
                - request:
                        set:
                            'foo': 11
                - request:
                        decr: foo
                - request:
                        incrby:
                            foo: 5
                - request:
                        delete:
                            - foo

    """

    @update_variables
    def action(self, includes: dict, variables: dict) -> any:
        import redis
        body = self.simple_input(variables)
        in_data = body['request']
        conf = in_data.get('conf', {})
        r = redis.StrictRedis(host=conf.get('host', 'localhost'),
                              port=conf.get('port', 6379),
                              db=conf.get('db', 0),
                              max_connections=1)
        [command] = [k for k in in_data.keys() if k != 'conf']
        value = in_data.get(command, [])
        if isinstance(value, dict):  # set: {key: value}
            flatlist = [str(item) for k in value for item in (k, value[k])]  # convert to str to avoid data errors
            result = getattr(r, command.lower())(*flatlist)
        elif isinstance(value, str):  # get: key
            result = getattr(r, command.lower())(value)
        else:  # ???
            result = getattr(r, command.lower())(*value)
        if isinstance(result, bytes):
            return variables, result.decode()
        return variables, result
