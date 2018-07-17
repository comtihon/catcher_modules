import redis
from catcher.steps.external_step import ExternalStep


class Redis(ExternalStep):
    """
    :Input:

    :conf:  redis configuration. Is an object.
    - host: redis host. Default is localhost
    - port: redis port. Default is 6379
    - db: redis database number. Default is 0

    :<command>: - command to run.

    :Examples:

    Set value (default configuration)
    ::
        redis:
            request:
                set:
                    - 'key'
                    - 'value'

    """

    def action(self, request: dict) -> any:
        in_data = request['request']
        conf = in_data.get('conf', {})
        r = redis.StrictRedis(host=conf.get('host', 'localhost'),
                              port=conf.get('port', 6379),
                              db=conf.get('db', 0))
        [command] = [k for k in in_data.keys() if k != 'conf']
        args = in_data.get(command, [])
        result = getattr(r, command.lower())(*args)
        if isinstance(result, bytes):
            return result.decode()
        return result
