from time import sleep

from catcher.steps.check import Operator
from catcher.steps.external_step import ExternalStep
from catcher.steps.step import Step, update_variables
from catcher.utils.file_utils import read_file
from catcher.utils.logger import debug
from catcher.utils.misc import try_get_object, fill_template_str
from catcher.utils.time_utils import to_seconds
import pika
import json


class Rabbit(ExternalStep):
    """
    :Input:

    :consume:  Consume message from rabbit.

    - server: is the rabbit host, <rabbit-host:rabbit-port>
    - username: is the username
    - password: is the password
    - queue: the name of the queue to consume from

    :publish: Publish message to rabbit exchange.

    - server: is the rabbit host, <rabbit-host:rabbit-port>
    - username: is the username
    - password: is the password
    - exchange: exchange to publish message
    - queue: queue to publish message *Optional* Either `exchange` or `queue` should present.
    - routing_key: routing key *Optional*
    - data: data to be produced.
    - data_from_file: File can be used as data source. *Optional* Either `data` or `data_from_file` should present.

    :Examples:

    Read message
    ::
        rabbit:
            consume:
                server: 'localhost:5672'
                username: 'guest'
                password: 'guest'
                queue: 'test.catcher.queue'

    Publish `data` variable as json message
    ::
        rabbit:
            publish:
                server: 'localhost:5672'
                username: 'guest'
                password: 'guest'
                exchange: 'test.catcher.exchange'
                routing_key: 'catcher.routing.key'
                data: '{{ data|tojson }}'

    """

    def __init__(self, **kwargs: dict) -> None:
        super().__init__(**kwargs)
        method = Step.filter_predefined_keys(kwargs)  # publish/consume
        self.method = method.lower()
        conf = kwargs[method]
        self.server = conf.get('server', '127.0.0.1:5672')
        self.username = conf.get('username', 'guest')
        self.password = conf.get('password', 'guest')
        self.data = None

        if self.method == 'consume':
            self.queue = conf.get('queue', None)
        elif self.method == 'publish':
            self.exchange = conf.get('exchange', None)
            self.routing_key = conf.get('routing_key', None)
            if self.exchange is None:
                self.queue = conf.get('queue', None)
                if self.queue is None:
                    raise AttributeError('Either an exchange or queue must be defined for publishing a message')

    @classmethod
    def construct_step(cls, body, *params, **kwargs):
        return cls(**body)

    @update_variables
    def action(self, includes: dict, variables: dict) -> tuple:
        amqpURL = 'amqp://{}:{}@{}/'
        parameters = pika.URLParameters(amqpURL.format(self.username, self.password, self.server))
        client = pika.BlockingConnection(parameters).channel()
        out = {}
        if self.method == 'consume':
            out = self.consume(topic, variables)
            if out is None:
                raise RuntimeError('No kafka messages were consumed')
        elif self.method == 'produce':
            self.produce(client, variables)
        else:
            raise AttributeError('unknown method: ' + self.method)
        return variables, out

    def produce(self, client, variables):
        message = self.__form_body(variables)
        client.basic_publish(exchange=self.exchange_name,
                             routing_key=self.routing_key,
                             properties=pika.BasicProperties(headers=None,body=json.dumps(message)))
        client.close()

    def __form_body(self, variables):
        data = self.data
        if data is None:
            data = read_file(fill_template_str(self.file, variables))
        return fill_template_str(data, variables)
