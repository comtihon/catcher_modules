from time import sleep

from catcher.steps.check import Operator
from catcher.steps.external_step import ExternalStep
from catcher.steps.step import Step, update_variables
from catcher.utils.logger import debug
from catcher.utils.misc import try_get_object, fill_template_str
from catcher.utils.time_utils import to_seconds
from catcher_modules.mq import MqStepMixin


class Kafka(ExternalStep, MqStepMixin):
    """
    Allows you to consume/produce messages from/to Apache `Kafka <https://kafka.apache.org/>`_

    :Input:

    :consume:  Consume message from kafka.

    - server: is the kafka host. Can be multiple, comma-separated.
    - group_id: is the consumer group id. If not specified - `catcher` will be used. *Optional*
    - topic: the name of the topic
    - timeout: is the consumer timeout. *Optional* (default is 1 sec)
    - where: search for specific message clause. *Optional*

    :produce: Produce message to kafka.

    - server: is the kafka host. Can be multiple, comma-separated.
    - topic: the name of the topic
    - data: data to be produced.
    - data_from_file: File can be used as data source. *Optional* Either `data` or `data_from_file` should present.

    :Examples:

    Read message with timestamp > 1000
    ::

        kafka:
            consume:
                server: '127.0.0.1:9092'
                group_id: 'test'
                topic: 'test_consume_with_timestamp'
                timeout: {seconds: 5}
                where:
                    equals: '{{ MESSAGE.timestamp > 1000 }}'

    Produce `data` variable as json message
    ::

        kafka:
            produce:
                server: '127.0.0.1:9092'
                topic: 'test_produce_json'
                data: '{{ data|tojson }}'

    """

    def __init__(self, **kwargs: dict) -> None:
        super().__init__(**kwargs)
        method = Step.filter_predefined_keys(kwargs)  # produce/consume
        self.method = method.lower()
        conf = kwargs[method]
        self.group_id = conf.get('group_id', 'catcher')
        self.server = conf.get('server', '127.0.0.1:9092')
        self.topic = conf['topic']
        timeout = conf.get('timeout', {'seconds': 1})
        self.timeout = to_seconds(timeout)
        self.where = conf.get('where', None)
        self.message = None
        if self.method != 'consume':
            self.message = conf.get('data', None)
            self.file = None
            if self.message is None:
                self.file = conf['data_from_file']

    @update_variables
    def action(self, includes: dict, variables: dict) -> tuple:
        from pykafka import KafkaClient
        client = KafkaClient(hosts=fill_template_str(self.server, variables))
        topic = client.topics[fill_template_str(self.topic, variables).encode('utf-8')]
        out = {}
        if self.method == 'consume':
            out = self.consume(topic, variables)
            if out is None:
                raise RuntimeError('No kafka messages were consumed')
        elif self.method == 'produce':
            self.produce(topic, variables)
        else:
            raise AttributeError('unknown method: ' + self.method)
        return variables, out

    def consume(self, topic, variables: dict) -> dict:
        from pykafka.common import OffsetType
        consumer = topic.get_simple_consumer(consumer_group=self.group_id.encode('utf-8'),
                                             auto_offset_reset=OffsetType.EARLIEST,
                                             reset_offset_on_start=False,
                                             consumer_timeout_ms=self.timeout * 1000)
        if self.where is not None:
            operator = Operator.find_operator(self.where)
        else:
            operator = None
        return Kafka.get_messages(consumer, operator, variables, self.timeout)

    def produce(self, topic, variables):
        message = self.form_body(self.message, self.file, variables)
        with topic.get_sync_producer() as producer:
            producer.produce(message.encode('utf-8'))

    @staticmethod
    def get_messages(consumer, where: Operator or None, variables, timeout) -> dict or None:
        try:
            while True:
                consumer.fetch()
                for message in consumer:
                    value = try_get_object(message.value.decode('utf-8'))
                    debug(value)
                    if Kafka.check_message(where, value, variables):
                        return value
                if timeout > 0:
                    sleep(1)
                    timeout -= 1
                else:
                    return None
        finally:
            consumer.commit_offsets()

    @staticmethod
    def check_message(where: Operator, message: str, variables: dict) -> bool:
        if where is None:
            return True
        variables = dict(variables)
        variables['MESSAGE'] = message
        return where.operation(variables)
