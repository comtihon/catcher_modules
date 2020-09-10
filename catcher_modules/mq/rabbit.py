from catcher.steps.external_step import ExternalStep
from catcher.steps.step import Step, update_variables
from catcher.utils.logger import warning
from catcher.utils.misc import try_get_object, fill_template_str, try_get_objects, fill_template
import ssl

from catcher_modules.mq import MqStepMixin


class Rabbit(ExternalStep, MqStepMixin):
    """
    Allows you to consume/produce messages from/to `RabbitMQ <https://www.rabbitmq.com/>`_

    :Input:

    :config: rabbitmq config object, used in other rabbitmq commands.

    - server: is the rabbit host, <rabbit-host:rabbit-port>
    - username: is the username
    - password: is the password
    - virtualhost: virtualhost *Optional* defaults to "/"
    - sslOptions: {'ssl_version': 'PROTOCOL_TLSv1, PROTOCOL_TLSv1_1 or PROTOCOL_TLSv1_2', 'ca_certs': '/path/to/ca_cert', 'keyfile': '/path/to/key', 'certfile': '/path/to/cert'. 'cert_reqs': 'CERT_NONE, CERT_OPTIONAL or CERT_REQUIRED'} 
                  Optional object to be used only when ssl is required. 
                  If an empty object is passed ssl_version defaults to PROTOCOL_TLSv1_2 and cert_reqs defaults to CERT_NONE
    - disconnect_timeout: number of seconds to wait for a disconnect before force closing the connection. Warning! Publish
                          may fail if you use to small timeout value.

    :consume:  Consume message from rabbit.

    - config: rabbitmq config object
    - queue: the name of the queue to consume from

    :publish: Publish message to rabbit exchange.

    - config: rabbitmq config object
    - exchange: exchange to publish message
    - routing_key: routing key
    - headers: headers json *Optional*
    - data: data to be produced
    - data_from_file: data to be published. File can be used as data source. *Optional* Either `data` or `data_from_file` should present.

    :Examples:

    Read message
    ::

        variables:
            rabbitmq_config:
                server: 127.0.0.1:5672
                username: 'guest'
                password: 'guest'
        steps:
            - rabbit:
                consume:
                    config: '{{ rabbitmq_config }}''
                    queue: 'test.catcher.queue'

    Publish `data` variable as message
    ::

        variables:
            rabbitmq_config:
                server: 127.0.0.1:5672
                sslOptions: {'ssl_version': 'PROTOCOL_TLSv1, PROTOCOL_TLSv1_1 or PROTOCOL_TLSv1_2', 'ca_certs': '/path/to/ca_cert', 'keyfile': '/path/to/key', 'certfile': '/path/to/cert'. 'cert_reqs': 'CERT_NONE, CERT_OPTIONAL or CERT_REQUIRED'}
                username: 'guest'
                password: 'guest'
        steps:
            - rabbit:
                publish:
                    config: '{{ rabbitmq_config }}''
                    exchange: 'test.catcher.exchange'
                    routing_key: 'catcher.routing.key'
                    headers: {'test.header.1': 'header1', 'test.header.2': 'header1'}
                    data: '{{ data|tojson }}'

    Publish `data_from_file` variable as json message
    ::

        variables:
            rabbitmq_config:
                server: 127.0.0.1:5672
                username: 'guest'
                password: 'guest'
        steps:
            - rabbit:
                publish:
                    config: '{{ rabbitmq_config }}''
                    exchange: 'test.catcher.exchange'
                    routing_key: 'catcher.routing.key'
                    data_from_file: '{{ /path/to/file }}'
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        method = Step.filter_predefined_keys(kwargs)  # publish/consume
        self.method = method.lower()
        conf = kwargs[method]
        self.config = conf['config']
        self.headers = conf.get('headers', {})
        self.message = None
        if self.method != 'consume':
            self.exchange = conf['exchange']
            self.routing_key = conf['routing_key']
            self.message = conf.get('data', None)
            self.file = None
            if self.message is None:
                self.file = conf['data_from_file']
        else:
            self.queue = conf['queue']

    @update_variables
    def action(self, includes: dict, variables: dict) -> any:
        # if virtual host is not specified default it to /
        config = try_get_objects(fill_template_str(self.config, variables))
        if config.get('virtualhost') is None:
            config['virtualhost'] = ''
        disconnect_timeout = int(config.get('disconnect_timeout', 10))  # 10 sec for connection closed exception
        connection_parameters = self._get_connection_parameters(config)

        if self.method == 'publish':
            message = self.form_body(self.message, self.file, variables)
            return variables, self.publish(connection_parameters,
                                           fill_template_str(self.exchange, variables),
                                           fill_template_str(self.routing_key, variables),
                                           fill_template(self.headers, variables),
                                           message,
                                           disconnect_timeout)
        elif self.method == 'consume':
            return variables, self.consume(connection_parameters,
                                           fill_template_str(self.queue, variables),
                                           disconnect_timeout)
        else:
            raise AttributeError('unknown method: ' + self.method)

    @staticmethod
    def publish(connection_parameters, exchange, routing_key, headers, message, disconnect_timeout):
        import pika
        from pika import exceptions
        properties = pika.BasicProperties(headers=headers)
        try:
            connection_parameters.blocked_connection_timeout = disconnect_timeout
            with pika.BlockingConnection(connection_parameters) as connection:
                channel = connection.channel()
                channel.basic_publish(exchange=exchange, routing_key=routing_key, properties=properties, body=message)
        except exceptions.ConnectionClosed:
            warning('Failed to gracefully close rabbit connection.')

    @staticmethod
    def consume(connection_parameters, queue, disconnect_timeout):
        message = None
        import pika
        connection_parameters.blocked_connection_timeout = disconnect_timeout
        with pika.BlockingConnection(connection_parameters) as connection:
            channel = connection.channel()
            method_frame, header_frame, body = channel.basic_get(queue)
            if method_frame:
                channel.basic_ack(method_frame.delivery_tag)
                message = try_get_object(body.decode('UTF-8'))
        return message

    def _get_connection_parameters(self, config):
        import pika
        amqpURL = 'amqp{}://{}:{}@{}/{}'
        sslOptions = config.get('sslOptions')
        parameters = pika.URLParameters(
            amqpURL.format('s' if sslOptions else '', config['username'], config['password'], config['server'],
                           config['virtualhost']))
        if sslOptions is not None:
            parameters.ssl = True
            parameters.ssl_options = self._get_ssl_options(sslOptions)
        return parameters

    @staticmethod
    def _get_ssl_options(ssl_options):
        # PROTOCOL_TLSv1, PROTOCOL_TLSv1_1 or PROTOCOL_TLSv1_2
        sslVersion = {
            'PROTOCOL_TLSv1': ssl.PROTOCOL_TLSv1,
            'PROTOCOL_TLSv1_1': ssl.PROTOCOL_TLSv1_1,
            'PROTOCOL_TLSv1_2': ssl.PROTOCOL_TLSv1_2
        }
        # CERT_NONE, CERT_OPTIONAL or CERT_REQUIRED
        certReqs = {
            'CERT_NONE': ssl.CERT_NONE,
            'CERT_OPTIONAL': ssl.CERT_OPTIONAL,
            'CERT_REQUIRED': ssl.CERT_REQUIRED,
        }

        return {
            'ssl_version': sslVersion.get(ssl_options.get('ssl_version'), ssl.PROTOCOL_TLSv1_2),
            'ca_certs': ssl_options.get('ca_certs'),
            'keyfile': ssl_options.get('keyfile'),
            'certfile': ssl_options.get('certfile'),
            'cert_reqs': certReqs.get(ssl_options.get('cert_reqs'), 'CERT_NONE')
        }
