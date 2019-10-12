from catcher.steps.external_step import ExternalStep
from catcher.steps.step import Step, update_variables
from catcher.utils.misc import try_get_object
import ssl
import logging

logging.getLogger("pika").setLevel(logging.WARN)


class Rabbit(ExternalStep):
    """
    :Input:

    :config: rabbitmq config object, used in other rabbitmq commands.

    - server: is the rabbit host, <rabbit-host:rabbit-port>
    - username: is the username
    - password: is the password
    - virtualhost: virtualhost *Optional* defaults to "/"
    - sslOptions: {'ssl_version': 'PROTOCOL_TLSv1, PROTOCOL_TLSv1_1 or PROTOCOL_TLSv1_2', 'ca_certs': '/path/to/ca_cert', 'keyfile': '/path/to/key', 'certfile': '/path/to/cert'. 'cert_reqs': 'CERT_NONE, CERT_OPTIONAL or CERT_REQUIRED'} 
                  Optional object to be used only when ssl is required. 
                  If an empty object is passed ssl_version defaults to PROTOCOL_TLSv1_2 and cert_reqs defaults to CERT_NONE

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
                url: 127.0.0.1:5672
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
                url: 127.0.0.1:5672
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
                url: 127.0.0.1:5672
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

    @update_variables
    def action(self, includes: dict, variables: dict) -> any:
        body = self.simple_input(variables)
        method = Step.filter_predefined_keys(body)  # publish/consume

        operation = body[method]

        # if virtual host is not specified default it to /
        config = operation['config']
        if config.get('virtualhost') is None:
            config['virtualhost'] = ''

        connection_parameters = self._get_connection_parameters(config)

        if method == 'publish':
            message = self._get_data(operation)
            return variables, self.publish(connection_parameters, operation['exchange'], operation['routing_key'],
                                           operation.get('headers'), message)
        elif method == 'consume':
            return variables, self.consume(connection_parameters, operation['queue'])
        else:
            raise AttributeError('unknown method: ' + method)

    @staticmethod
    def publish(connection_parameters, exchange, routing_key, headers, message):
        import pika
        properties = pika.BasicProperties(headers=headers)
        with pika.BlockingConnection(connection_parameters) as connection:
            channel = connection.channel()
            channel.basic_publish(exchange=exchange, routing_key=routing_key, properties=properties, body=message)

    @staticmethod
    def consume(connection_parameters, queue):
        message = None
        import pika
        with pika.BlockingConnection(connection_parameters) as connection:
            channel = connection.channel()
            method_frame, header_frame, body = channel.basic_get(queue)
            if method_frame:
                channel.basic_ack(method_frame.delivery_tag)
                message = try_get_object(body.decode('UTF-8'))
        return message

    @staticmethod
    def _get_data(operation):
        if operation.get('data') is not None:
            return str(operation.get('data'))
        elif operation.get('data_from_file') is not None:
            with open(operation['data_from_file'], 'r') as f:
                return f.read()
        raise AttributeError('data or data_from_file should be passed: ' + operation)

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
