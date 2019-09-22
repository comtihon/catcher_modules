from time import sleep

from catcher.steps.external_step import ExternalStep
from catcher.steps.step import Step, update_variables


class Rabbit(ExternalStep):
    """
    :Input:

    :config: rabbitmq config object, used in other rabbitmq commands.

    - server: is the rabbit host, <rabbit-host:rabbit-port>
    - username: is the username
    - password: is the password

    :consume:  Consume message from rabbit.

    - config: rabbitmq config object
    - queue: the name of the queue to consume from

    :publish: Publish message to rabbit exchange.

    - config: rabbitmq config object
    - exchange: exchange to publish message
    - routing_key: routing key *Optional*
    - data: data to be produced.

    :Examples:

    Read message
    ::
        variables:
            rabbitmq_config:
                url: 127.0.0.1:5672
                username: 'guest'
                password: 'guest'
        rabbit:
            consume:
                config: '{{ rabbitmq_config }}''
                queue: 'test.catcher.queue'

    Publish `data` variable as json message
    ::
        variables:
            rabbitmq_config:
                url: 127.0.0.1:5672
                username: 'guest'
                password: 'guest'
        rabbit:
            publish:
                config: '{{ rabbitmq_config }}''
                exchange: 'test.catcher.exchange'
                routing_key: 'catcher.routing.key'
                data: '{{ data|tojson }}'

    """

    @update_variables
    def action(self, includes: dict, variables: dict) -> any:
        body = self.simple_input(variables)
        method = Step.filter_predefined_keys(body)  # publish/consume

        operation = body[method]
        config = operation['config']

        import pika
        amqpURL = 'amqp://{}:{}@{}/'
        parameters = pika.URLParameters(amqpURL.format(config['username'], config['password'], config['server']))
        connection = pika.BlockingConnection(parameters)
        rabbitChannel = connection.channel()

        if method == 'publish':
            properties = pika.BasicProperties(None)
            return variables, self.publish(rabbitChannel, operation['exchange'], operation['routing_key'], properties,operation['data'])
        else:
            raise AttributeError('unknown method: ' + method)
        
    def publish(self, rabbitChannel, exchange, routingKey, properties, message):
        rabbitChannel.basic_publish(exchange=exchange,
                             routing_key=routingKey,
                             properties=properties,body=message)
        rabbitChannel.close()
