from abc import abstractmethod, ABCMeta
from catcher.steps.external_step import ExternalStep
from catcher.steps.step import update_variables


class DockerCmd(metaclass=ABCMeta):
    @abstractmethod
    def action(self, network):
        pass


class IdBasedCmd:
    def __init__(self, **kwargs: dict) -> None:
        self._id = kwargs.get('name', kwargs.get('hash'))
        if self._id is None:
            raise ValueError('no id for container')

    def get_container(self):
        import docker
        client = docker.from_env()
        return client.containers.get(self._id)


class StopCmd(IdBasedCmd, DockerCmd):
    def action(self, network):
        return self.get_container().stop()


class StatusCmd(IdBasedCmd, DockerCmd):
    def action(self, network):
        return self.get_container().status


class DisconnectCmd(IdBasedCmd, DockerCmd):
    def action(self, network):
        import docker
        client = docker.from_env()
        container = self.get_container()
        if not container or container.status == 'exited':
            raise ValueError('Container exited; Can\'t disconnect.')
        return client.networks.get(network).disconnect(container)


class ConnectCmd(IdBasedCmd, DockerCmd):
    def action(self, network: str):
        import docker
        client = docker.from_env()
        container = self.get_container()
        if not container or container.status == 'exited':
            raise ValueError('Container exited; can\'t connect.')
        return client.networks.get(network).connect(container)


class LogsCmd(IdBasedCmd, DockerCmd):
    def action(self, network):
        return self.get_container().logs().decode()


class StartCmd(DockerCmd):
    def __init__(self, image: str, **kwargs: dict) -> None:
        super().__init__()
        self._image = image
        self._name = kwargs.get('name')
        self._cmd = kwargs.get('cmd')
        self._detached = kwargs.get('detached', True)
        self._ports = kwargs.get('ports')
        self._env = kwargs.get('environment', None)

    def action(self, network):
        import docker
        client = docker.from_env()
        output = client.containers.run(self._image,
                                       self._cmd,
                                       name=self._name,
                                       detach=self._detached,
                                       network=network,
                                       ports=self._ports,
                                       environment=self._env)
        if not self._detached:
            return output.decode()
        else:
            return output.id


class ExecCmd(IdBasedCmd, DockerCmd):
    def __init__(self, cmd: str, **kwargs: dict) -> None:
        super().__init__(**kwargs)
        self._cmd = cmd
        self._dir = kwargs.get('dir')
        self._user = kwargs.get('user', 'root')
        self._env = kwargs.get('environment', None)

    def action(self, network):
        res = self.get_container().exec_run(cmd=self._cmd,
                                            workdir=self._dir,
                                            user=self._user,
                                            environment=self._env)
        if res.exit_code != 0:
            raise res.output.decode()
        return res.output.decode()


class CmdFactory:
    @staticmethod
    def get_cmd(command: dict) -> DockerCmd:
        if 'start' in command:
            return StartCmd(**command['start'])
        if 'exec' in command:
            return ExecCmd(**command['exec'])
        if 'stop' in command:
            return StopCmd(**command['stop'])
        if 'status' in command:
            return StatusCmd(**command['status'])
        if 'connect' in command:
            return ConnectCmd(**command['connect'])
        if 'disconnect' in command:
            return DisconnectCmd(**command['disconnect'])
        if 'logs' in command:
            return LogsCmd(**command['logs'])
        raise ValueError('Unknown command: ' + str(command))


class Docker(ExternalStep):
    """
    check logs

    :Input:

    :start: run container. Return hash.

    - image: container's image.
    - name: container's name. *Optional*
    - cmd: command to run in the container. *Optional*
    - detached: should it be run detached? *Optional* (default is True)
    - ports: dictionary of ports to bind. Keys - container ports, values - host ports.
    - environment: a dictionary of environment variables

    :stop: stop a container.

    - name: container's name. *Optional*
    - hash: container's hash. *Optional* Either name or hash should present

    :status: get the container status.

    - name: container's name. *Optional*
    - hash: container's hash. *Optional* Either name or hash should present

    :disconnect: disconnect a container from a network (network failure simulation)

    - name: container's name. *Optional*
    - hash: container's hash. *Optional* Either name or hash should present

    :connect: connect a container to a network. All containers share the same network per test.

    - name: container's name. *Optional*
    - hash: container's hash. *Optional* Either name or hash should present

    :exec: execute a command inside a running container.

    - name: container's name. *Optional*
    - hash: container's hash. *Optional* Either name or hash should present
    - cmd: command to execute.
    - dir: directory, where this command will be executed. *Optional*
    - user: user to execute this command. *Optional* (default is root)
    - environment: a dictionary of environment variables

    :logs: get container's logs.

    - name: container's name. *Optional*
    - hash: container's hash. *Optional* Either name or hash should present

    :Examples:

    Run blocking command in a new container and check the output.
    ::
        steps:
            - docker:
                start:
                    image: 'alpine'
                    cmd: 'echo hello world'
                    detached: false
                register: {echo: '{{ OUTPUT.strip() }}'}
            - check:
                equals: {the: '{{ echo }}', is: 'hello world'}

    """

    @update_variables
    def action(self, includes: dict, variables: dict) -> dict or tuple:
        network = self._ensure_network(variables['TEST_NAME'])
        cmd = CmdFactory.get_cmd(self.simple_input(variables))
        out = cmd.action(network)
        return variables, out

    @staticmethod
    def _ensure_network(name: str):
        import docker
        client = docker.from_env()
        filtered = client.networks.list(names=[name])
        if not filtered:
            return client.networks.create(name, driver="bridge").id
        return filtered[0].id
