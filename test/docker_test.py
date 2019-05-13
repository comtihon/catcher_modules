from os.path import join

from test.abs_test_class import TestClass

from catcher.core.runner import Runner


class DockerTest(TestClass):
    def __init__(self, method_name):
        super().__init__('docker', method_name)

    def test_create_container(self):
        self.populate_file('main.yaml', '''---
                    steps:
                        - docker: 
                            start: 
                                image: 'alpine'
                                cmd: 'echo hello world'
                                detached: false
                            register: {echo: '{{ OUTPUT.strip() }}'}
                        - check:
                            equals: {the: '{{ echo }}', is: 'hello world'}
                    ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

    def test_stop_container(self):
        self.populate_file('main.yaml', '''---
                            steps:
                                - docker: 
                                    start: 
                                        image: 'jamesdbloom/mockserver'
                                    register: {id: '{{ OUTPUT.strip() }}'}
                                - docker:
                                    stop:
                                        hash: '{{ id }}'
                                - docker:
                                    status:
                                        hash: '{{ id }}'
                                    register: {status: '{{ OUTPUT }}'}
                                - check:
                                    equals: {the: '{{ status }}', is: 'exited'}
                            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

    def test_start_detached(self):
        self.populate_file('main.yaml', '''---
                            steps:
                                - docker: 
                                    start: 
                                        image: 'jamesdbloom/mockserver'
                                        ports:
                                            '1080/tcp': 8000
                                    register: {hash: '{{ OUTPUT }}'}
                                - docker:
                                    status:
                                        hash: '{{ hash }}'
                                    register: {status: '{{ OUTPUT }}'}
                                - wait:
                                    seconds: 5
                                    for:
                                        - http:
                                            put:
                                                url: 'http://localhost:8000/mockserver/expectation'
                                                body:
                                                    httpRequest: {'path': '/some/path'}
                                                    httpResponse: {'body': 'hello world'}
                                                response_code: 201
                                - http:
                                    get:
                                        url: 'http://localhost:8000/some/path'
                                        response_code: 200
                                    register: {reply: '{{ OUTPUT }}'}
                                - check:
                                    equals: {the: '{{ reply }}', is: 'hello world'}
                                - docker:
                                    stop:
                                        hash: '{{ hash }}'            
                            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

    def test_get_logs(self):
        self.populate_file('main.yaml', '''---
                            steps:
                                - docker: 
                                    start: 
                                        image: 'alpine'
                                        cmd: 'echo hello world'
                                    register: {id: '{{ OUTPUT }}'}
                                - docker:
                                    logs:
                                        hash: '{{ id }}'
                                    register: {out: '{{ OUTPUT.strip() }}'}
                                - check:
                                    equals: {the: '{{ out }}', is: 'hello world'}
                            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

    def test_exec(self):
        self.populate_file('main.yaml', '''---
                                    steps:
                                        - docker: 
                                            start: 
                                                image: 'postgres:alpine'
                                                environment:
                                                    POSTGRES_PASSWORD: test
                                                    POSTGRES_USER: user
                                                    POSTGRES_DB: test
                                            register: {hash: '{{ OUTPUT }}'}
                                        - wait: {'seconds': 2}
                                        - docker:
                                            exec:
                                                hash: '{{ hash }}'
                                                cmd: >
                                                    psql -U user -d test -c \
                                                    "CREATE TABLE test(rno integer, name character varying)"
                                            register: {create_result: '{{ OUTPUT.strip() }}'}
                                        - check:
                                            equals: {the: '{{ create_result }}', is: 'CREATE TABLE'}
                                        - docker:
                                            stop:
                                                hash: '{{ hash }}'            
                                    ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

    def test_connect_disconnect(self):
        self.populate_file('main.yaml', '''---
                            steps:
                                - docker: 
                                    start: 
                                        image: 'jamesdbloom/mockserver'
                                        ports:
                                            '1080/tcp': 8000
                                    register: {hash: '{{ OUTPUT }}'}
                                - wait:
                                    seconds: 5
                                    for:
                                        - http:
                                            put:
                                                url: 'http://localhost:8000/mockserver/expectation'
                                                body:
                                                    httpRequest: {'path': '/some/path'}
                                                    httpResponse: {'body': 'hello world'}
                                                response_code: 201
                                - http:
                                    get:
                                        url: 'http://localhost:8000/some/path'
                                        response_code: 200
                                - docker:
                                    disconnect:
                                        hash: '{{ hash }}'
                                - http:
                                    get:
                                        url: 'http://localhost:8000/some/path'
                                        should_fail: true
                                - docker:
                                    connect:
                                        hash: '{{ hash }}'
                                - http:
                                    get:
                                        url: 'http://localhost:8000/some/path'
                                        response_code: 200
                                - docker:
                                    stop:
                                        hash: '{{ hash }}'            
                            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
