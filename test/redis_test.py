from os.path import join

import redis
from catcher.core.runner import Runner
from catcher.utils.misc import try_get_object

from test.abs_test_class import TestClass


class RedisTest(TestClass):
    def __init__(self, method_name):
        super().__init__('redis', method_name)

    def test_set(self):
        self.populate_file('main.yaml', '''---
            steps:
                - redis:
                    request:
                        set:
                            - foo
                            - 11
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        r = redis.StrictRedis()
        self.assertEqual(b'11', r.get('foo'))

    def test_set_complex(self):
        self.populate_file('main.yaml', '''---
            variables:
                complex: 
                    a: 1
                    b: 'c'
                    d: [1,2,4]
            steps:
                - redis:
                    request:
                        set:
                            - key
                            - '{{ complex }}'
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        r = redis.StrictRedis()
        self.assertEqual({'a': 1, 'b': 'c', 'd': [1, 2, 4]}, try_get_object(r.get('key').decode()))

    def test_get_number(self):
        r = redis.StrictRedis()
        r.set('key', 17)
        self.populate_file('main.yaml', '''---
            steps:
                - redis:
                    request:
                        get:
                            - key
                    register: {var: '{{ OUTPUT }}'}
                - check: 
                    equals: {the: '{{ var }}', is: 17} 
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

    def test_set_get(self):
        self.populate_file('main.yaml', '''---
            variables:
                complex: 
                    a: 1
                    b: 'c'
                    d: [1,2,4]
            steps:
                - redis:
                    request:
                        set:
                            - key
                            - '{{ complex }}'
                - redis:
                    request:
                        get:
                            - key
                    register: {var: '{{ OUTPUT }}'}
                - check: 
                    equals: {the: '{{ var }}', is: '{{ complex }}'} 
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

    def test_incr_decr_delete(self):
        self.populate_file('main.yaml', '''---
            steps:
                - redis:
                    actions:
                        - request:
                                set:
                                    - foo
                                    - 11
                        - request:
                                decr:
                                    - foo
                        - request:
                                get:
                                    - foo
                          register: {var: '{{ OUTPUT }}'}
                - check: 
                    equals: {the: '{{ var }}', is: 10}
                - redis:
                    actions:
                        - request:
                                incrby:
                                    - foo
                                    - 5
                        - request:
                                get:
                                    - foo
                          register: {var: '{{ OUTPUT }}'}
                - check: 
                    equals: {the: '{{ var }}', is: 15}
                - redis:
                    request:
                        delete:
                            - foo
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        r = redis.StrictRedis()
        self.assertIsNone(r.get('foo'))
