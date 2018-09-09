from os.path import join

from catcher.core.runner import Runner
from pymongo import MongoClient

from test.abs_test_class import TestClass


class MongoTest(TestClass):
    def __init__(self, method_name):
        super().__init__('postgres', method_name)

    @property
    def conf(self):
        return 'mongodb://test:test@localhost'

    def setUp(self):
        super().setUp()
        client = MongoClient(self.conf)
        db = client.get_database('test')
        collection = db.test
        collection.insert_one({'msg': 'one', 'test': True, 'list': [1, 2, 3]})

    def tearDown(self):
        super().tearDown()
        client = MongoClient(self.conf)
        db = client.get_database('test')
        collection = db.test
        collection.delete_many({})

    def test_read_simple_query(self):
        self.populate_file('test_inventory.yml', '''
        mongo:
            database: test
            username: test
            password: test
            host: localhost
            port: 27017
        ''')

        self.populate_file('main.yaml', '''---
            steps:
                - mongo:
                    request:
                        conf: '{{ mongo }}'
                        collection: 'test'
                        command: 'find_one'
                    register: {document: '{{ OUTPUT }}'}
                - check:
                    equals: {the: '{{ document["msg"] }}', is: "one"}
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), join(self.test_dir, 'test_inventory.yml'))
        self.assertTrue(runner.run_tests())

    def test_write_simple_query(self):
        self.populate_file('test_inventory.yml', '''
        mongo:
            database: test
            username: test
            password: test
            host: localhost
            port: 27017
        ''')

        self.populate_file('main.yaml', '''---
                steps:
                    - mongo:
                        request:
                            conf: '{{ mongo }}'
                            collection: 'test'
                            insert_one:
                                'author': 'Mike'
                                'text': 'My first blog post!'
                                'tags': ['mongodb', 'python', 'pymongo']
                                'date': '{{ NOW_DT }}'
                        register: {id: '{{ OUTPUT }}'}
                ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), join(self.test_dir, 'test_inventory.yml'))
        self.assertTrue(runner.run_tests())
        client = MongoClient(self.conf)
        db = client.get_database('test')
        collection = db.test
        d = collection.find_one({'author': 'Mike'})
        self.assertEqual('My first blog post!', d['text'])
        self.assertEqual(['mongodb', 'python', 'pymongo'], d['tags'])

    def test_insert_many(self):
        self.populate_file('test_inventory.yml', '''
        mongo:
            database: test
            username: test
            password: test
            host: localhost
            port: 27017
        ''')

        self.populate_file('main.yaml', '''---
                steps:
                    - mongo:
                        request:
                            conf: '{{ mongo }}'
                            collection: 'test'
                            insert_many:
                                - {'foo': 'baz'}
                                - {'foo': 'bar'}
                ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), join(self.test_dir, 'test_inventory.yml'))
        self.assertTrue(runner.run_tests())
        client = MongoClient(self.conf)
        db = client.get_database('test')
        collection = db.test
        cur = collection.find()
        docs = list(cur)  # 0 index is a document from set_up
        self.assertEqual('baz', docs[1]['foo'])
        self.assertEqual('bar', docs[2]['foo'])

    def test_read_many(self):
        self.populate_file('test_inventory.yml', '''
        mongo:
            database: test
            username: test
            password: test
            host: localhost
            port: 27017
        ''')

        self.populate_file('main.yaml', '''---
                steps:
                    - mongo:
                        actions:
                            - request:
                                conf: '{{ mongo }}'
                                collection: 'test'
                                insert_many:
                                    - {'author': 'Mike', 'text': 'post1'}
                                    - {'author': 'Bob', 'text': 'post1'}
                                    - {'author': 'Mike', 'text': 'post2'}
                            - request:
                                conf: '{{ mongo }}'
                                collection: 'test'
                                find: {'author': 'Mike'}
                              register: {posts: '{{ OUTPUT }}'}
                    - check:
                        equals: {the: '{{ posts }}', is: [{'author': 'Mike', 'text': 'post1'}, {'author': 'Mike', 'text': 'post2'}]}
        ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), join(self.test_dir, 'test_inventory.yml'))
        self.assertTrue(runner.run_tests())

    def test_find_projection(self):
        self.populate_file('test_inventory.yml', '''
        mongo:
            database: test
            username: test
            password: test
            host: localhost
            port: 27017
        ''')

        self.populate_file('main.yaml', '''---
                steps:
                    - mongo:
                        actions:
                            - request:
                                conf: '{{ mongo }}'
                                collection: 'test'
                                insert_many:
                                    - {'author': 'Mike', 'text': 'post1', 'other': 'optional'}
                                    - {'author': 'Bob', 'text': 'post1'}
                                    - {'author': 'Mike', 'text': 'post2'}
                            - request:
                                conf: '{{ mongo }}'
                                collection: 'test'
                                find:
                                    filter: {'author': 'Mike'}
                                    projection: {'other': False}
                                list_params: true
                              register: {posts: '{{ OUTPUT }}'}
                    - check:
                        equals: {the: '{{ posts }}', is: [{'author': 'Mike', 'text': 'post1'}, {'author': 'Mike', 'text': 'post2'}]}
        ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), join(self.test_dir, 'test_inventory.yml'))
        self.assertTrue(runner.run_tests())

    def test_chaining(self):
        self.populate_file('test_inventory.yml', '''
        mongo:
            database: test
            username: test
            password: test
            host: localhost
            port: 27017
        ''')

        self.populate_file('main.yaml', '''---
                steps:
                    - mongo:
                        actions:
                            - request:
                                conf: '{{ mongo }}'
                                collection: 'test'
                                insert_many:
                                    - {'author': 'Mike', 'text': 'post1'}
                                    - {'author': 'Bob', 'text': 'post1'}
                                    - {'author': 'Mike', 'text': 'post2'}
                            - request:
                                conf: '{{ mongo }}'
                                collection: 'test'
                                find: {'author': 'Mike'}
                                next: 'count'
                              register: {posts_num: '{{ OUTPUT }}'}
                    - check:
                        equals: {the: '{{ posts_num }}', is: 2}
        ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), join(self.test_dir, 'test_inventory.yml'))
        self.assertTrue(runner.run_tests())
