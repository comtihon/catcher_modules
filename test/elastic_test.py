from os.path import join

import pytest
from catcher.core.runner import Runner
from elasticsearch import Elasticsearch
from requests import request

from test.abs_test_class import TestClass


class ElasticTest(TestClass):
    def __init__(self, method_name):
        super().__init__('elastic', method_name)
        self.es = Elasticsearch()

    def setUp(self):
        super().setUp()
        request('PUT', 'http://localhost:9200/test')

    def tearDown(self):
        super().tearDown()
        request('DELETE', 'http://localhost:9200/test')

    @pytest.mark.skip(reason="elastic docker stopped working in travis")
    def test_search_simple(self):
        res = self.es.create('test', id=1, body={'name': 'test_document_1', 'payload': 'one two three'})
        assert res['result'] == 'created'
        self.es.indices.refresh(index="test")
        self.populate_file('main.yaml', '''---
            steps:
                - elastic: 
                    search: 
                        url: 'http://127.0.0.1:9200'
                        index: test
                        query: {match_all: {}}
                    register: {doc: '{{ OUTPUT }}'}
                - check:
                    equals: {the: '{{ doc }}', is: [{'name': 'test_document_1', 'payload': 'one two three'}]}
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

    @pytest.mark.skip(reason="elastic docker stopped working in travis")
    def test_search_advanced(self):
        res = self.es.create('test', id=1, body={'name': 'test_document_1', 'payload': 'one two three'})
        assert res['result'] == 'created'
        res = self.es.create('test', id=2, body={'name': 'test_document_2', 'payload': 'three four five'})
        assert res['result'] == 'created'
        res = self.es.create('test', id=3, body={'name': 'test_document_3', 'payload': 'five six seven'})
        assert res['result'] == 'created'

        self.es.indices.refresh(index="test")
        self.populate_file('main.yaml', '''---
            steps:
                - elastic: 
                    search: 
                        url: 'http://127.0.0.1:9200'
                        index: test
                        query:         
                            match: {payload : "three"}
                        _source: ['name']
                    register: {doc: '{{ OUTPUT }}'}
                - check:
                    equals: {the: '{{ doc }}', is: [{'name': 'test_document_1'}, {'name': 'test_document_2'}]}
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

    @pytest.mark.skip(reason="elastic docker stopped working in travis")
    def test_bool_filter(self):
        res = self.es.create('test', id=1, body={'shape': 'round', 'color': 'red'})
        assert res['result'] == 'created'
        res = self.es.create('test', id=2, body={'shape': 'square', 'color': 'red'})
        assert res['result'] == 'created'
        res = self.es.create('test', id=3, body={'shape': 'round', 'color': 'white'})
        assert res['result'] == 'created'

        self.es.indices.refresh(index="test")
        self.populate_file('main.yaml', '''---
            steps:
                - elastic: 
                    search: 
                        url: 'http://127.0.0.1:9200'
                        index: test
                        query:         
                            bool: 
                                must: 
                                    - term: {shape: "round"}
                                    - bool:
                                        should:
                                            - term: {color: "red"}
                                            - term: {color": "blue"}
                    register: {doc: '{{ OUTPUT }}'}
                - check:
                    equals: {the: '{{ doc }}', is: [{'shape': 'round', 'color': 'red'}]}
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
