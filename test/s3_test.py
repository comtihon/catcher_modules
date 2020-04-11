from os.path import join

import boto3
from catcher.core.runner import Runner
from catcher.utils.file_utils import ensure_empty

import test
from test.abs_test_class import TestClass


class S3Test(TestClass):
    def __init__(self, method_name):
        super().__init__('s3', method_name)
        self.s3 = boto3.client('s3',
                               endpoint_url='http://127.0.0.1:9001',  # see tearDown before using your own url
                               aws_access_key_id='minio',
                               aws_secret_access_key='minio123'
                               )
        self.s3r = boto3.resource('s3',
                                  endpoint_url='http://127.0.0.1:9001',  # see tearDown before using your own url
                                  aws_access_key_id='minio',
                                  aws_secret_access_key='minio123'
                                  )

    def setUp(self):
        super().setUp()
        ensure_empty(join(test.get_test_dir(self.test_name), 'resources'))

    def tearDown(self):
        super().tearDown()
        response = self.s3.list_buckets()
        for bucket in response['Buckets']:
            bucket = self.s3r.Bucket(bucket['Name'])
            bucket.objects.all().delete()
            bucket.delete()

    def test_put_root(self):
        self.populate_file('main.yaml', '''---
            variables:
                s3_config:
                    url: http://127.0.0.1:9001
                    key_id: minio
                    secret_key: minio123
            steps:
                - s3:
                    put:
                        config: '{{ s3_config }}'
                        path: /foo/file.txt
                        content: 1234
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        response = self.s3.get_object(Bucket='foo', Key='file.txt')
        self.assertEqual('1234', response['Body'].read().decode())

    def test_put_template(self):
        self.populate_file('main.yaml', '''---
                    variables:
                        s3_config:
                            url: http://127.0.0.1:9001
                            key_id: minio
                            secret_key: minio123
                        content: 1234
                    steps:
                        - s3:
                            put:
                                config: '{{ s3_config }}'
                                path: /foo/file.txt
                                content: '{{ content }}'
                    ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        response = self.s3.get_object(Bucket='foo', Key='file.txt')
        self.assertEqual('1234', response['Body'].read().decode())

    def test_put_resource(self):
        self.populate_file('resources/my_file', "{{ content }}")

        self.populate_file('main.yaml', '''---
                            variables:
                                s3_config:
                                    url: http://127.0.0.1:9001
                                    key_id: minio
                                    secret_key: minio123
                                content: 1234
                            steps:
                                - s3:
                                    put:
                                        config: '{{ s3_config }}'
                                        path: /foo/file.txt
                                        content_resource: 'my_file'
                            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        response = self.s3.get_object(Bucket='foo', Key='file.txt')
        self.assertEqual('1234', response['Body'].read().decode())

    def test_put_subdirs(self):
        self.populate_file('main.yaml', '''---
            variables:
                s3_config:
                    url: http://127.0.0.1:9001
                    key_id: minio
                    secret_key: minio123
            steps:
                - s3:
                    put:
                        config: '{{ s3_config }}'
                        path: /foo/baz/bar/file.txt
                        content: 1234
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        response = self.s3.get_object(Bucket='foo', Key='baz/bar/file.txt')
        self.assertEqual('1234', response['Body'].read().decode())

    def test_get_existing(self):
        self.s3.create_bucket(Bucket='foo')
        self.s3.put_object(Bucket='foo', Key='baz/bar/file.txt', Body='1234')
        self.populate_file('main.yaml', '''---
            variables:
                s3_config:
                    url: http://127.0.0.1:9001
                    key_id: minio
                    secret_key: minio123
            steps:
                - s3:
                    get:
                        config: '{{ s3_config }}'
                        path: /foo/baz/bar/file.txt
                    register: {data: '{{ OUTPUT }}'}
                - check:
                    equals: {the: '{{ data }}', is: 1234}
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

    def test_get_non_existing(self):
        self.populate_file('main.yaml', '''---
            variables:
                s3_config:
                    url: http://127.0.0.1:9001
                    key_id: minio
                    secret_key: minio123
            steps:
                - s3:
                    get:
                        config: '{{ s3_config }}'
                        path: /foo/baz/bar/file.txt
                    register: {data: '{{ OUTPUT }}'}
                - check:
                    equals: {the: '{{ data }}', is: 1234}
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertFalse(runner.run_tests())
        self.s3.create_bucket(Bucket='foo')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertFalse(runner.run_tests())

    def test_list_dir(self):
        self.s3.create_bucket(Bucket='foo')
        self.s3.put_object(Bucket='foo', Key='baz/bar/file.txt', Body='1234')
        self.s3.put_object(Bucket='foo', Key='file1.txt', Body='1234')
        self.s3.put_object(Bucket='foo', Key='file2.txt', Body='1234')
        self.populate_file('main.yaml', '''---
            variables:
                s3_config:
                    url: http://127.0.0.1:9001
                    key_id: minio
                    secret_key: minio123
            steps:
                - s3:
                    list:
                        config: '{{ s3_config }}'
                        path: /foo
                    register: {data: '{{ OUTPUT }}'}
                - check:
                    equals: {the: '{{ data }}', is: ['baz/bar/file.txt','file1.txt', 'file2.txt']}
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

    def test_list_subdirs(self):
        self.s3.create_bucket(Bucket='foo')
        self.s3.put_object(Bucket='foo', Key='baz/bar/file.txt', Body='1234')
        self.s3.put_object(Bucket='foo', Key='baz/file1.txt', Body='1234')
        self.s3.put_object(Bucket='foo', Key='file2.txt', Body='1234')
        self.populate_file('main.yaml', '''---
            variables:
                s3_config:
                    url: http://127.0.0.1:9001
                    key_id: minio
                    secret_key: minio123
            steps:
                - s3:
                    list:
                        config: '{{ s3_config }}'
                        path: /foo/baz
                    register: {data: '{{ OUTPUT }}'}
                - check:
                    equals: {the: '{{ data }}', is: ['baz/bar/file.txt','baz/file1.txt']}
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

    def test_delete_file(self):
        self.s3.create_bucket(Bucket='foo')
        self.s3.put_object(Bucket='foo', Key='baz/bar/file.txt', Body='1234')
        self.populate_file('main.yaml', '''---
                    variables:
                        s3_config:
                            url: http://127.0.0.1:9001
                            key_id: minio
                            secret_key: minio123
                    steps:
                        - s3:
                            delete:
                                config: '{{ s3_config }}'
                                path: /foo/baz/bar/file.txt
                    ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        try:
            self.s3.get_object(Bucket='foo', Key='baz/bar/file.txt')
            self.fail('Key must not exist')
        except Exception as e:
            self.assertTrue('NoSuchKey' in str(e))

    def test_delete_empty_dir(self):
        self.s3.create_bucket(Bucket='foo')
        self.s3.put_object(Bucket='foo', Key='baz/bar/dir/', Body='')
        self.populate_file('main.yaml', '''---
                            variables:
                                s3_config:
                                    url: http://127.0.0.1:9001
                                    key_id: minio
                                    secret_key: minio123
                            steps:
                                - s3:
                                    delete:
                                        config: '{{ s3_config }}'
                                        path: /foo/baz/bar/dir/
                            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        try:
            self.s3.get_object(Bucket='foo', Key='baz/bar/dir/')
            self.fail('Key must not exist')
        except Exception as e:
            self.assertTrue('NoSuchKey' in str(e))

    def test_delete_dir_recursive(self):
        self.s3.create_bucket(Bucket='foo')
        self.s3.put_object(Bucket='foo', Key='baz/bar/dir/file1.txt', Body='test')
        self.s3.put_object(Bucket='foo', Key='baz/bar/dir/dir2/file2.txt', Body='test2')
        self.populate_file('main.yaml', '''---
                                    variables:
                                        s3_config:
                                            url: http://127.0.0.1:9001
                                            key_id: minio
                                            secret_key: minio123
                                    steps:
                                        - s3:
                                            delete:
                                                config: '{{ s3_config }}'
                                                path: /foo/baz/bar/dir/
                                    ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        try:
            self.s3.get_object(Bucket='foo', Key='baz/bar/dir/')
            self.fail('Key must not exist')
        except Exception as e:
            self.assertTrue('NoSuchKey' in str(e))
