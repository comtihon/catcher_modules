from typing import List

from catcher.steps.external_step import ExternalStep
from catcher.steps.step import Step, update_variables


class S3(ExternalStep):
    """
    :Input:

    :config: s3 config object, used in other s3 commands.

    - key_id: access key id
    - secret_key: secret for the access key
    - region: region. *Optional*.
    - url: endpoint_url url. Can be used to run against Minio. *Optional*

    :put: put file to s3

    - config: s3 config object
    - path: path including the filename. First dir treats like a bucket.
    F.e. /my_bucket/subdir/file or my_bucket/subfir/file
    - content: file's content. *Optional*
    - content_resource: path to a file. *Optional*. Either `content` or `content_resource` must be set.

    :get: Get file from s3

    - config: s3 config object
    - path: path including the filename

    :list: List S3 directory

    - config: s3 config object
    - path: path to the directory being listed

    :Examples:

    Put data into s3
    ::
        s3:
            put:
                config: '{{ s3_config }}'
                path: /foo/bar/file.csv
                content: '{{ my_data }}'

    Get data from s3
    ::

        s3:
            get:
                config: '{{ s3_config }}'
                path: /foo/bar/file.csv
            register: {csv: '{{ OUTPUT }}'

    List files
    ::

        s3:
            list:
                config: '{{ s3_config }}'
                path: /foo/bar/
            register: {files: '{{ OUTPUT }}'

    """

    @update_variables
    def action(self, includes: dict, variables: dict) -> any:
        body = self.simple_input(variables)
        method = Step.filter_predefined_keys(body)  # get/put/list

        oper = body[method]
        conf = oper['config']

        import boto3
        s3_client = boto3.client('s3',
                                 endpoint_url=conf.get('url'),
                                 aws_access_key_id=conf['key_id'],
                                 aws_secret_access_key=conf['secret_key'],
                                 region_name=conf.get('region')
                                 )

        if method == 'get':
            return variables, self._get_file(s3_client, oper['path'])
        elif method == 'put':
            content = str(oper.get('content'))
            if not content:
                if 'content_resource' not in oper:
                    raise ValueError('No content for s3 put')
                with open(oper['content_resource'], 'r') as f:
                    content = f.read()
            return variables, self._put_file(s3_client, oper['path'], content)
        elif method == 'list':
            return variables, self._list_dir(conf, oper['path'])
        else:
            raise AttributeError('unknown method: ' + method)

    def _get_file(self, s3_client, path):
        bucket, filename = self._parse_path(path)
        response = s3_client.get_object(Bucket=bucket, Key=filename)
        # TODO check response
        return response['Body'].read().decode()

    def _put_file(self, s3_client, path, content, retry=True):
        from botocore.exceptions import ClientError
        bucket, filename = self._parse_path(path)
        try:
            res = s3_client.put_object(Bucket=bucket, Key=filename, Body=content)
            return self._check_response(res)
        except ClientError as e:
            if retry and hasattr(e, 'response') and 'Error' in e.response and 'Code' in e.response['Error']:
                if e.response['Error']['Code'] == 'NoSuchBucket':
                    res = s3_client.create_bucket(Bucket=bucket)
                    self._check_response(res)
                    return self._put_file(s3_client, path, content, False)
            raise e

    def _list_dir(self, conf: dict, path: str) -> List[str]:
        import boto3
        res = boto3.resource('s3',
                             endpoint_url=conf.get('url'),
                             aws_access_key_id=conf['key_id'],
                             aws_secret_access_key=conf['secret_key'],
                             region_name=conf.get('region')
                             )

        bucket, rest = self._parse_path(path)
        bucket = res.Bucket(bucket)
        data = []
        for obj in bucket.objects.all():
            if obj.key.startswith(rest):
                data += [obj.key]
        return data

    @staticmethod
    def _check_response(res):
        if 'ResponseMetadata' in res and 'HTTPStatusCode' in res['ResponseMetadata'] \
                and res['ResponseMetadata']['HTTPStatusCode'] == 200:
            return True
        raise Exception("Operation failed")

    @staticmethod
    def _parse_path(path: str):
        splitted = [s for s in path.split('/') if s != '']
        return splitted[0], '/'.join(splitted[1:])
