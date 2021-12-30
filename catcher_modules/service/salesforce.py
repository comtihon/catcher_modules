import collections

import requests
from catcher.steps.external_step import ExternalStep
from catcher.steps.step import Step, update_variables


class Salesforce(ExternalStep):
    """
    Allows you to work with `Salesforce <https://www.salesforce.com/>`_.

    :Config:

    - instance: your Salesforce instance. *Optional*. Either instance or instance_url must exist.
    - instance_url: full URL of your instance. *Optional*. Either instance or instance_url must exist.
    - username: your username *Optional*.
    - password: your password *Optional*.
    - security_token: token is usually provided when you change your password *Optional*.
    - organizationId: whitelisted Organization ID. *Optional*.
    - consumer_key: consumer key from your app for JWT auth. *Optional*.
    - privatekey_file: path to the private key file (with resources as root) for the JWT auth. *Optional*
    - client_id: used for requests tracking. *Optional*.
    - domain: domain to be used. *Optional*.
    - session: Salesforce session. *Optional*.


    :Input:

    :query: run SOQL query

    - soql: query to run
    - config: Config object

    :<action>: record's possible action: create/update/upsert/get/get_by_custom_id/delete/deleted/updated

    - <record>: <data>
    where **<record>** is SF record name.
    and **<data>** is an action param. In case of multiple params use list (see examples)

    :Examples:

    Run SOQL query
    ::

        salesforce:
            query:
                config:
                    password='password'
                    username='myemail@example.com'
                    organizationId='OrgId'
                soql: "SELECT Id, Email FROM Contact WHERE LastName = 'Jones'"
            register: {contacts: '{{ OUTPUT }}'}

    Create new record
    ::

        salesforce:
            create:
                config:
                    password='password'
                    username='myemail@example.com'
                    organizationId='OrgId'
                Contact:
                    LastName: Smith
                    Email: example@example.com

    Upsert a record. First param is the upsert id, second is the record
    ::

        salesforce:
            upsert:
                config:
                    password='password'
                    username='myemail@example.com'
                    organizationId='OrgId'
                Contact:
                    - customExtIdField__c/11999
                    - LastName: Smith
                      Email: example@example.com

    """
    valid_conf = ['instance', 'instance_url', 'username', 'password',
                  'security_token', 'organizationId', 'consumer_key', 'privatekey_file',
                  'client_id', 'domain', 'session']
    sessions = {}

    @update_variables
    def action(self, includes: dict, variables: dict) -> any:
        body = self.simple_input(variables)
        method = Step.filter_predefined_keys(body)  # query/<record_based>

        salesforce = self._do_login(body[method]['config'])

        if method == 'query':
            return variables, self._query(salesforce, body['query'])
        else:  # record action
            return variables, self.deep_convert_dict(self._record(salesforce, method, body[method]))

    def _do_login(self, config):
        conf = {key: value for (key, value) in config.items() if key in self.valid_conf}
        if 'instance' in conf and 'instance_url' in conf:
            raise Exception('instance and instance_url are mutually exclusive')
        if 'session' in conf:
            conf['session'] = self.sessions.get(conf['session'], requests.Session())

        from simple_salesforce import Salesforce
        return Salesforce(**conf)

    def _query(self, sf, query):
        import json
        return json.loads(json.dumps(sf.query_all(query['soql'])))  # json to avoid OrderedDict

    def _record(self, sf, action, params: dict):
        del params['config']
        record_name = list(params.keys())[0]
        if not hasattr(sf, record_name):
            raise AttributeError("Salesforce object doesn't have record " + record_name)
        record = getattr(sf, record_name)
        if isinstance(params[record_name], list):
            return getattr(record, action)(*params[record_name])
        else:
            return getattr(record, action)(params[record_name])

    @staticmethod
    def deep_convert_dict(layer):
        to_ret = layer
        if isinstance(layer, collections.OrderedDict):
            to_ret = dict(layer)

        try:
            for key, value in to_ret.items():
                to_ret[key] = Salesforce.deep_convert_dict(value)
        except AttributeError:
            pass

        return to_ret
