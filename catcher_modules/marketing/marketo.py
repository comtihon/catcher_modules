from catcher.steps.external_step import ExternalStep
from catcher.steps.step import Step, update_variables
from catcher.utils.logger import debug


class Marketo(ExternalStep):
    """
    Allows you to read/write/delete leads in Adobe `Marketo <https://www.marketo.com>`_ marketing automation tool.

    :Config:

    - munchkin_id: mailserver's host
    - client_id: mailserver's host. *Optional*. Default is 993.
    - client_secret: your username

    :read: read lead record from Marketo

    - conf: config object
    - fields: fields to retrieve
    - filter_key: field to use for filtering. *Optional*. Default is email.
    - filter_value: list or single value used in filtering

    :write: write lead record to Marketo

    - conf: config object
    - action: 'createOnly', 'updateOnly', 'createOrUpdate', 'createDuplicate'
    - lookupField: field to use as a key, for updating. *Optional*. Default is email.
    - leads: list of dicts to write to Marketo

    :delete: delete lead from Marketo

    - conf: config object
    - by: field name by which lead will be deleted
    - having: list or single value

    :Examples:

    Read lead by custom_id field
    ::

        marketo:
            read:
                conf:
                    munchkin_id: '{{ marketo_munchkin_id }}'
                    client_id: '{{ marketo_client_id }}'
                    client_secret: '{{ marketo_client_secret }}'
                fields: ['id', 'email', 'custom_field_1']
                filter_key: 'custom_id'
                filter_value: ['my_value_1', 'my_value_2']
            register: {leads: '{{ OUTPUT }}'}

    Update leads in Marketo by custom_id
    ::

        marketo:
            write:
                conf:
                    munchkin_id: '{{ marketo_munchkin_id }}'
                    client_id: '{{ marketo_client_id }}'
                    client_secret: '{{ marketo_client_secret }}'
                action: 'updateOnly'
                lookupField: 'custom_id'
                leads:
                    - custom_id: 14
                      email: 'foo@bar.baz'
                      custom_field_1: 'some value'
                    - custom_id: 15
                      email: 'foo2@bar.baz'
                      custom_field_1: 'some other value'

    Delete all leads with emails
    ::

        marketo:
            delete:
                conf:
                    munchkin_id: '{{ marketo_munchkin_id }}'
                    client_id: '{{ marketo_client_id }}'
                    client_secret: '{{ marketo_client_secret }}'
                by: 'custom_id'
                having: ['my_value_1', 'my_value_2']

    """

    @update_variables
    def action(self, includes: dict, variables: dict) -> dict or tuple:
        body = self.simple_input(variables)
        method = Step.filter_predefined_keys(body)  # read/write
        step = body[method]
        if method == 'read':
            return variables, self._read(step)
        elif method == 'write':
            return variables, self._write(step)
        elif method == 'delete':
            return variables, self._delete(step)
        else:
            raise AttributeError('unknown method: ' + method)

    def _read(self, step):
        config = step['conf']
        from marketorestpython.client import MarketoClient
        mc = MarketoClient(config['munchkin_id'], config['client_id'], config['client_secret'], None, None)
        filter_values = self._get_field_as_list(step['filter_value'])
        fields = self._get_field_as_list(step['fields'])
        res = mc.execute(method='get_multiple_leads_by_filter_type',
                         filterType=step.get('filter_key', 'email'),
                         filterValues=[str(v) for v in filter_values],
                         fields=fields,
                         batchSize=None)
        debug('Found {}'.format(res))
        return res

    def _write(self, step):
        config = step['conf']
        from marketorestpython.client import MarketoClient
        mc = MarketoClient(config['munchkin_id'], config['client_id'], config['client_secret'], None, None)
        leads = self._get_field_as_list(step['leads'])
        res = mc.execute(method='create_update_leads',
                         leads=leads,
                         action=step['action'],
                         lookupField=step.get('lookupField', 'email'),
                         asyncProcessing='false',
                         partitionName='Default')
        debug('Write result = {}'.format(res))
        return res

    def _delete(self, step):
        config = step['conf']
        from marketorestpython.client import MarketoClient
        mc = MarketoClient(config['munchkin_id'], config['client_id'], config['client_secret'], None, None)
        having = self._get_field_as_list(step['having'])
        found = mc.execute(method='get_multiple_leads_by_filter_type',
                           filterType=step.get('by', 'email'),
                           filterValues=[str(v) for v in having],
                           fields=['id'],
                           batchSize=None)
        debug('Found {}'.format(found))
        res = mc.execute(method='delete_lead', id=[lead['id'] for lead in found])
        debug(res)
        return res

    @staticmethod
    def _get_field_as_list(field):
        if not isinstance(field, list):
            return [field]
        return field
