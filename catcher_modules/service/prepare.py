from catcher.steps.external_step import ExternalStep

import catcher_modules.database
from catcher.utils import misc
from catcher_modules.utils import module_utils


class Prepare(ExternalStep):
    """
    Used for bulk actions to prepare test data. Is useful when you need to prepare a lot of data.
    This step consists of 3 parts:

    1. write sql ddl schema file (optional) - describe all tables/schemas/privileges needed to be created
    2. prepare data in a csv file (optional)
    3. call Catcher's prepare step to populate csv content into the database

    Both sql schema and csv file supports templates.

    **Important**:

    * populate step is designed to be supported by all steps (in future). Currently it is supported only by
      Postges/Oracle/MSSql/MySql/SQLite steps.
    * to populate json as Postgres Json data type you need to use **use_json: true** flag

    :Input:

    :populate: Populate existing service with predefined data.

    - <service_name>: See each own step's documentation for the parameters description and
                      information. Note, that not all steps are compatible with prepare step.
    - variables: Variables, which will override state (only for this prepare step).

    Please, keep it mind, that resources directory is used for all data and schema files.

    Populate existing postgres with data from `pg_data_file`.
    ::

        steps:
            - prepare:
                populate:
                    postgres:
                        conf: {{ pg_conf }}
                        schema: {{ pg_schema_file }}
                        data: {{ pg_data_file }}

    Multiple populates and can be run at the same time. This will populate existing s3 with data, start local
    salesforce and postgres in docker and populates them as well.
    ::

        steps:
            - prepare:
                populate:
                    s3:
                        conf: {{ s3_url }}
                        path: {{ s3_path }}
                        data: {{ s3_data }}
                    postgres:
                        conf: {{ pg_conf }}
                        schema: {{ pg_schema_file }}
                        data: {{ pg_data_file }}

    Prepare step with variables override.
    ::

        - prepare:
             populate:
               postgres:
                    conf: '{{ postgres_conf }}'
                    schema: create_personal_data_customer.sql
               variables:
                    email: '{{ random("email") }}'

    """
    def action(self, includes: dict, variables: dict) -> dict or tuple:
        input_data = self.simple_input(variables)
        variables_override = misc.merge_two_dicts(variables, input_data['populate'].get('variables'))
        db_modules = module_utils.list_modules_in_package(catcher_modules.database)
        for service, data in input_data['populate'].items():
            if service == 'variables':
                continue
            if service in db_modules:  # database
                found = module_utils.find_class_in_module('catcher_modules.database.' + service, service)
                found(**{service: data}).populate(variables_override, **data)
            # TODO cache
            # TODO s3
            # TODO http mock
        return variables
