from catcher.steps.external_step import ExternalStep

import catcher_modules.database
from catcher_modules.utils import module_utils


class Prepare(ExternalStep):
    """
    High level test function. Populate or mock specific service with data.

    :Input:

    :populate: Populate existing service with predefined data.

    - <service_name>: See each own step's documentation for the parameters description and
                      information. Note, that not all steps are compatible with prepare step.
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

    """
    def action(self, includes: dict, variables: dict) -> dict or tuple:
        input_data = self.simple_input(variables)
        db_modules = module_utils.list_modules_in_package(catcher_modules.database)
        for service, data in input_data['populate'].items():
            if service in db_modules:  # database
                found = module_utils.find_class_in_module('catcher_modules.database.' + service, service)
                found(**{service: data}).populate(variables, **data)
            # TODO cache
            # TODO s3
            # TODO http mock
        return variables
