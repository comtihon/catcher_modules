from catcher.steps.external_step import ExternalStep
from catcher_modules.utils import module_utils
import catcher_modules.database


class Expect(ExternalStep):
    """
    This is the opposite for prepare. It compares expected data from csv to what you have in the database.
    csv file supports templates.

    **Important**:

    * populate step is designed to be supported by all steps (in future). Currently it is supported only
      by Postges/Oracle/MSSql/MySql/SQLite steps.
    * Schema comparison is not implemented.
    * You can use strict comparison (only data from csv should be in the table, in the same order as csv)
      or the default one (just check if the data is there)

    :Input:

    :compare: Compare the existing data with expected one.

    - <service_name>: See each own step's documentation for the parameters description and
                      information. Note, that not all steps are compatible with prepare step.

    Check expected schema and data in postgres.
    ::

        steps:
            - expect:
                compare:
                    postgres:
                        url: {{ pg_conf }}
                        schema: {{ expected_schema_file }}
                        data: {{ expected_data_file }}

    Check data in s3 and redshift.
    ::

        steps:
            - expect:
                compare:
                    s3:
                        url: {{ s3_url }}
                        path: {{ expected_path }}
                        csv:
                            header: true
                            headers: {{ expected_headers }}
                    redshift:
                        url: {{ redshift_url }}
                        schema: {{ expected_schema }}
                        data: {{ expected_data }}

    """

    def action(self, includes: dict, variables: dict) -> dict or tuple:
        input_data = self.simple_input(variables)
        db_modules = module_utils.list_modules_in_package(catcher_modules.database)
        for service, data in input_data['compare'].items():
            if service in db_modules:  # database
                found = module_utils.find_class_in_module('catcher_modules.database.' + service, service)
                found(**{service: data}).expect(variables, **data)
            # TODO mongodb
            # TODO mq
            # TODO cache
            # TODO s3
            # TODO http mock
        return variables
