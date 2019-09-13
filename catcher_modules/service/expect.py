from catcher.steps.external_step import ExternalStep
from catcher_modules.utils import module_utils
import catcher_modules.database


class Expect(ExternalStep):
    """
    High level test function. Compare data with expected one.

    :Input:

    Check expected schema and data in postgres.
    ::
        steps:
            - expect:
                compare:
                    postgres:
                        url: {{ pg_conf }}
                        schema: {{ expected_schema_file }}
                        data: {{ expected_data_file }}

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
                found = module_utils.find_class_in_module(f'catcher_modules.database.{service}', service)
                found(**{service: data}).check(variables['RESOURCES_DIR'], **data)
            # TODO cache
            # TODO s3
            # TODO http mock
        return variables
