import json
import os
import subprocess
from json import JSONDecodeError

from catcher.steps.external_step import ExternalStep
from catcher.steps.step import Step, update_variables
from catcher.utils.logger import debug, warning


class Selenium(ExternalStep):
    """
    Run selenium test

    :test:

    - driver: path to the driver executable. *Optional*. If not specified - will try to use PATH variable.
    - file: path to your file with the test

    :Examples:

    Read lead by custom_id field
    ::

        selenium:
            test:
                driver: '/opt/bin/geckodriver'
                file: 'my_test.py'

    """

    @update_variables
    def action(self, includes: dict, variables: dict) -> dict or tuple:
        body = self.simple_input(variables)
        # TODO determine the webdriver and check it is installed
        # TODO set variables as env var
        method = Step.filter_predefined_keys(body)
        step = body[method]
        driver = step.get('driver')
        file = step['file']

        my_env = os.environ.copy()  # TODO populate with variables
        if driver is not None:
            my_env['PATH'] = my_env['PATH'] + ':' + os.path.dirname(driver)
        for k, v in variables.items():
            my_env[k] = v
        p = subprocess.Popen(self._form_cmd(file), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=my_env)
        if p.wait() == 0:
            out = p.stdout.read().decode()
            debug(out)
            return variables, self._parse_output(out)
        else:
            out = p.stdout.read().decode()
            warning(out)
            raise Exception('Execution failed.')

    @staticmethod
    def _form_cmd(test_file: str):
        if test_file.endswith('.py'):  # python executable
            return ['python', test_file]
        return None

    @staticmethod
    def _parse_output(output: str):
        try:
            return json.loads(output)
        except JSONDecodeError:
            return output
