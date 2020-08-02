import os

from catcher.steps.external_step import ExternalStep
from catcher.steps.step import Step, update_variables
from catcher.utils import external_utils


class Selenium(ExternalStep):
    """
    Run selenium test

    :test:

    - driver: path to the driver executable. *Optional*. If not specified - will try to use PATH variable.
    - file: path to your file with the test
    - libraries: path to selenium client libraries. *Optional*. Used for sources compilation (f.e. .java -> .class).
      Default is /usr/lib/java/*

    :Examples:

    Run selenium python
    ::

        - selenium:
            test:
                driver: '/opt/bin/geckodriver'
                file: 'my_test.py'

    Compile and run java selenium (MySeleniumTest.java should be in resource dir, selenium cliend libraries should be
    in /usr/share/java/)
    ::

        - selenium:
            test:
                driver: '/usr/lib/geckodriver'
                file: MySeleniumTest.java
                libraries: '/usr/share/java/*'

    Python, JavaScript and Jar archives with selenium tests can be stored in any directory, while Java and Kotlin source
    files must be stored in resources only, as they need to be compiled first.

    """

    @update_variables
    def action(self, includes: dict, variables: dict) -> dict or tuple:
        body = self.simple_input(variables)
        # TODO determine the webdriver and check it is installed
        method = Step.filter_predefined_keys(body)
        step = body[method]
        driver = step.get('driver')
        file = step['file']
        library = step.get('library', '/usr/share/java/*')
        if library is not None and not isinstance(library, list):
            library = [library]
        my_env = os.environ.copy()
        if driver is not None:
            my_env['PATH'] = my_env['PATH'] + ':' + os.path.dirname(driver)
        return variables, external_utils.run_cmd_simple(file, variables, env=my_env, libraries=library)
