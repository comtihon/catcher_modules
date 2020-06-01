from os.path import join

from catcher.core.runner import Runner
from catcher.utils.file_utils import ensure_empty

import test
from test.abs_test_class import TestClass
from test.test_utils import check_file


class SeleniumTest(TestClass):
    def __init__(self, method_name):
        super().__init__('selenium', method_name)

    def setUp(self):
        super().setUp()
        ensure_empty(join(test.get_test_dir(self.test_name), 'steps'))

    def test_python_selenium_with_output(self):
        self.populate_file('steps/test.py', '''from selenium import webdriver
from selenium.webdriver.common.keys import Keys

driver = webdriver.Firefox()
driver.get("http://www.python.org")
assert "Python" in driver.title
elem = driver.find_element_by_name("q")
elem.clear()
elem.send_keys("pycon")
elem.send_keys(Keys.RETURN)
assert "No results found." not in driver.page_source
driver.close()
print('{"variable":"value"}')

        ''')
        self.populate_file('main.yaml', '''---
            steps:
                - selenium:
                    test:
                        driver: '/home/val/geckodriver'
                        file: test/tmp/selenium/steps/test.py
                    register: {variable: '{{ OUTPUT.variable }}'}
                - echo: {from: '{{ variable }}', to: variable.output}
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        self.assertTrue(check_file(join(self.test_dir, 'variable.output'), 'value'))

    def test_access_variables(self):
        self.populate_file('steps/test.py', '''from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import os

driver = webdriver.Firefox()
driver.get(os.environ['site_url'])
assert "Python" in driver.title
elem = driver.find_element_by_name("q")
elem.clear()
elem.send_keys("pycon")
elem.send_keys(Keys.RETURN)
assert "No results found." not in driver.page_source
driver.close()
print('{"variable":"value"}')

                ''')
        self.populate_file('main.yaml', '''---
                    variables:
                        site_url: 'http://www.python.org'
                    steps:
                        - selenium:
                            test:
                                driver: '/home/val/geckodriver'
                                file: test/tmp/selenium/steps/test.py
                    ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

    def test_str_output(self):
        self.populate_file('steps/test.py', '''from selenium import webdriver
from selenium.webdriver.common.keys import Keys

driver = webdriver.Firefox()
driver.get("http://www.python.org")
assert "Python" in driver.title
elem = driver.find_element_by_name("q")
elem.clear()
elem.send_keys("pycon")
elem.send_keys(Keys.RETURN)
assert "No results found." not in driver.page_source
driver.close()
print('some plain text output')
print('and here')

        ''')
        self.populate_file('main.yaml', '''---
            steps:
                - selenium:
                    test:
                        driver: '/home/val/geckodriver'
                        file: test/tmp/selenium/steps/test.py
                    register: {variable: '{{ OUTPUT }}'}
                - echo: {from: '{{ variable }}', to: variable.output}
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        self.assertTrue(check_file(join(self.test_dir, 'variable.output'), 'some plain text output\nand here\n'))
