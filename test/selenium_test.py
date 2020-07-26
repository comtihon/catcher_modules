from os.path import join

import pytest
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
        ensure_empty(join(test.get_test_dir(self.test_name), 'resources'))
        self.populate_file('resources/test.py', '''from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import os

driver = webdriver.Firefox()
try:
    driver.get(os.environ['site_url'])
    assert "Python" in driver.title
    elem = driver.find_element_by_name("q")
    elem.clear()
    elem.send_keys("pycon")
    elem.send_keys(Keys.RETURN)
    assert "No results found." not in driver.page_source
finally:
    driver.close()''')
        self.populate_file('resources/google_search.js', '''const {Builder, By, Key, until} = require('selenium-webdriver');
        async function basicExample(){
            let driver = await new Builder().forBrowser('firefox').build();
            try{
                await driver.get(process.env.site_url);
                await driver.findElement(By.name('q')).sendKeys('webdriver', Key.RETURN);
                await driver.wait(until.titleContains('webdriver'), 1000);
                await driver.getTitle().then(function(title) {
                            console.log('{\"title\":\"' + title + '\"}')
                    });
                driver.quit();
            }
            catch(err) {
                console.error(err);
                process.exitCode = 1;
                driver.quit();
              }
        }
        basicExample();
        ''')
        self.populate_file('resources/MySeleniumTest.java', '''package selenium;

import org.openqa.selenium.By; 
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

public class MySeleniumTest {

    public static void main(String[] args) {
        WebDriver driver = new FirefoxDriver();
        try {
            driver.get(System.getenv("site_url"));
            WebElement element = driver.findElement(By.name("q"));
            element.sendKeys("Cheese!");
            element.submit();
        } finally {
            driver.quit();
        }
    }
}
        ''')

    @pytest.mark.skip(reason="too complex to test in travis")
    def test_access_variables(self):
        self._add_output('resources/test.py', "print('{\"variable\":\"value\"}')")
        self.populate_file('main.yaml', '''---
                    variables:
                        site_url: 'http://www.python.org'
                    steps:
                        - selenium:
                            test:
                                driver: '/usr/lib/geckodriver'
                                file: test/tmp/selenium/resources/test.py
                            register: {variable: '{{ OUTPUT.variable }}'}
                        - echo: {from: '{{ variable }}', to: variable.output}
                    ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        self.assertTrue(check_file(join(self.test_dir, 'variable.output'), 'value'))

    @pytest.mark.skip(reason="too complex to test in travis")
    def test_str_output(self):
        self._add_output('resources/test.py', "print('some plain text output')", "print('and here')")
        self.populate_file('main.yaml', '''---
            variables:
                site_url: 'http://www.python.org'
            steps:
                - selenium:
                    test:
                        driver: '/usr/lib/geckodriver'
                        file: test/tmp/selenium/resources/test.py
                    register: {variable: '{{ OUTPUT }}'}
                - echo: {from: '{{ variable }}', to: variable.output}
            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        self.assertTrue(check_file(join(self.test_dir, 'variable.output'), 'some plain text output\nand here\n'))

    @pytest.mark.skip(reason="too complex to test in travis")
    def test_py_fail(self):
        self.populate_file('main.yaml', '''---
                            variables:
                                site_url: 'http://www.example.org'
                            steps:
                                - selenium:
                                    test:
                                        driver: '/usr/lib/geckodriver'
                                        file: test/tmp/selenium/resources/test.py
                            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertFalse(runner.run_tests())

    @pytest.mark.skip(reason="too complex to test in travis")
    def test_js_selenium(self):
        self.populate_file('main.yaml', '''---
                            variables:
                                site_url: 'http://www.google.com/ncr'
                            steps:
                                - selenium:
                                    test:
                                        driver: '/usr/lib/geckodriver'
                                        file: test/tmp/selenium/resources/google_search.js
                                    register: {title: '{{ OUTPUT.title }}'}
                                - echo: {from: '{{ title }}', to: variable.output}
                            ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())
        with open(join(self.test_dir, 'variable.output')) as f:
            self.assertTrue('Google' in f.read())

    @pytest.mark.skip(reason="too complex to test in travis")
    def test_js_fail(self):
        self.populate_file('main.yaml', '''---
                                    variables:
                                        site_url: 'http://www.example.com'
                                    steps:
                                        - selenium:
                                            test:
                                                driver: '/usr/lib/geckodriver'
                                                file: test/tmp/selenium/resources/google_search.js
                                    ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertFalse(runner.run_tests())

    @pytest.mark.skip(reason="too complex to test in travis")
    def test_java_selenium(self):
        self.populate_file('main.yaml', '''---
                                    variables:
                                        site_url: 'http://www.google.com/ncr'
                                    steps:
                                        - selenium:
                                            test:
                                                driver: '/usr/lib/geckodriver'
                                                file: test/tmp/selenium/resources/MySeleniumTest.java
                                                library: '/usr/share/java/*'
                                    ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertTrue(runner.run_tests())

    @pytest.mark.skip(reason="too complex to test in travis")
    def test_java_fail(self):
        self.populate_file('main.yaml', '''---
                                    variables:
                                        site_url: 'http://www.example.com'
                                    steps:
                                        - selenium:
                                            test:
                                                driver: '/usr/lib/geckodriver'
                                                file: test/tmp/selenium/resources/MySeleniumTest.java
                                                library: '/usr/share/java/*'
                                    ''')
        runner = Runner(self.test_dir, join(self.test_dir, 'main.yaml'), None)
        self.assertFalse(runner.run_tests())

    def _add_output(self, file, *output):
        with open(join(self.test_dir, file), 'a+') as w:
            w.write('\n' + '\n'.join(output))
