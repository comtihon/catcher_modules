Front End testing with Selenium
===============================

The test
--------
You can run `Selenium`_ steps to involve your front-end applications in the end-to-end test. Selenium step consists of
two parts - selenium test and Catcher's :meth:`catcher_modules.frontend.selenium` step definition which will trigger it.

.. _selenium: https://www.selenium.dev/

Selenium step can look like::

    - selenium:
        test:
            file: resources/MySeleniumTest.java
            libraries: '/usr/share/java/*'

| **file** argument should point to your Selenium test. Catcher currently support JavaScript, Python, Java, Kotlin and
  Jar-files (in case you already prepared tests and don't need to compile them every time you run the step).
| This example will run your Java-based Selenium test. In case of Java/Kotlin it will try to compile it using provided
  **libraries** argument.

Java source file looks like::

    package selenium;

    import org.openqa.selenium.By;
    import org.openqa.selenium.WebDriver;
    import org.openqa.selenium.WebElement;
    import org.openqa.selenium.firefox.FirefoxDriver;


    public class MySeleniumTest {

        public static void main(String[] args) {
            WebDriver driver = new FirefoxDriver();
            try {
                driver.get("http://www.google.com/ncr");
                WebElement element = driver.findElement(By.name("q"));
                element.sendKeys("Cheese!");
                element.submit();
            } finally {
                driver.quit();
            }
        }
    }

It will go to google and search by "Cheese!" string. Catcher's step will pass if Selenium test passes.

Variables and output
--------------------
Catcher pushes it's context variables to Selenium steps as environment variables, so you can access them from tests.
For example::

    variables:
        site_url: 'http://www.google.com/ncr'
    steps:
        - selenium:
            test:
                driver: '/usr/lib/geckodriver'
                file: resources/test.js
            register: {title: '{{ OUTPUT.title }}'}
        - echo: {from: '{{ title }}', to: variable.output}

Here Catcher's context has **site_url** variable which is accessible within JavaScript (for other languages similarly).
As well as page's title is accessible for Catcher as an output variable.

test.js::

    const {Builder, By, Key, until} = require('selenium-webdriver');
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

To output any value simply use stdout. In case of Json you can access fields directly (f.e. `OUTPUT.title` json output).
If Catcher fails to parse json - it will use the whole output as a plain text.

Libraries and Dependencies
--------------------------
Catcher `dockerfile`_ comes with **nodeJS**, **Java1.8**, **Kotlin** and **Python** Selenium libraries installed,
as well as **Firefox**, **Chrome** and **Opera** drivers.

.. _dockerfile: https://github.com/comtihon/catcher_modules/blob/master/Dockerfile

| To run Selenium steps locally you need all dependencies, `libraries`_ and `drivers`_ be installed in your system.
  Driver should be included in your PATH or passed as a **driver** argument.
| In case of Java/Kotlin scr compilation you should have **javac**/**kotlinc** installed in your system.
| In case of jar files running - **java** should be installed.
| In case of JavaScript - **nodeJs** should be installed.

.. _drivers: https://www.selenium.dev/documentation/en/webdriver/driver_requirements/
.. _libraries: https://www.selenium.dev/documentation/en/selenium_installation/installing_selenium_libraries