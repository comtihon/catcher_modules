.. image:: https://github.com/comtihon/catcher_modules/actions/workflows/test.yml/badge.svg?branch=master
    :target: https://github.com/comtihon/catcher_modules/actions/workflows/test.yml
.. image:: https://img.shields.io/pypi/v/catcher_modules.svg
    :target: https://pypi.python.org/pypi/catcher_modules
.. image:: https://img.shields.io/pypi/pyversions/catcher_modules.svg
    :target: https://pypi.python.org/pypi/catcher_modules
.. image:: https://img.shields.io/pypi/wheel/catcher_modules.svg
    :target: https://pypi.python.org/pypi/catcher_modules
.. image:: https://patrolavia.github.io/telegram-badge/chat.png
    :target: https://t.me/catcher_e2e

Catcher modules
===============

| External `Catcher`_ modules repository.
| Besides the `built-in`_ Catcher support `external`_ modules: as python or any other executable scripts.
| See `Catcher`_ documentation on installation catcher with catcher-modules.

.. _Catcher: https://github.com/comtihon/catcher
.. _built-in: https://catcher-test-tool.readthedocs.io/en/latest/source/internal_modules.html
.. _external: https://catcher-test-tool.readthedocs.io/en/latest/source/steps.html#external-modules

Usage
-----
You can either write your own module in python or as external shell script. Both ways are covered
in Catcher documentation for `external`_ modules.

Read the `docs`_ for existing modules usage info: :meth:`catcher_modules`

.. _docs: https://catcher-modules.readthedocs.io/en/latest/


Contribution
------------
If you believe your external python module can be useful for other people you can create a pull request here.
You can find quick support in the telegram channel.

Contributors:
-------------
* Many thanks to `Ekaterina Belova <https://github.com/kbelova>`_ for core & modules contribution.

Additional dependencies
-----------------------
| `libclntsh.dylib` is required for `oracle`. Read more `here <https://oracle.github.io/odpi/doc/installation.html>`_.
| `unixodbc-dev` and `driver <https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server>`_ are required for `mssql`
| `Webdriver <https://www.selenium.dev/documentation/en/webdriver/driver_requirements/#quick-reference>`_ is required for selenium
| Webdriver `bundle <https://www.selenium.dev/documentation/en/selenium_installation/installing_selenium_libraries/>`_ for your language (except python) is required for selenium.