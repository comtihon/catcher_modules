.. image:: https://travis-ci.org/comtihon/catcher_modules.svg?branch=master
    :target: https://travis-ci.org/comtihon/catcher_modules
.. image:: https://img.shields.io/pypi/v/catcher_modules.svg
    :target: https://pypi.python.org/pypi/catcher_modules
.. image:: https://img.shields.io/pypi/pyversions/catcher_modules.svg
    :target: https://pypi.python.org/pypi/catcher_modules
.. image:: https://img.shields.io/pypi/wheel/catcher_modules.svg
    :target: https://pypi.python.org/pypi/catcher_modules

Catcher modules
===============

`Catcher`_ modules repository.
Catcher support external and internal modules. This repository contains
internal Catcher modules written in python.
To implement your internal module just extend `ExternalStep` class and
create a pull request to this repository.
In case of external modules there is no need to store them in repository.
They can be implemented in any language/script and stored anywhere. Read
more about external modules [here](https://github.com/comtihon/catcher/blob/master/doc/modules.md)

.. _catcher: https://github.com/comtihon/catcher
.. _modules:

## Modules
### Databases
* Couchbase  
requirements: `libcouchbase`_
* Postgres  
requirements `postgres`_


.. _libcouchbase: http://developer.couchbase.com/documentation/server/4.5/sdk/c/start-using-sdk.html

.. _postgres: http://www.postgresql.org/docs/current/static/libpq.html
