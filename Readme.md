# Catcher modules
[![Build Status](https://travis-ci.org/comtihon/catcher_modules.svg?branch=master)](https://travis-ci.org/comtihon/catcher_modules)
[![PyPI](https://img.shields.io/pypi/v/catcher_modules.svg)](https://pypi.python.org/pypi/catcher_modules)
[![PyPI](https://img.shields.io/pypi/pyversions/catcher_modules.svg)](https://pypi.python.org/pypi/catcher_modules)
[![PyPI](https://img.shields.io/pypi/wheel/catcher_modules.svg)](https://pypi.python.org/pypi/catcher_modules)  
[Catcher](https://github.com/comtihon/catcher) modules repository.
Catcher support external and internal modules. This repository contains
internal Catcher modules written in python.
To implement your internal module just extend `ExternalStep` class and
create a pull request to this repository.
In case of external modules there is no need to store them in repository.
They can be implemented in any language/script and stored anywhere. Read
more about external modules [here](https://github.com/comtihon/catcher/blob/master/doc/modules.md)

## Modules
### Databases
* Couchbase  
requirements:
[libcouchbase](http://developer.couchbase.com/documentation/server/4.5/sdk/c/start-using-sdk.html)
* Postgres  
requirements
[libpq](http://www.postgresql.org/docs/current/static/libpq.html)
