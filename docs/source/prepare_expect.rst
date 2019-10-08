Prepare
=======

There is a special step `prepare`_, which allows you to populate your data sources with batch operations.
It is extremely useful when testing big data pipelines or when you need some amount of data pre-populated.

.. _prepare: https://catcher-modules.readthedocs.io/en/latest/source/catcher_modules.service.html#catcher-modules-service-prepare-module

How it worked before?
---------------------

You have to populate the database manually::

    ---
    variables:
        username: 'test'
    steps:
        - postgres:
            actions:
                - request:
                    conf: '{{ postgres }}'
                    query: "CREATE TABLE foo(user_id integer primary key, email varchar(36) NOT NULL);"
                  name: 'create foo table'
                - request:
                    conf: '{{ postgres }}'
                    query: "CREATE TABLE bar(key varchar(36) primary key, value varchar(36) NOT NULL);"
                  name: 'create bar table'
                - request:
                    conf: '{{ postgres }}'
                    query: "INSERT INTO foo values (1,"{{username}}1@test.com"),(2,"{{username}}2@test.com");
                  name: 'populate foo table'
                - request:
                    conf: '{{ postgres }}'
                    query: "INSERT INTO bar values ("key1","value1"),("key2","value2");
                  name: 'populate bar table'
        ... and now you can run your test steps

It is not so nice, as you have to write a lot of steps just to pre-populate your data source.

How it works now?
-----------------
| For the postgres example above.
| 1. You create a schema file with DDL.

`resources/schema.sql`::

    CREATE TABLE foo(
        user_id      varchar(36)    primary key,
        email        varchar(36)    NOT NULL
    );

    CREATE TABLE bar(
        key           varchar(36)    primary key,
        value         varchar(36)    NOT NULL
    );

| 2. You create a data file for each table you'd like to populate.

`resources/foo.csv`::

    user_id,email
    1,{{username}}1@test.com
    2,{{username}}2@test.com

`resources/bar.csv`::

    key,value
    k1,v1
    k2,v2

| 3. You call one prepare step for postgres.

`test.yml`::

    ---
    steps:
        - prepare:
            populate:
                postgres:
                    conf: '{{ postgres }}'
                    schema: pg_schema.sql
                    data:
                        foo: foo.csv
                        bar: bar.csv
        ... and now you can run your test steps

| In this step you prepare all the data needed. Tables will be created and populated.
| **Note** on templating - it is fully supported. Even new rows can be generated in the csv files.

`foo.csv`::

    user_id,email
    {% for user in users %}
    {{ loop.index }},{{ user }}
    {% endfor %}
    4,other_email

Expect
======

There is a special step `expect`_, which allows you to check your data sources with predefined data.
It is extremely useful when testing big data pipelines or when your services produce a lot of data to be checked.

.. _expect: https://catcher-modules.readthedocs.io/en/latest/source/catcher_modules.service.html#catcher-modules-service-expect-module

How it worked before?
---------------------

You have to check the database manually::

    steps:
        - ... calls to your services producing data
        - postgres:
            request:
                conf: '{{ postgres }}'
                query: 'select count(*) from foo'
            register: {documents: '{{ OUTPUT }}'}
        - check:
            equals: {the: '{{ documents }}', is: 2}
        - postgres:
            request:
                conf: '{{ postgres }}'
                query: 'select count(*) from bar'
            register: {documents: '{{ OUTPUT }}'}
        - check:
            equals: {the: '{{ documents }}', is: 2}

Even if you run postgres + check steps as a registered include it is still a lot of unnecessary (from now) steps.

How it works now?
-----------------
| For the postgres example above.
| 1. Create csv with expected data for the tables.

`resources/foo.csv`::

    user_id,email
    1,test1@test.com
    2,test2@test.com

`resources/bar.csv`::

    key,value
    k1,v1
    k2,v2

| 2. Run the expect step.

`test.yml`::

        steps:
            - ... calls to your services producing data
            - expect:
                compare:
                    postgres:
                        conf: 'test:test@localhost:5433/test'
                        data:
                            foo: foo.csv
                            bar: bar.csv


| **Note** that not all steps support prepare-expect for now.
| **Note** on templating - it is fully supported. Even new rows can be generated in the csv files.
