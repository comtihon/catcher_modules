FROM comtihon/catcher_base:latest

WORKDIR /catcher_modules
COPY catcher_modules catcher_modules
COPY requirements.txt requirements.txt
COPY setup.py setup.py

RUN apk update \
  && apk add gcc g++ curl unixodbc-dev postgresql-dev libcouchbase-dev libffi-dev mariadb-connector-c-dev \
  && pip install Cython

RUN curl -O https://download.microsoft.com/download/e/4/e/e4e67866-dffd-428c-aac7-8d28ddafb39b/msodbcsql17_17.5.2.2-1_amd64.apk \
    && apk add --allow-untrusted msodbcsql17_17.5.2.2-1_amd64.apk

RUN pip install -e "file://`pwd`#egg=catcher-modules[all]"

WORKDIR /opt/catcher/