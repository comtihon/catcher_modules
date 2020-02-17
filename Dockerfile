FROM comtihon/catcher_base:latest

WORKDIR /catcher_modules
COPY catcher_modules catcher_modules
COPY requirements.txt requirements.txt
COPY setup.py setup.py

RUN apk update \
  && apk add gcc musl-dev postgresql-dev libcouchbase-dev freetds-dev \
  && pip install Cython

RUN pip install -e "file://`pwd`#egg=catcher-modules[all]"

WORKDIR /opt/catcher/