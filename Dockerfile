FROM comtihon/catcher_base:latest

WORKDIR /catcher_modules
COPY catcher_modules catcher_modules
COPY requirements.txt requirements.txt
COPY Readme.rst Readme.rst
COPY setup.py setup.py

## database client libraries
# client libraries for postgres, mysql, couchbase
RUN apk update \
  && apk add --no-cache wget git unzip build-base gcc abuild binutils binutils-doc g++ cmake ninja curl zip bash extra-cmake-modules \
  unixodbc-dev postgresql-dev libcouchbase-dev libffi-dev mariadb-connector-c-dev \
  && pip install Cython docutils
# client library for mssql
RUN curl -O https://download.microsoft.com/download/e/4/e/e4e67866-dffd-428c-aac7-8d28ddafb39b/msodbcsql17_17.5.2.2-1_amd64.apk \
    && apk add --allow-untrusted msodbcsql17_17.5.2.2-1_amd64.apk

## languages for external step support
# install java
# install java, kotlin, node js
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
ENV PATH="$JAVA_HOME/bin:${PATH}"
ENV PATH="/root/.sdkman/candidates/kotlin/current/bin:${PATH}"
RUN apk --no-cache add openjdk11 nodejs npm \
  && curl -s https://get.sdkman.io | bash \
  && bash -c 'source "/root/.sdkman/bin/sdkman-init.sh" && sdk install kotlin'

## browsers
RUN apk add firefox-esr chromium chromium-chromedriver

## selenium drivers (firefox, opera)
RUN wget https://github.com/mozilla/geckodriver/releases/download/v0.30.0/geckodriver-v0.30.0-linux64.tar.gz \
  && mkdir -p /usr/lib/selenium && tar -xf geckodriver-v0.30.0-linux64.tar.gz -C /usr/bin/ \
  && chmod +x /usr/bin/geckodriver
RUN wget https://github.com/operasoftware/operachromiumdriver/releases/download/v.102.0.5005.61/operadriver_linux64.zip \
  && unzip operadriver_linux64.zip -d /usr/bin/ \
  && chmod +x /usr/bin/operadriver_linux64

RUN pip install -e "file://`pwd`#egg=catcher-modules[all]"
WORKDIR /opt/catcher/

## selenium libraries
RUN npm install selenium-webdriver
RUN mkdir -p /usr/share/java \
 && wget https://selenium-release.storage.googleapis.com/3.141/selenium-java-3.141.59.zip \
 && unzip selenium-java-3.141.59.zip -d /usr/share/java \
 && mv /usr/share/java/libs/*.jar /usr/share/java/
RUN wget https://repo1.maven.org/maven2/com/google/auto/service/auto-service-annotations/1.0-rc7/auto-service-annotations-1.0-rc7.jar -P /usr/share/java/
