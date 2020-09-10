from catcher.steps.external_step import ExternalStep
from catcher.steps.step import Step, update_variables


class Elastic(ExternalStep):
    """
    Allows you to get data from `Elasticsearch <https://www.elastic.co/elastic-stack>`_. Useful, when your services push their
    logs there and you need to check the logs automatically from the test.

    :Input:

    :search: search elastic

    - url: RFC-1738 compatible (can contain user credentials) server url.
    - index: ES index (database).
    - query: your query to run.
    - <other param>: you can add any param here (see Search with limiting fields for an example)

    :refresh: Trigger a refresh for an index.

    - url: RFC-1738 compatible (can contain user credentials) server url.
    - index: ES index (database).

    :Examples:

    Search with limiting fields
    ::

        elastic:
            search:
                url: 'http://127.0.0.1:9200'
                index: test
                query:
                    match: {payload : "three"}
                _source: ['name']
            register: {docs: '{{ OUTPUT }}'}

    Connect to multiple ES instances. One simple and one secured
    ::

        elastic:
            search:
                url:
                    - 'http://127.0.0.1:9200'
                    - 'https://{{ user }}:{{ secret }}@{{ host2 }}:443'
                index: test
                query: {match_all: {}}

    Refresh index
    ::

        elastic:
            refresh:
                url: 'http://127.0.0.1:9092'
                index: test

    In bool query `must` and `should` are lists
    ::

        elastic:
            search:
                url: 'http://127.0.0.1:9200'
                index: test
                query:
                    bool:
                        must:
                            - term: {shape: "round"}
                            - bool:
                                should:
                                    - term: {color: "red"}
                                    - term: {color": "blue"}
    """

    @update_variables
    def action(self, includes: dict, variables: dict) -> any:
        from elasticsearch import Elasticsearch
        body = self.simple_input(variables)
        method = Step.filter_predefined_keys(body)  # search/refresh
        conf = body[method]
        index = conf['index']
        url = conf['url']
        if isinstance(url, str):
            url = [url]
        es = Elasticsearch(url)
        if method == 'search':
            return variables, self._search(es, index, conf)
        elif method == 'refresh':
            return variables, self._refresh(es, index)
        else:
            raise AttributeError('unknown method: ' + method)

    def _search(self, es, index, conf):
        query = dict([(key, value) for key, value in conf.items() if key != 'index' and key != 'url'])
        res = es.search(index, query)
        return [hit['_source'] for hit in res['hits']['hits']]

    def _refresh(self, es, index):
        return es.indices.refresh(index)
