import csv


def csv_to_generator(stream):
    for row in csv.reader(stream):
        yield row


def table_to_generator(table, engine):
    connection = engine.connect()
    proxy = connection.execution_options(stream_results=True).execute("select * from {}".format(table))
    while True:
        batch = proxy.fetchmany(1000)
        if not batch:
            break

        for row in batch:
            yield row
    proxy.close()
    connection.close()
