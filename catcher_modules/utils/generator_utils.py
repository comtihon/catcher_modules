import csv


def csv_to_generator(filename):
    with open(filename, "r") as csv_file:
        for row in csv.reader(csv_file):
            yield row


# TODO make me memory-efficient
def table_to_generator(table, engine):
    from sqlalchemy.orm import Session
    session = Session(engine)
    try:
        return (row for row in session.query(table).all())
    finally:
        session.close()