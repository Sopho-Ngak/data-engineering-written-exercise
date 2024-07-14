#!/usr/bin/env python

import sys
import argparse

# import csv
# import json
# import sqlalchemy

from src.etl import ETL

LIST_COMMANDS = [
    "run_etl",
    "load_to_postgresql",
    "export_json",
]


def parse_arguments():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='ETL for loading csv file into a postgres table and exporting it to a JSON file')

    parser.add_argument("command_name", type=str, choices=LIST_COMMANDS,
                        help="Name of the command to be executed", )
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_arguments()
    etl = ETL()
    if args.command_name == "run_etl":
        etl.run_etl()
    elif args.command_name == "load_to_postgresql":
        etl.load_to_postgres()
    elif args.command_name == "export_json":
        etl.export_json_from_postgres()
    else:
        print("Invalid command. Please select from the following commands: ", LIST_COMMANDS)
        sys.exit(1)


# # connect to the database
# engine = sqlalchemy.create_engine("postgresql://codetest:password@database/codetest")
# connection = engine.connect()

# metadata = sqlalchemy.schema.MetaData(engine)

# # make an ORM object to refer to the table
# Example = sqlalchemy.schema.Table('examples', metadata, autoload=True, autoload_with=engine)

# # read the CSV data file into the table
# with open('/data/example.csv') as csv_file:
#   reader = csv.reader(csv_file)
#   next(reader)
#   for row in reader:
#     connection.execute(Example.insert().values(name = row[0]))

# # output the table to a JSON file
# with open('/data/example_python.json', 'w') as json_file:
#   rows = connection.execute(sqlalchemy.sql.select([Example])).fetchall()
#   rows = [{'id': row[0], 'name': row[1]} for row in rows]
#   json.dump(rows, json_file, separators=(',', ':'))
