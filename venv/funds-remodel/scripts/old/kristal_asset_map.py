import csv
import psycopg2
import configparser
import pandas as pd
from methods import *

config = configparser.ConfigParser()
config.read('../../configs/config.ini')
pd.set_option("mode.chained_assignment", None)

print("Started executing kristal_asset_map.py!!!")


def write_data_to_csv(data, output_file):
    if not data:  # Check if data is empty
        print("No data to write.")
        return

    fieldnames = data[0].keys()

    try:
        with open(output_file, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

        print(f"Data successfully written to {output_file}.")
    except Exception as e:
        print(f"An error occurred while writing to the file: {e}")


try:
    connection = psycopg2.connect(host=config['postgresDB']['host'],
                                  user=config['postgresDB']['user'],
                                  password=config['postgresDB']['pass'],
                                  port=config['postgresDB']['port'],
                                  database=config['postgresDB']['db'])

    # Open and read the file as a single buffer
    fd = open('../../files/in/kristal_asset_queries.sql', 'r')
    sqlFile = fd.read()
    fd.close()
    sqlCommands = sqlFile.split(';')
    kristal_to_asset_data = postgresql_to_record(connection, sqlCommands[0])
    asset_to_kristal_data = postgresql_to_record(connection, sqlCommands[1])

    if kristal_to_asset_data != 1:
        write_data_to_csv(kristal_to_asset_data, "../../files/itd/kristal_to_asset.csv")
    else:
        print("Error occurred while fetching kristal_to_asset_data.")

    if asset_to_kristal_data != 1:
        write_data_to_csv(asset_to_kristal_data, "../../files/itd/asset_to_kristal.csv")
    else:
        print("Error occurred while fetching asset_to_kristal_data.")


finally:
    if connection:
        connection.close()
        print("Finished executing kristal_asset_map.py!!!")
