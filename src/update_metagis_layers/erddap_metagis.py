import pyodbc
import requests
import re
from bs4 import BeautifulSoup
import dotenv
import os
import pandas as pd
from datetime import datetime, time

wkdir=os.path.abspath(os.path.dirname(__file__))

class ERDDAPToMetagis:
    """
    A module that reads the necessary metadata from an ERDDAP server of interest to create new layers
    in a new Layer Collection in the Metagis database.  In this case, Bio-Oracle is used as an example.

    :param db_config: A dictionary containing the database configuration.
    :type db_config: dict
    """

    def __init__(self, db_config):
        """
        :param db_config: A dictionary containing the database configuration.
        :type db_config: dict
        """
        self.db_config = db_config

    @staticmethod
    def extract_grid_info(grid_text):
        """
        Extract grid information from DDS text.

        :param grid_text: The text containing grid information.
        :type grid_text: str
        :return: A list of grid identifiers.
        :rtype: list
        """
        pattern = r"GRID\s*{(?:.|\n)*?} (\w+);"
        matches = re.findall(pattern, grid_text)
        return matches

    def insert_data(self, entries: list):
        """
        Inserts the data from each entry from get_data into layers in Metagis database. 
        Each row entry has: title, download_url, layer, url, temporal_dates_extent, bbox,
        attributes, responsible_party, metadata.

        :param entries: A list of GIS layer data dictionaries.
        :type entries: list
        """
        try:
            conn = pyodbc.connect(
                f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.db_config['server']};"
                f"DATABASE={self.db_config['database']};UID={self.db_config['username']};"
                f"PWD={self.db_config['password']}"
            )
            cursor = conn.cursor()

            for layer in entries:
                # GIS Layer with basic metadata, as well as temporal extent, date created, and erddap url
                cursor.execute(
                    """
                    INSERT INTO gislayer (default_title, server_type, layer_name, url_server, 
                    temporal_dates_extent, user_name_created, user_id_created, date_created, url_erddap) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        layer["gislayer"]["title"], "url", layer["gislayer"]["layer"], 
                        layer["gislayer"]["url"], layer["gislayer"]["temporal_dates_extent"], 
                        "BIO-ORACLE Import", 14868, datetime.datetime.now(), layer["gislayer"]["url_erddap"]
                    )
                )
                new_id = cursor.execute('SELECT @@IDENTITY AS id;').fetchone()[0]
                conn.commit()

                # GIS Layer Bounding Box
                cursor.execute(
                    """
                    INSERT INTO gislayer_bbox (gislayer_fk, west, east, north, south) 
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        new_id, layer["gislayer_bbox"]["geospatial_lon_min"], 
                        layer["gislayer_bbox"]["geospatial_lon_max"], 
                        layer["gislayer_bbox"]["geospatial_lat_max"], 
                        layer["gislayer_bbox"]["geospatial_lat_min"]
                    )
                )

                # Metadata
                cursor.execute(
                    """
                    INSERT INTO metadata (metadata_type, metadata_value, fk_gislayer_id) 
                    VALUES (?, ?, ?)
                    """,
                    (
                        layer["metadata"]["metadata_type"], 
                        layer["metadata"]["metadata_value"], new_id
                    )
                )

                # Attributes
                for attribute in layer["gislayer_attribute"]["attributes"]:
                    cursor.execute(
                        """
                        INSERT INTO gislayer_attribute (gislayer_fk, name, name_custom, show) 
                        VALUES (?, ?, ?, ?)
                        """,
                        (new_id, attribute["name"], attribute["name_custom"], 0)
                    )

                # Responsible Party
                cursor.execute(
                    """
                    INSERT INTO gislayer_responsible_party (gislayer_id, name, role) 
                    VALUES (?, ?, ?)
                    """,
                    (new_id, layer["gislayer_responsible_party"]["name"], 
                     layer["gislayer_responsible_party"]["role"])
                )

                # Layer Collection
                fk_layercollection_id = 178 if self.db_config['database'] == 'metagis_dev' else 171
                fk_layertheme_id = 1142 if self.db_config['database'] == 'metagis_dev' else 1148
                default_title = layer["gislayer"]["title"]
                sub_theme_title = default_title.split()[1]
                sub_sub_theme_title = " ".join(default_title.split()[:2])

                cursor.execute(
                    """
                    INSERT INTO layercollectionitem (fk_gislayer_id, fk_layercollection_id, fk_layertheme_id, 
                    layer_title, sub_theme_title, sub_sub_theme_title, advanced_filter) 
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (new_id, fk_layercollection_id, fk_layertheme_id, default_title, 
                     sub_theme_title, sub_sub_theme_title, 0)
                )
                conn.commit()

            print("Data inserted successfully.")
        except pyodbc.Error as e:
            print(f"An error occurred: {e}")
        finally:
            if conn:
                conn.close()

    @staticmethod
    def get_data(url: str) -> list:
        """
        Fetch and process GIS data from an ERDDAP index url. Returns a list of dictionaries, structured to be inserted into new layers
        in the Metagis database using the insert_data method.

        :param url: The URL to fetch data from.
        :type url: str
        :return: A list of processed GIS layer data dictionaries.
        :rtype: list
        """
       
        response = requests.get(url)
        data = response.json()
        entries = []
        for dataset in data['table']['rows']:
            # Skip this row
            if dataset[15] == 'allDatasets':
                continue

            # Process attributes and metadata
            dds_data = requests.get(f"https://erddap.bio-oracle.org/erddap/griddap/{dataset[15]}.dds").text
            grid_infos = ERDDAPToMetagis.extract_grid_info(dds_data)
            attributes = [{'name': grid, 'name_custom': grid, 'show': 0} for grid in grid_infos]

            # bounds and temporal extent
            try:
                r = requests.get(dataset[10])
                coverdata = r.json()
            except Exception as e:
                print(f"request to {dataset[10]} failed, no coverage data found.")
                continue
            
            for metarow in coverdata['table']['rows']:
                if metarow[1] == "NC_GLOBAL" and metarow[2] == "geospatial_lon_max":
                    lon_max = metarow[4]
                if metarow[1] == "NC_GLOBAL" and metarow[2] == "geospatial_lat_max":
                    lat_max = metarow[4]
                if metarow[1] == "NC_GLOBAL" and metarow[2] == "geospatial_lon_min":
                    lon_min = metarow[4]
                if metarow[1] == "NC_GLOBAL" and metarow[2] == "geospatial_lat_min":
                    lat_min = metarow[4]
                if metarow[1] == "NC_GLOBAL" and metarow[2] == "time_coverage_start":
                    startdate = metarow[4]
                if metarow[1] == "NC_GLOBAL" and metarow[2] == "time_coverage_end":
                    enddate = metarow[4]

            # Download URL:
            downloaddata = requests.get(dataset[5])
            downloaddata = downloaddata.text

            soup = BeautifulSoup(downloaddata, 'html.parser')
            bin_images = soup.find_all('img', class_='B', alt='[BIN]')

            if bin_images:
                bin_image = bin_images[0]
                a_tag = bin_image.parent.find_next_sibling('td').find('a', href=True)
                if a_tag:
                    href_value = a_tag['href']
                else:
                    print("No href found after [BIN] string.")
            else:
                print("No [BIN] image found in the HTML.")
            
            # make dictionary of metadata
            entry = {
                'gislayer': {
                    'url_erddap': dataset[0],
                    'title': dataset[6],
                    'layer': dataset[15],
                    'url': dataset[4],
                    'temporal_dates_extent': startdate + '/' + enddate,
                },
                    'gislayer_bbox': {
                    'geospatial_lat_max': lat_max,
                    'geospatial_lat_min': lat_min,
                    'geospatial_lon_max': lon_max,
                    'geospatial_lon_min': lon_min,
                },
                'gislayer_responsible_party': {
                    'name': 'Bio-Oracle',
                    'role': 'resourceProvider',
                },
                'metadata': {
                    'metadata_type': 'download_url',
                    'metadata_value': dataset[5] + href_value,
                },
                'gislayer_attribute': {
                    'attributes': attributes,
                },
            }
            entries.append(entry)
        return entries


    def delete_layers_from_user(self, userid):
        """
        Deletes all GIS layers and associated records for a given user from the database.
        """
        # Connect to the database
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={db_config['server']};"
            f"DATABASE={db_config['database']};"
            f"UID={db_config['username']};"
            f"PWD={db_config['password']}"
        )
        cursor = conn.cursor()

        try:
            # Fetch layer IDs created by the user
            select_query = f"SELECT id FROM gislayer WHERE user_id_created = {userid}"
            cursor.execute(select_query)
            rows = cursor.fetchall()

            # Extract IDs into a comma-separated string
            ids = [str(row[0]) for row in rows]
            if not ids:
                print("No layers found.")
                return

            id_list_string = ','.join(ids)

            # Delete associated records
            queries = [
                f"DELETE FROM gislayer_bbox WHERE gislayer_fk IN ({id_list_string})",
                f"DELETE FROM metadata WHERE fk_gislayer_id IN ({id_list_string})",
                f"DELETE FROM gislayer_attribute WHERE gislayer_fk IN ({id_list_string})",
                f"DELETE FROM gislayer_responsible_party WHERE gislayer_id IN ({id_list_string})",
                f"DELETE FROM layercollectionitem WHERE fk_gislayer_id IN ({id_list_string})",
                f"DELETE FROM gislayer WHERE id IN ({id_list_string})"
            ]

            # Execute each query
            for query in queries:
                cursor.execute(query)

            # Commit transaction
            conn.commit()
            print("Layers deleted successfully.")

        except Exception as e:
            # Roll back transaction on error
            conn.rollback()
            print(f"Error: {e}")

        finally:
            # Ensure resources are closed
            cursor.close()
            conn.close()

def run_erddap_to_metagis(db_config):
    """
    Main function to create new layers in the Metagis database from an ERDDAP server.
    Gets data from the ERDDAP server, inserts it into the Metagis database, and can delete layers from a user. Requires
    a database configuration dictionary, which should contain the server, database, username, and password.

    :param db_config: A dictionary containing the database configuration.
    :type db_config: dict

    """
     # Example usage
    erddap_to_metagis = ERDDAPToMetagis(db_config)
    data = erddap_to_metagis.get_data("https://erddap.bio-oracle.org/erddap/info/index.json")
    if not data:
        print("No data to insert.")
        return
    df = pd.DataFrame(data).to_csv(f'{wkdir}/erddap_data.csv')

    input("Press Enter to insert data into Metagis database or Ctrl+C to exit.")
    erddap_to_metagis.insert_data(db_config, data)

    input("Press Enter to delete layers from user with ID 14868 or Ctrl+C to exit.")
    erddap_to_metagis.delete_layers_from_user(14868)

# Database configuration
db_config = {
    'server': None,
    'database': None,
    'kerbuser': None,
    'kerbpwd': None,
    'mguser': None,
    'mgpwd': None
}

import dotenv
credsfile = f"{os.path.dirname(os.path.abspath(__file__))}/../../data/creds/mg.env"

if os.path.exists(credsfile):
    dotenv.load_dotenv(credsfile)
    db_config['server'] = os.getenv('SERVER')
    db_config['database'] = os.getenv('DB')
    db_config['kerbuser'] = os.getenv('KERB_USER')
    db_config['kerbpwd'] = os.getenv('KERB_PWD')
    db_config['mguser'] = os.getenv('MG_USER')
    db_config['mgpwd'] = os.getenv('MG_PWD')
import sys

print("Arguments passed to script:", sys.argv)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Update Metagis Layers")
    parser.add_argument('--erddapurl', required=True, help="ERDDAP URL, ex https://erddap.bio-oracle.org/erddap/info/index.json")
    args = parser.parse_args()

    if not all(db_config.values()):
        db_config['server'] = input("Enter Metagis server: ")
        db_config['database'] = input("Enter Metagis database: ")
        db_config['username'] = input("Enter Metagis username: ")
        db_config['password'] = input("Enter Metagis password: ")
        db_config['kerbuser'] = input("Enter Kerberos username: ")
        db_config['kerbpwd'] = input("Enter Kerberos password: ")
    from db import DBConnect
    db_connection = DBConnect(db_config)
    db_connection.obtain_kerberos_ticket(db_config['kerbuser'], db_config['kerbpwd'])
    run_erddap_to_metagis(args.erddapurl, db_config)