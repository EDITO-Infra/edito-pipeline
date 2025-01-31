import logging
import pyodbc
import pandas as pd
from datetime import date
from dotenv import load_dotenv
import os
import subprocess


today = date.today().strftime("%Y-%m-%d")
logger = logging.getLogger(__name__)


class DBConnect:
    """
    Manages a connection to a SQL Server database.

    :param server: The server address.
    :type server: str
    :param database: The database name.
    :type database: str
    """
    def __init__(self, db_config):
        """
        Initializes the DatabaseConnection with the server and database details.

        :param server: The server address.
        :type server: str
        :param database: The database name.
        :type database: str
        """
        self.db_config = db_config
        self.connection = None

    def connect(self, server=None, database=None, mguser=None, mgpwd=None):
        """
        Establishes a connection to the SQL Server database. Needs kerberos ticket

        :raises pyodbc.Error: If there is an error in establishing the connection.
        """
        if server and database and mguser and mgpwd:
            connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={server},1433;"
                f"DATABASE={database};"
                f"UID={mguser};"
                f"PWD={mgpwd};"
                f"Trusted_Connection=yes;"
                f"TrustServerCertificate=yes;"
            )
            try:
                self.connection = pyodbc.connect(connection_string)
                print("Connection successful!")
                logger.info("Connection successful!")
            except pyodbc.Error as e:
                print(f"Error in connection: {e}")
                logger.error(f"Error in connection: {e}")
        else:
            logger.error("Server, database, username, and password not provided")
    
    def close(self):
        """
        Closes the connection to the SQL Server database.
        """
        if self.connection:
            self.connection.close()
            print("Connection closed.")
            logger.info("Connection closed.")


    def obtain_kerberos_ticket(self, kerbuser=None, kerbpwd=None):
        """
        Gets kerberos ticket, to authenticate db connection.

        :param secret_manager: The secret manager object.
        :type secret_manager: SecretManager
        """
        try:
            if kerbuser and kerbpwd:
                subprocess.run(['kinit', '-V', kerbuser], input=kerbpwd, text=True, check=True)
                self.kerberos_ticket = True
            else:
                logger.error("Kerberos user and password not provided")
        except subprocess.CalledProcessError as e:
            print(f"Error obtaining kerberos ticket: {e}")
            logger.error(f"Error obtaining kerberos ticket: {e}")


class DBQuery:
    """
    Fetches current database info before new data is inserted

    :param db_connection: The database connection object.
    :type db_connection: DatabaseConnection
    """
    def __init__(self, db_connection):
        """
        Initializes the DatabaseQuery with a database connection.

        :param db_connection: The database connection object.
        :type db_connection: DatabaseConnection
        """
        self.db_connection = db_connection

    def fetch_single_value(self, query, params):
        """
        Fetches a single value from the database.

        :param query: The SQL query to execute.
        :type query: str
        :param params: The parameters to pass to the query.
        :type params: tuple
        :return: The single value fetched from the database, or None if no value is found.
        :rtype: any
        """
        try:
            cursor = self.db_connection.connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()
            return row[0] if row else None
        except pyodbc.Error as e:
            logger.error(f"Error fetching data: {e}")
            return None

    def fetch_multiple_rows(self, query, params):
        """
        Fetches multiple rows from the database.

        :param query: The SQL query to execute.
        :type query: str
        :param params: The parameters to pass to the query.
        :type params: tuple
        :return: A list of rows fetched from the database.
        :rtype: list
        """
        try:
            cursor = self.db_connection.connection.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            cursor.close()
            return rows
        except pyodbc.Error as e:
            logger.error(f"Error fetching data: {e}")
            return []

    def get_layer_download_url(self, metagis_id):
        """
        Retrieves the download URL for a specific layer.

        :param metagis_id: The ID of the layer.
        :type metagis_id: int
        :return: The download URL for the layer.
        :rtype: str
        """
        query = (
            "SELECT metadata.metadata_value "
            "FROM metadata "
            "WHERE metadata.fk_gislayer_Id = ? AND metadata.metadata_type = 'download_url'"
        )
        val = self.fetch_single_value(query, (metagis_id,))
        logger.info(f"Download URL fetched for layer {metagis_id}")
        return val

    def get_layer_collection_id(self, layer_collection):
        """
        Retrieves the collection ID for a specific collection.

        :param layer_collection: The collection name.
        :type layer_collection: str
        :return: The collection ID.
        :rtype: int
        """
        query = (
            "SELECT id "
            "FROM layercollection "
            "WHERE name = ?"
        )
        val = self.fetch_single_value(query, (layer_collection,))
        logger.info(f"Collection ID fetched for {layer_collection}")
        return val


    def get_thematic_lot_theme_id(self, layer_collection_id, thematic_lot):
        """
        Retrieves the thematic lot ID for a specific thematic lot.

        :param thematic_lot: The thematic lot name.
        :type thematic_lot: str
        :return: The thematic lot ID.
        :rtype: int
        """
        query = (
            "SELECT id "
            "FROM layertheme "
            "WHERE fk_layercollection_id = ? AND name = ?"
        )
        val = self.fetch_single_value(query, (layer_collection_id, thematic_lot))
        logger.info(f"Thematic lot ID fetched for {thematic_lot}")
        return val

    def get_layer_themes(self, metagis_id):
        """
        Retrieves the themes for a specific layer.

        :param metagis_id: The ID of the layer.
        :type metagis_id: int
        :return: A list of themes for the layer.
        :rtype: list
        """
        query = (
            "SELECT layercollectionitem.sub_theme_title, layercollectionitem.sub_sub_theme_title, layercollectionitem.layer_title "
            "FROM layercollectionitem "
            "INNER JOIN layertheme ON layercollectionitem.fk_layertheme_id = layertheme.id "
            "WHERE layercollectionitem.fk_gislayer_id = ? AND layercollectionitem.fk_layercollection_id = 170"
        )
        vals = self.fetch_multiple_rows(query, (metagis_id,))
        logger.info(f"Themes fetched for layer {metagis_id}")
        return vals

    def get_layer_native_variables(self, metagis_id):
        """
        Retrieves the native variables for a specific layer.

        :param metagis_id: The ID of the layer.
        :type metagis_id: int
        :return: A list of native variables for the layer.
        :rtype: list
        """
        query = (
            "SELECT name "
            "FROM gislayer_attribute "
            "WHERE gislayer_attribute.gislayer_fk = ?"
        )
        vals = self.fetch_multiple_rows(query, (metagis_id,))
        logger.info(f"Native variables fetched for layer {metagis_id}")
        return vals

    def get_layer_arco_variables(self, metagis_id):
        """
        Retrieves the ARCO variables for a specific layer.

        :param metagis_id: The ID of the layer.
        :type metagis_id: int
        :return: A list of ARCO variables for the layer.
        :rtype: list
        """
        query = (
            "SELECT name_custom "
            "FROM gislayer_attribute "
            "WHERE gislayer_attribute.gislayer_fk = ?"
        )
        vals = self.fetch_multiple_rows(query, (metagis_id,))
        logger.info(f"Arco variables fetched for layer {metagis_id}")
        return vals

    def get_single_arco_variable(self, metagis_id, native_variable):
        """
        Retrieves a single ARCO variable for a specific layer and native variable.

        :param metagis_id: The ID of the layer.
        :type metagis_id: int
        :param native_variable: The native variable name.
        :type native_variable: str
        :return: The ARCO variable name.
        :rtype: str
        """
        query = (
            "SELECT name_custom "
            "FROM gislayer_attribute "
            "WHERE gislayer_attribute.gislayer_fk = ? AND gislayer_attribute.name = ?"
        )
        val = self.fetch_single_value(query, (metagis_id, native_variable))
        logger.info(f"Arco variable fetched for layer {metagis_id} {native_variable}")
        return val

    def get_stac_status(self, metagis_id, native_variable):
        """
        Retrieves the STAC status for a specific layer and native variable.

        :param metagis_id: The ID of the layer.
        :type metagis_id: int
        :param native_variable: The native variable name.
        :type native_variable: str
        :return: The STAC status.
        :rtype: str
        """
        query = (
            "SELECT export_stac "
            "FROM gislayer_attribute "
            "WHERE gislayer_attribute.gislayer_fk = ? AND gislayer_attribute.name = ?"
        )
        val = self.fetch_single_value(query, (metagis_id, native_variable))
        logger.info(f"STAC status fetched for layer {metagis_id} {native_variable}")
        return val

    def get_layer_data_product(self, metagis_id):
        """
        Retrieves the data product information for a specific layer.

        :param metagis_id: The ID of the layer.
        :type metagis_id: int
        :return: The data product information.
        :rtype: str
        """
        query = (
            "SELECT metadata.metadata_value "
            "FROM metadata "
            "WHERE metadata.fk_gislayer_Id = ? AND metadata.metadata_type = 'edito_info'"
        )
        val = self.fetch_single_value(query, (metagis_id,))
        logger.info(f"Data product fetched for layer {metagis_id}")
        return val

    def get_all_layers_per_theme(self, layer_theme_id):
        """
        Retrieves all layers for a specific theme.

        :param layer_theme_id: The ID of the theme.
        :type layer_theme_id: int
        :return: A DataFrame containing all layers for the theme.
        :rtype: pandas.DataFrame
        """
        query = (
            "SELECT fk_gislayer_id AS metagis_id, layercollectionitem.sub_theme_title, layercollectionitem.sub_sub_theme_title, layercollectionitem.layer_title "
            "FROM layercollectionitem "
            "WHERE layercollectionitem.fk_layercollection_id = 170 AND layercollectionitem.fk_layertheme_id = ?"
        )
        rows = self.fetch_multiple_rows(query, (layer_theme_id,))
        if rows:
            rows = [tuple(row) for row in rows]
            return pd.DataFrame(rows, columns=['metagis_id', 'edito_variable_family', 'edito_collection', 'collection_item']) if rows else pd.DataFrame()
        return pd.DataFrame()


class DBInsert:
    """
    Handles insertion and updates of database records.

    :param db_connection: The database connection object.
    :type db_connection: DatabaseConnection
    """
    def __init__(self, db_connection):
        """
        Initializes the DatabaseInsert with a database connection.

        :param db_connection: The database connection object.
        :type db_connection: DatabaseConnection
        """
        self.db_connection = db_connection

    def execute_update(self, query, params):
        """
        Executes an update query on the database.

        :param query: The SQL query to execute.
        :type query: str
        :param params: The parameters to pass to the query.
        :type params: tuple
        :return: True if the update was successful, False otherwise.
        :rtype: bool
        """
        try:
            cursor = self.db_connection.connection.cursor()
            cursor.execute(query, params)
            self.db_connection.connection.commit()
            cursor.close()
            return True
        except pyodbc.Error as e:
            print(f"Error executing update: {e}")
            return False

    def insert_subtheme(self, metagis_id, subtheme):
        """
        Updates the subtheme for a specific layer.

        :param metagis_id: The ID of the layer.
        :type metagis_id: int
        :param subtheme: The subtheme to set.
        :type subtheme: str
        :return: The updated subtheme.
        :rtype: str
        """
        query = (
            "UPDATE layercollectionitem "
            "SET sub_theme_title = ? "
            "WHERE fk_gislayer_id = ? AND fk_layercollection_id = 170"
        )
        if self.execute_update(query, (subtheme, metagis_id)):
            logger.info(f"Subtheme updated for layer {metagis_id} {subtheme}")
            return subtheme

    def insert_subsubtheme(self, metagis_id, subsubtheme):
        """
        Updates the sub-subtheme for a specific layer.

        :param metagis_id: The ID of the layer.
        :type metagis_id: int
        :param subsubtheme: The sub-subtheme to set.
        :type subsubtheme: str
        :return: The updated sub-subtheme.
        :rtype: str
        """
        query = (
            "UPDATE layercollectionitem "
            "SET sub_sub_theme_title = ? "
            "WHERE fk_gislayer_id = ? AND fk_layercollection_id = 170"
        )
        if self.execute_update(query, (subsubtheme, metagis_id)):
            logger.info(f"Subsubtheme updated for layer {metagis_id} {subsubtheme}")
            return subsubtheme

    def insert_title(self, metagis_id, title):
        """
        Updates the title for a specific layer.

        :param metagis_id: The ID of the layer.
        :type metagis_id: int
        :param title: The title to set.
        :type title: str
        :return: The updated title.
        :rtype: str
        """
        query = (
            "UPDATE layercollectionitem "
            "SET layer_title = ? "
            "WHERE fk_gislayer_id = ? AND fk_layercollection_id = 170"
        )
        if self.execute_update(query, (title, metagis_id)):
            logger.info(f"Title updated for layer {metagis_id} {title}")
            return title

    def insert_download_url(self, metagis_id, download_url):
        """
        Updates the download URL for a specific layer.

        :param metagis_id: The ID of the layer.
        :type metagis_id: int
        :param download_url: The download URL to set.
        :type download_url: str
        :return: The updated download URL.
        :rtype: str
        """
        query = (
            "UPDATE metadata "
            "SET metadata_value = ? "
            "WHERE fk_gislayer_Id = ? AND metadata_type = 'download_url'"
        )
        if self.execute_update(query, (download_url, metagis_id)):
            logger.info(f"Download URL updated for layer {metagis_id} {download_url}")
            return download_url

    def insert_data_product(self, db_query, metagis_id, data_product):
        """
        Updates or inserts the data product information for a specific layer.

        :param db_query: The database query object.
        :type db_query: DatabaseQuery
        :param metagis_id: The ID of the layer.
        :type metagis_id: int
        :param data_product: The data product information to set.
        :type data_product: str
        :return: The updated or inserted data product information.
        :rtype: str
        """
        # First, check if the record exists
        check_query = (
            "SELECT COUNT(*) "
            "FROM metadata "
            "WHERE fk_gislayer_Id = ? AND metadata_type = 'edito_info'"
        )
        exists = db_query.fetch_single_value(check_query, (metagis_id,))
        
        if exists:
            if data_product == db_query.get_layer_data_product(metagis_id):
                logger.info(f"Data product already exists for layer {metagis_id} {data_product}")
                return data_product
            update_query = (
                "UPDATE metadata "
                "SET metadata_value = ? "
                "WHERE fk_gislayer_Id = ? AND metadata_type = 'edito_info'"
            )
            if self.execute_update(update_query, (data_product, metagis_id)):
                logger.info(f"Data product updated for layer {metagis_id} {data_product}")
                return data_product
        else:
            # If the record does not exist, perform an INSERT
            insert_query = (
                "INSERT INTO metadata (fk_gislayer_Id, metadata_type, metadata_value) "
                "VALUES (?, 'edito_info', ?)"
            )
            if self.execute_update(insert_query, (metagis_id, data_product)):
                logger.info(f"Data product inserted for layer {metagis_id} {data_product}")
                return data_product

    def insert_attributes(self, db_query, metagis_id, updated_layer_attributes_df):
        """
        Inserts or updates attributes for a specific layer.

        :param db_query: The database query object.
        :type db_query: DatabaseQuery
        :param metagis_id: The ID of the layer.
        :type metagis_id: int
        :param updated_layer_attributes_df: DataFrame containing the updated layer attributes.
        :type updated_layer_attributes_df: pandas.DataFrame
        :return: A list of tuples containing native and ARCO variable pairs.
        :rtype: list
        """
        # List to collect the records
        native_arco_pairs = []
        
        for _, row in updated_layer_attributes_df.iterrows():
            if pd.isna(row['arco_variable']) or row['arco_variable'] == '':
                logger.info(f"No arco var for native_variable {row['native_variable']} in updated attributes CSV")
                continue

            old_arco_variable = db_query.get_single_arco_variable(metagis_id, row['native_variable'])
            
            export_to_stac = 1  # True
            show_value = 0  # False
            if not old_arco_variable:
                print(f"no old arco var for native_variable: {row['native_variable']} inserting native arco var pair")
                insert_query = (
                    "INSERT INTO gislayer_attribute (gislayer_fk, name, name_custom, show, export_stac) "
                    "VALUES (?, ?, ?, ?, ?)"
                )
                if self.execute_update(insert_query, (metagis_id, row['native_variable'], row['arco_variable'], show_value, export_to_stac)):
                    # Append as a tuple (native_variable, arco_variable)
                    native_arco_pairs.append((row['native_variable'], row['arco_variable']))
                    logger.info(f"inserted layer {metagis_id} native var {row['native_variable']}, arco var {row['arco_variable']}")
            
            elif old_arco_variable != row['arco_variable']:
                logger.info(f"old arco var {old_arco_variable} is different from new arco var {row['arco_variable']}")
                update_query = (
                    "UPDATE gislayer_attribute "
                    "SET name_custom = ?, export_stac = ? "
                    "WHERE gislayer_fk = ? AND name = ?"
                )
                if self.execute_update(update_query, (row['arco_variable'], export_to_stac, metagis_id, row['native_variable'])):
                    # Append as a tuple (native_variable, arco_variable)
                    native_arco_pairs.append((row['native_variable'], row['arco_variable']))
                    logger.info(f"updated layer {metagis_id} native var {row['native_variable']}, old arco var {old_arco_variable}, new arco var {row['arco_variable']}")
            
            elif old_arco_variable == row['arco_variable']:
                logger.info(f"old arco var {old_arco_variable} is the same as new arco var {row['arco_variable']}")
                # Append as a tuple (native_variable, arco_variable)
                native_arco_pairs.append((row['native_variable'], row['arco_variable']))
        return native_arco_pairs 
