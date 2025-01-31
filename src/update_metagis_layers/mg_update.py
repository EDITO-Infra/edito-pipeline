import os
import pandas as pd
from datetime import date
import logging
import dotenv
from db import DBQuery, DBInsert, DBConnect

logger = logging.getLogger(__name__)

class UpdateManager:
    """
    Inserts data from a CSV file into layers in the Metagis database.
    """
    def __init__(self, db_query: ('DBQuery'), db_insert: ('DBInsert'), df: pd.DataFrame):
        """
        :param db_query: DatabaseQuery object
        :param db_insert: DatabaseInsert object
        """
        self.db_query = db_query
        self.db_insert = db_insert
        self.df = df

    def update_variable_family(self, metagis_id, new_value):
        old_value = self.db_query.get_layer_themes(metagis_id)[0][0]
        updated_value = self.db_insert.insert_subtheme(metagis_id, new_value)
        return ('subtheme', old_value, updated_value)

    def update_collection(self, metagis_id, new_value):
        old_value = self.db_query.get_layer_themes(metagis_id)[0][1]
        updated_value = self.db_insert.insert_subsubtheme(metagis_id, new_value)
        return ('subsubtheme', old_value, updated_value)

    def update_title(self, metagis_id, new_value):
        old_value = self.db_query.get_layer_themes(metagis_id)[0][2]
        updated_value = self.db_insert.insert_title(metagis_id, new_value)
        return ('title', old_value, updated_value)

    def update_download_url(self, metagis_id, new_value):
        old_value = self.db_query.get_layer_download_url(metagis_id)
        updated_value = self.db_insert.insert_download_url(metagis_id, new_value)
        return ('download_url', old_value, updated_value)

    def update_data_product(self, metagis_id, new_value):
        old_value = self.db_query.get_layer_data_product(metagis_id)
        updated_value = self.db_insert.insert_data_product(self.db_query, metagis_id, new_value)
        return ('data_product', old_value, updated_value)

    def update_attributes(self, metagis_id: int, updated_layer_attributes_df: pd.DataFrame):
        old_arco_variables = [row[0] for row in self.db_query.get_layer_arco_variables(metagis_id)]
        updated_native_arco_pairs = self.db_insert.insert_attributes(self.db_query, metagis_id, updated_layer_attributes_df)
        updates = []
        for idx, (native_var, new_arco_var) in enumerate(updated_native_arco_pairs):
            old_arco_var = old_arco_variables[idx] if idx < len(old_arco_variables) else ''
            updates.append(('native_variable', native_var, native_var))
            updates.append(('arco_variable', old_arco_var, new_arco_var))
        return updates

    def update_layers(self, df: pd.DataFrame):
        updated_layers= []

        for _, row in df.iterrows():
            metagis_id = row['id']
            updates = {}
            if metagis_id not in updates:
                updates[metagis_id] = {'id': metagis_id}

            if 'edito_variable_family' in row and pd.notna(row['edito_variable_family']):
                update_type, old_value, new_value = self.update_variable_family(metagis_id, row['edito_variable_family'])
                updates[metagis_id][f'{update_type}_old'] = old_value
                updates[metagis_id][f'{update_type}_new'] = new_value

            if 'edito_collection' in row and pd.notna(row['edito_collection']):
                update_type, old_value, new_value = self.update_collection(metagis_id, row['edito_collection'])
                updates[metagis_id][f'{update_type}_old'] = old_value
                updates[metagis_id][f'{update_type}_new'] = new_value

            if 'collection_item' in row and pd.notna(row['collection_item']):
                update_type, old_value, new_value = self.update_title(metagis_id, row['collection_item'])
                updates[metagis_id][f'{update_type}_old'] = old_value
                updates[metagis_id][f'{update_type}_new'] = new_value

            if 'download_url' in row and pd.notna(row['download_url']):
                update_type, old_value, new_value = self.update_download_url(metagis_id, row['download_url'])
                updates[metagis_id][f'{update_type}_old'] = old_value
                updates[metagis_id][f'{update_type}_new'] = new_value

            if 'data_product' in row and 'native_variable' in row and 'arco_variable' in row:
                if pd.notna(row['data_product']) and pd.notna(row['native_variable']) and pd.notna(row['arco_variable']):
                    update_type, old_value, new_value = self.update_data_product(metagis_id, row['data_product'])
                    updates[metagis_id][f'{update_type}_old'] = old_value
                    updates[metagis_id][f'{update_type}_new'] = new_value
                    attribute_updates = self.update_attributes(metagis_id, df[df['id'] == metagis_id])
                    for attr_update_type, old_attr_value, new_attr_value in attribute_updates:
                        updates[metagis_id][f'{attr_update_type}_old'] = old_attr_value
                        updates[metagis_id][f'{attr_update_type}_new'] = new_attr_value
                else:
                    logger.warning(f"Skipping update for layer {metagis_id} due to missing data_product, native_variable, or arco_variable.")
            updated_layers.append(updates[metagis_id])
        return updated_layers

def run_update(csv_file: str, db_config: dict):
    """
    Main function to update Metagis layers from a CSV file.

    :param csv_file: Path to CSV file containing updates
    :type csv_file: str
    :param db_config: Dictionary containing database connection information
    :type db_config: dict
    """
    wkdir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(wkdir, "../../data")
    
    db_connection = DBConnect(db_config)
    db_connection.obtain_kerberos_ticket(db_config['kerbuser'], db_config['kerbpwd'])
    db_connection.connect(db_config['server'], db_config['database'], db_config['mguser'], db_config['mgpwd'])
    print("Connected to database")

    db_query = DBQuery(db_connection)
    db_insert = DBInsert(db_connection)
    try:
        df = pd.read_csv(csv_file)

        # Check for required columns
        if 'id' not in df.columns or df['id'].isnull().any():
            raise ValueError("CSV file must contain an 'id' column with values.")
        if 'thematic_lot' not in df.columns or df['thematic_lot'].isnull().any():
            raise ValueError("CSV file must contain a 'thematic_lot' column with strings.")

        manager = UpdateManager(db_query, db_insert, df)
        updates = manager.update_layers(df)

        # Save updates to CSV
        updates_df = pd.DataFrame(updates)
        updated_csv_path = os.path.join(data_dir, f"metagis_db_updates_{date.today().strftime('%Y-%m-%d')}.csv")
        updates_df.to_csv(updated_csv_path, index=False)
        print(f"Database updates saved to {updated_csv_path}")
    finally:
        db_connection.close()

# Database configuration
db_config = {
    'server': None,
    'database': None,
    'kerbuser': None,
    'kerbpwd': None,
    'mguser': None,
    'mgpwd': None
}

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Update Metagis Layers")
    parser.add_argument('--csv_file', help="CSV file to process: ex data/updated_attributes.csv, data/updated_transformation_table.csv")
    args = parser.parse_args()
    if not os.path.exists(args.csv_file):
        raise FileNotFoundError(f"File not found: {args.csv_file}")
    
    if not all(db_config.values()):
        db_config['server'] = input("Enter Metagis server: ")
        db_config['database'] = input("Enter Metagis database: ")
        db_config['username'] = input("Enter Metagis username: ")
        db_config['password'] = input("Enter Metagis password: ")
        db_config['kerbuser'] = input("Enter Kerberos username: ")
        db_config['kerbpwd'] = input("Enter Kerberos password: ")
        run_update(args.csv_file, db_config)
    else:
        run_update(args.csv_file, db_config)