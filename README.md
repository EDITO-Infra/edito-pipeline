# Metagis EDITO Pipeline

## Overview

This project involves the processing of each Layer for a given Layer Collection from Metagis into a STAC Catalog that is compatible with the EDITO STAC Catalog.  It has been desigend foremost for the EDITO Layer Collection, but can process other Layer Collections such as Bio-Oracle.  The pipeline reforms the metadata that comes from metagis, or attempts to retrieve it from elsewhere, to make items sufficient for the EDITO STAC Catalog.  Where necessary(configured in Metagis) data products from given layers in the Layer Collection can also be converted into an ARCO format and these features will also be added to the STAC Catalog. 

First create a LayerCatalog, composed of the Layers from a Layer Collection.  Then for each Layer obtain at least one native data product, and the necessary metadata to make a 'native STAC' feature. The subtheme(variable family), subsubtheme(collection) and dataset title(item) govern how this STAC feature fits in the STAC catalog.  For Layers where 'attributes' have been configured in Metagis, at least one native data product can be converted into an ARCO format (.zarr or .parquet) and ARCO assets can be added to 'ARCO STAC' features.  These features use the same subtheme (variable family), but then the ARCO attributes determine which collection the item(s) fall into, and the spatial and temporal bounds to make the STAC item add the feature to the STAC catalog. 

## File Descriptions

### Main Scripts

- **main.py**: Entry point for running the pipeline.
- **pipeline.py**: Manages the overall pipeline process, including creating the layer catalog, processing layers, and generating pipeline results.
- **layer_catalog.py**: Manages the layer catalog, including loading and updating layer metadata.
- **layer.py**: Defines the `Layer` class, which handles individual layer processing.
- **pipeline_results.py**: Manages the results of the pipeline, including logging and reporting.
- **resto.py**: Handles posting STAC data to a specified Resto instance.
- **stac.py**: Builds a local STAC using the updated layers from the Metagis database.
- **arco_converter.py**: Manages the conversion of native assets to ARCO formats, and uploading to s3.

### ARCO Conversion Scripts

The ARCO conversion process involves several scripts that handle the conversion of native assets to various ARCO formats. Below are the descriptions of these scripts:

- **arco_conversion/nc_to_zarr.py**: Converts NetCDF files to Zarr format.
- **arco_conversion/tif_to_zarr.py**: Converts TIFF files to Zarr format.
- **arco_conversion/csv_to_parquet.py**: Converts CSV files to Parquet format.
- **arco_conversion/parquet_to_zarr.py**: Converts Parquet files to Zarr format.
- **arco_conversion/shp_to_parquet.py**: Converts Shapefile (SHP) files to Parquet format.
- **arco_conversion/gdb_to_parquet.py**: Converts Geodatabase (GDB) files to Parquet format.
- **arco_conversion/arco_conv_utils.py**: Contains utility functions for ARCO conversion, including uploading converted assets to S3 and updating layer metadata.

### Utility Scripts

These scripts provide various utility functions used throughout the pipeline:

- **utils/s3.py**: Contains functions for interacting with S3, including uploading, downloading, and managing S3 objects.
- **utils/core.py**: Provides core utility functions such as logging configuration, HTTP session management, and file handling.

#### data/layer_collection_config/ 
  Directory to store the layer collection dictionaries downloaded from a config endpoint (ex. https://emodnet.ec.europa.eu/geoviewer/edito.php)
  These can be loaded into the pipeline if using outside VPN(ex. on the datalab)

#### data/layer_catalogs/
  Directory to store previously processed layer catalogs. Stored by "<layercollection><date><start/progress/end>.json"
  Can be used when running the pipeline on a layer collection to re-use previously transferred and/or converted datasets that are still valid.

#### Using Config Files

The configuration files are in `.yaml` format and stored in the `data` directory. They contain key-value pairs to configure the pipeline. Below is a description of the required and optional configuration parameters.

##### Minimum Configuration

- **layer_collection**: Name of the layer collection (e.g., 'edito').
- **layer_collection_config**: Location of the layer collection config (e.g., 'https://emodnet.ec.europa.eu/geoviewer/edito.php').
- **stac_title**: Title of the local STAC catalog.
- **stac_s3**: Location on the S3 bucket where the STAC is uploaded.

##### Optional Configuration
- **previous_layer_catalog**: Local json file containing previously processed layer catalog
- **select**: Dictionary containing attributes and values to select specific layers. Default is an empty dictionary.
- **transfer_native_to_s3**: Boolean indicating whether to transfer native assets to S3. Default is `False`.
- **convert_arco**: Boolean indicating whether to convert native assets to ARCO formats. Default is `False`.
- **create_backup_stac**: Boolean indicating whether to create a backup of the STAC catalog. Default is `False`.
- **stac_s3_backup**: Location on the S3 bucket where the STAC backup is uploaded. Default is an empty string.
- **cp_transformation_table**: Boolean indicating whether to create a transformation table for the Central Portal. Default is `False`.
- **central_portal_php_config**: Location of the Central Portal PHP configuration file. Default is an empty string.
- **attributes_table**: Boolean indicating whether to create an attributes table. Default is `False`.
- **attribute_s3**: Location on the S3 bucket where the attributes table is uploaded. Default is an empty string.
- **push_to_resto**: Boolean indicating whether to push the STAC data to a Resto instance. Default is `False`.
- **resto_instance**: URL of the Resto instance. Default is an empty string.

##### Example Configuration File

```yaml
layer_collection: 'edito'
layer_collection_config: 'https://emodnet.ec.europa.eu/geoviewer/edito.php'
stac_title: 'EDITO STAC Catalog'
stac_s3: 's3://my-bucket/stac'
previous_layer_catalog: layer_catalogs/edito_2025_end.json
select:
  - id:
    - 13662
transfer_native_to_s3: true
convert_arco: true
create_backup_stac: true
stac_s3_backup: 's3://my-bucket/stac-backup'
cp_transformation_table: true
central_portal_php_config: 'path/to/php/config'
attributes_table: true
attribute_s3: 's3://my-bucket/attributes'
push_to_resto: true
resto_instance: 'https://resto-instance-url'
```

#### Using previous layer catalogs

These are JSON files of previous states of processed layer catalogs. They are updated versions of the layer catalog.  They can have temporal_extent, geographical_extent, native_assets(the s3 transferred assets), WMS assets, ARCO assets, etc.  In short, its a quick reference to rebuild a processed layer catalog.  If you specify layer ids in 'layers_to_update', if there is a previous state file, those layers will be removed from the loaded previous layer catalog.  Thus that layer will be reprocessed when the pipeline is run, but all other layers in the previous state file will be used and prevent unnecessary reprocessing.  Important to note that existence of a layer in a previous state does not mean data from the previous state will be used.  Certain traits between both the current state and previous state must be equal in order for certain info to be reused.  For example if a download url has changed from the current state with the previous state, the native_asset on the s3 bucket can't be reused since the data product has possibly been changed/updated.


## Run Metagisconfig2stac

Specify the path to the config file
Execute the `main.py` script with the chosen config yaml as an argument.  

```bash
cd src
python main.py --config='../data/config-myconfig.yaml'
```

See example config files in the data directory

config-biooracle.yaml : Run Bio Oracle layer collection through pipeline, no native transfer, upload STAC to s3, don't ingest in resto

config-devsbh.yaml : Run EDITO dev layer collection through pipeline, target only EMODnet Seabed Habitats,
    upload to 'dev_stac_sbh' on s3, don't ingest in resto.

config-edito.yaml: Run EDITO layer collection, use previous layer catalog, transfer native, convert ARCO, upload to 'stac' on s3, ingest in resto, make transformation table, make attributes table. 

If unsure about what a given configuration key pair does, look in the scripts for pipeline_config['key'] to see what effect it has. 

## Run update_metagis.py

To update Metagis layers from a CSV file, use the `mg_update.py` script. Follow these steps:

1. Fill in the database credentials at the end of mg_update.py in 'db_config' or be ready to input them when prompted.
2. Prepare your CSV file with the necessary columns (`id`, `thematic_lot`, etc.).
3. Run the script with the path to your CSV file as an argument:

```sh
cd src/update_metagis_layers
python mg_update.py --csv_file="path/to/your/csv_file.csv"
```

### Running the ERDDAP to Metagis Script

To create new layers in the Metagis database from an ERDDAP server, use the `erddap_metagis.py` script. Follow these steps:

1. Ensure you have the required database credentials at the end of 'erddap_metagis.py' in 'db_config' or be ready to input them when prompted.
2. Run the script with the ERDDAP URL as an argument:

```sh
cd src/update_metagis_layers
python erddap_metagis.py --erddapurl https://erddap.bio-oracle.org/erddap/info/index.json
```

## Requirements

pip install -r requirements.txt

## Credentials needed

  - data/creds/emods3.env:
    - EMOD_ACCESS_KEY
    - EMOD_SECRET_KEY
  - data/creds/resto.env:
    - RESTO_USERNAME
    - RESTO_PASSWORD

To update metagis layers need to be on VLIZ VPN and have Metagis credentials. Fill in credentials in the scripts themselves or when prompted

## License

This project is licensed under the MIT License.

### Contact

samuel.fooks@vliz.be