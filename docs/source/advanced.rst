Advanced
=========

Here is the advanced documentation.


Previous Layer Catalog
======================

The pipeline can be run with a previous layer catalog to update the existing catalog. The previous layer catalog is a 
JSON file containing a previous layer catalog that has been processed by the pipeline. 
The file should be located in the `data/layer_catalogs` directory. The pipeline will process the current layer catalog, but if there is 
metadata from a previous layer catalog that can be used the metadata from the previous layer catalog will be used for the layer.

Ex if the download_url is the same in the previous layer and the layer being processed, and the previous layer has a 'native_asset' on s3,
that native asset will be used for the current layer.

Ex if the attributes of the previous layer are the same as the current layer catalog, and the native_asset is the same,
and there are 'converted_arco_assets' in the previous layer, these will be used for the current layer.

In short, prevent the double transfer of data to s3 and the double conversion of native assets to ARCO formats. Use a 
previous layer catalog if you can.


Using Configuration Files
=========================

The configuration files are in `.yaml` format and stored in the `data` directory. They contain key-value pairs to configure the pipeline. Below is a description of the required and optional configuration parameters.

Minimum Configuration
---------------------

- **layer_collection**: Name of the layer collection (e.g., 'edito').
- **layer_collection_config**: Location of the layer collection config (e.g., 'https://emodnet.ec.europa.eu/geoviewer/edito.php').
- **stac_title**: Title of the local STAC catalog.
- **stac_s3**: Location on the S3 bucket where the STAC is uploaded.

Optional Configuration
----------------------

- **previous_layer_catalog**: Local json file containing previously processed layer catalog
- **thematic_lots_to_process**: List of thematic lots to process. Default is an empty list.
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

Example Configuration File
--------------------------

    .. code-block:: yaml

        layer_collection: edito
        layer_collection_config: https://emodnet.ec.europa.eu/geoviewer/edito.php
        stac_title: EDITO STAC Catalog
        stac_s3: s3://my-bucket/stac
        previous_layer_catalog: layer_catalogs/edito_2025_end.json
        select:
            thematic_lot:
                - EMODnet Seabed Habitats
        transfer_native_to_s3: True
        convert_arco: True
        create_backup_stac: True
        stac_s3_backup: s3://my-bucket/stac-backup
        cp_transformation_table: True
        central_portal_php_config: https://emodnet.ec.europa.eu/geoviewer/config.php
        attributes_table: True
        attribute_s3: s3://my-bucket/attributes
        push_to_resto: True
        resto_instance: https://resto-instance-url


Running the Pipeline with a Configuration File
----------------------------------------------

To run the pipeline with a configuration file that targets the EDITO layer collection, transfers native assets to s3, converts 
native assets to an ARCO format where possible, creates a local STAC catalog, pushes that STAC to s3, 
creates a backup of an existing STAC, creates a transformation table for the Central Portal, 
creates an attributes table, and pushes the STAC to a Resto instance. 

Standard edito configuration file is located in the `data` directory.

    .. code-block:: bash

        python main.py --config ../data/config-edito.yaml

Running the Pipeline using BioOracle layer colection
----------------------------------------------------------------------

To run the pipeline with a configuration file that targets the BioOracle layer collection, does not transfer native assets to s3,
does convert native assets to an ARCO format where possible, creates a local STAC catalog, pushes the local STAC to s3,
does not create a backup of an existing STAC, does not create a transformation table for the Central Portal, 
does not create an attributes table, and does not push the STAC to a Resto instance.

    .. code-block:: bash

        python main.py --config ../data/config-biooracle.yaml


Running the Pipeline using EDITO on metagis_dev
----------------------------------------------------------------------

To run the pipeline with a configuration file that targets the EDITO layer collection from metagis_dev, keeps only layers from the 
thematic lot EMODnet Seabed Habitats, transfers native assets to s3, converts native assets to an ARCO format where possible,
builds a local STAC catalog, pushes the local STAC to s3, creates a backup of an existing STAC, creates a transformation table for the Central Portal,
creates an attributes table, but does not push the STAC to a Resto instance.

    .. code-block:: bash

        python main.py --config ../data/config-devsbh.yaml


Selecting layers
================

In the configuration file, you can select specific layers with by adding 'select' to the layer configuration.
Then under select, you add the attribute of the layer that you want to target and under that the value(s) that attribute may have
to be selected.

Ex if you want to select layers that have the attribute 'thematic_lot' with the value 'EMODnet Seabed Habitats' and 
the layer 'id' is 100000, you would add the following to the configuration file:

    .. code-block:: yaml

        layer_collection: edito
        layer_collection_config: https://emodnet.ec.europa.eu/geoviewer/edito.php
        stac_title: EDITO STAC Catalog
        stac_s3: s3://my-bucket/stac
        select:
            thematic_lot:
                - EMODnet Seabed Habitats
            id:
                - 100000