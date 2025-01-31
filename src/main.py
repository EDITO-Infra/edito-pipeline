import os
from metagisconfig2stac.utils.core import CoreUtils
import argparse
wkdir = os.path.abspath(os.path.dirname(__file__))
logger = CoreUtils.get_logger(append_logs=False)
datadir = os.path.abspath(os.path.join(wkdir, '..', 'data'))


def main():
    """
    Main function to run the pipeline.  Uses the configuration 'config.yaml' in the data directory
    by default.  To change the configuration, change the 'config.yaml' file in the data directory.
    See the README for more information on the configuration file.

    :return:
    """
    # Validate and load the configuration
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='Path to the configuration file')
    args = parser.parse_args()
    if args.config:
        config_yaml = args.config
    else:
        config_yaml = input("Enter the path to the configuration file: ")
    config = CoreUtils.load_yaml(f"{datadir}/{config_yaml}")
    config = CoreUtils.validate_config(config)
    # Log the validated configuration
    logger.info("Configuration loaded and validated successfully.")

    from metagisconfig2stac.pipeline import Pipeline
    pipeline = Pipeline(config)

    #pipeline.finish_previous_layer_catalog(config['layer_catalog_file'])
    pipeline.run_full_pipeline()
    

if __name__ == '__main__':
    main()
