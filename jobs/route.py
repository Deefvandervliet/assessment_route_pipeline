"""
route.py
~~~~~~~~~~
This module contains the batch and streaming ETL for handling route information
see https://github.com/jpatokal/openflights/blob/master/data/routes.dat

Dataset owner: Team x
Raw data ingested by: team x
Code owner: team x
Code creator(s): David van der Vliet
Last edit: 19-12-2022
"""

import argparse


def main(args=None):
    """
    Runs the ETL process for routes.
    :return: None
    """
    parser = argparse.ArgumentParser()
    # parser.add_argument("--config_file", help="configuration file", required=True)
    parser.add_argument("--scenario", help="all the diffrent streaming scenarios", required=False, default=False)
    options, args2 = parser.parse_known_args()

    config_file: str = options.scenario
    application_name: str = "etl_job_routes"

    print("hello world")


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
