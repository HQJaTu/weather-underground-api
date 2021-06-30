#!/usr/bin/env python3

# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

import os
import sys
import argparse
import logging
from datetime import datetime, date, timedelta
import numpy
import csv
from google.cloud import bigquery  # pip3 install google-cloud-bigquery
from google.cloud.bigquery import SchemaField, TimePartitioningType
from fmiopendata.wfs import download_stored_query

log = logging.getLogger(__name__)

# Data types: https://cloud.google.com/bigquery/docs/schemas
# Mode: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableFieldSchema.FIELDS.mode
# Possible values include NULLABLE, REQUIRED and REPEATED.
weather_table_name = 'fmi_weather_data'
weather_table_schema = [
    SchemaField('observation_time', 'TIMESTAMP', mode='required'),
    SchemaField('stationid', 'STRING', mode='required'),
    SchemaField('humidityPerc', 'NUMERIC', mode='required'),
    SchemaField('tempC', 'NUMERIC', mode='required'),
    SchemaField('dewPointC', 'NUMERIC', mode='required'),
    SchemaField('windDir', 'INT64', mode='nullable'),
    SchemaField('windspeed', 'NUMERIC', mode='required'),
    SchemaField('windgust', 'NUMERIC', mode='required'),
    SchemaField('pressureMbar', 'NUMERIC', mode='required'),
    SchemaField('precipRateMM', 'NUMERIC', mode='nullable'),
    SchemaField('precipIntensityMMpH', 'NUMERIC', mode='nullable'),
    SchemaField('snowDepthCM', 'NUMERIC', mode='nullable'),
]

airquality_table_name = 'fmi_airquality_data'
airquality_table_schema = [
    SchemaField('observation_time', 'TIMESTAMP', mode='required'),
    SchemaField('stationid', 'STRING', mode='required'),
    SchemaField('sulphurDioxide', 'NUMERIC', mode='nullable'),
    SchemaField('nitrogenMonoxide', 'NUMERIC', mode='nullable'),
    SchemaField('nitrogenDioxide', 'NUMERIC', mode='nullable'),
    SchemaField('ozone', 'NUMERIC', mode='nullable'),
    SchemaField('odorousSulphurCompounds', 'NUMERIC', mode='nullable'),
    SchemaField('carbonMonoxide', 'NUMERIC', mode='nullable'),
    SchemaField('particulateMatter10um', 'NUMERIC', mode='required'),
    SchemaField('particulateMatter2_5um', 'NUMERIC', mode='nullable'),
    SchemaField('airQualityIndex', 'NUMERIC', mode='nullable'),
    SchemaField('mustaHiiliPM2_5', 'NUMERIC', mode='nullable'),
]


def _setup_logger():
    log_formatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(log_formatter)
    console_handler.propagate = False
    log.addHandler(console_handler)
    log.setLevel(logging.DEBUG)

    lib_log = logging.getLogger('libwunderground')
    lib_log.setLevel(logging.DEBUG)


def init_bigquery(credentials_file: str, dataset_id: str):
    log.debug("Instantiate GCP BigQuery client")
    client = bigquery.Client.from_service_account_json(credentials_file)
    project = client.project

    # Get a handle into dataset. Create if not exists.
    dataset = bigquery.Dataset("%s.%s" % (project, dataset_id))
    # List of valid locations: https://cloud.google.com/bigquery/docs/locations
    dataset.location = "europe-north1"  # Hamina, Finland
    dataset.description = "Weather Underground records"
    log.debug("Confirm BigQuery dataset '%s' existence" % dataset_id)
    dataset = client.create_dataset(dataset, exists_ok=True)  # Make an API request.

    log.debug("Confirm BigQuery table '%s'.'%s' existence" % (dataset_id, weather_table_name))
    weather_table_ref = dataset.table(weather_table_name)
    weather_table = bigquery.Table(weather_table_ref, schema=weather_table_schema)
    if False:
        # NOTE: This really doesn't work with free tier. Un-partitioned working ok.
        # Cannot load any data with Free tier
        weather_table.time_partitioning = bigquery.table.TimePartitioning(type_=TimePartitioningType.DAY,
                                                                          # TimePartitioningType.MONTH
                                                                          field='observation_time')
    weather_table = client.create_table(weather_table, exists_ok=True)

    airquality_table_ref = dataset.table(airquality_table_name)
    airquality_table = bigquery.Table(airquality_table_ref, schema=airquality_table_schema)
    airquality_table = client.create_table(airquality_table, exists_ok=True)

    return client, dataset, weather_table, airquality_table


def get_most_recent_record(bigquery_client: bigquery.Client, bigquery_dataset) -> date:
    query = """SELECT MAX(observation_time)
FROM `%s`""" % weather_table_name

    job_config = bigquery.QueryJobConfig()
    job_config.default_dataset = bigquery_dataset
    job = bigquery_client.query(query, job_config=job_config)
    result = job.result()

    if result.total_rows != 1:
        return None

    page = next(result.pages)
    row = next(page)
    if not row or row[0] is None:
        return None

    most_recent = row[0].date()

    return most_recent


def loop_weather_observation_days(start_date: date, bigquery_client: bigquery.Client, bigquery_dataset,
                                  table: bigquery.Table):
    # Expect location to be: 'Lappeenranta lentoasema'
    # 61.037706067804294, 28.120219550719806
    # 61.05607120921371, 28.17515118922236
    lpr_lentoasema = (28.12, 61.04, 28.175, 61.06)

    date_to_get = start_date
    log.info("Start looping from date: %s" % date_to_get)
    now_date = datetime.utcnow().date()
    required_columns = [idx for idx, field in enumerate(weather_table_schema) if field.mode.upper() == 'REQUIRED']
    previous_filename = None
    previous_partition = None
    partition_format = "%Y%m"

    while date_to_get < now_date:
        log.info("Get date: %s" % date_to_get)
        # FMI: No more than 168.000000 hours allowed.
        date_to_get_begin = datetime(date_to_get.year, date_to_get.month, date_to_get.day, 0, 0, 0)
        date_to_get_end = datetime(date_to_get.year, date_to_get.month, date_to_get.day, 23, 59, 59)

        obs = download_stored_query("fmi::observations::weather::multipointcoverage",
                                    args=["bbox=%f,%f,%f,%f" % lpr_lentoasema,
                                          "starttime=%sZ" % date_to_get_begin.isoformat(timespec="seconds"),
                                          "endtime=%sZ" % date_to_get_end.isoformat(timespec="seconds")])
        if not obs.data:
            log.error("No data for %s! Skipping day." % date_to_get)
            date_to_get += timedelta(days=1)
            continue

        latest_tstep = max(obs.data.keys())
        point_name = list(obs.data[latest_tstep].keys())[0]

        filename = "fmi-%s.csv" % date_to_get.strftime(partition_format)
        if previous_filename and previous_filename != filename:
            load_data(previous_filename, previous_partition.strftime(partition_format), bigquery_client, table)

        previous_filename = filename
        # previous_partition = date(obs_time.year, obs_time.month, obs_time.day)
        previous_partition = date(date_to_get.year, date_to_get.month, 1)
        if not os.path.exists(filename):
            do_header = True
        else:
            do_header = False

        if obs.data:
            with open(filename, 'a', newline='') as csvfile:
                data_writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
                if do_header:
                    data_writer.writerow([field.name for field in weather_table_schema])

                for measurement_ts in obs.data:
                    for measurement in obs.data[measurement_ts][point_name]:
                        if numpy.isnan(obs.data[measurement_ts][point_name][measurement]['value']):
                            obs.data[measurement_ts][point_name][measurement]['value'] = None
                        elif measurement == 'Wind direction':
                            obs.data[measurement_ts][point_name][measurement]['value'] = int(
                                obs.data[measurement_ts][point_name][measurement]['value'])
                        if False:
                            # Output all the measurements for debugging purposes. Very noisy!
                            log.debug("%s: %s = %s" % (
                                measurement_ts, measurement, obs.data[measurement_ts][point_name][measurement]))

                    record_out = (
                        measurement_ts,  # observation_time
                        point_name,  # stationid
                        obs.data[measurement_ts][point_name]['Relative humidity']['value'],  # humidityPerc
                        obs.data[measurement_ts][point_name]['Air temperature']['value'],  # tempC
                        obs.data[measurement_ts][point_name]['Dew-point temperature']['value'],  # dewPointC
                        obs.data[measurement_ts][point_name]['Wind direction']['value'],  # windDir
                        obs.data[measurement_ts][point_name]['Wind speed']['value'],  # windspeed
                        obs.data[measurement_ts][point_name]['Gust speed']['value'],  # windgust
                        obs.data[measurement_ts][point_name]['Pressure (msl)']['value'],  # pressureMbar
                        obs.data[measurement_ts][point_name]['Precipitation amount']['value'],  # precipRateMM
                        obs.data[measurement_ts][point_name]['Precipitation intensity']['value'],  # precipIntensityMMpH
                        obs.data[measurement_ts][point_name]['Snow depth']['value'],  # snowDepthCM
                    )
                    first_missing_column = None
                    for idx in required_columns:
                        if record_out[idx] is None or record_out[idx] == '':
                            first_missing_column = idx
                            break
                    if first_missing_column:
                        log.warning(
                            "Skipping record for %s, not all required fields present. First missing data: %d) %s" % (
                                measurement_ts, first_missing_column, weather_table_schema[first_missing_column].name))
                        continue

                    data_writer.writerow(record_out)

        date_to_get += timedelta(days=1)

    log.info("Done looping.")


def loop_airquality_observation_days(start_date: date, bigquery_client: bigquery.Client, bigquery_dataset,
                                     table: bigquery.Table):
    # WFS stored query: https://www.ilmatieteenlaitos.fi/tallennetut-kyselyt
    # Expect location to be: 'Ihalainen, mine'
    # Broad Lappeenranta area:
    # bottom-left: 60.99828519705738, 28.075474817358778
    # top-right  : 61.17181001300627, 28.53396038629801
    # Southern-Finland:
    # bottom-left: 60.24903142585997, 26.08497167407856
    # top-right  : 61.73855814776994, 29.312786321552778
    # fmi::observations::airquality::hourly::multipointcoverage
    # - 'Virolahti Koivuniemi Ääpälä'
    # urban::observations::airquality::hourly::multipointcoverage:
    # - 'Lappeenranta Ihalainen',
    # - 'Lappeenranta Joutsenon keskusta',
    # - 'Lappeenranta keskusta 4',
    # - 'Lappeenranta Lauritsala',
    # - 'Lappeenranta Pulp',
    # - 'Lappeenranta Tirilä Pekkasenkatu',
    bounding_box = (28.08, 60.99, 28.53, 61.17)

    date_to_get = start_date
    log.info("Start looping from date: %s" % date_to_get)
    now_date = datetime.utcnow().date()
    end_date = datetime(1900, 1, 1).date()
    required_columns = [idx for idx, field in enumerate(airquality_table_schema) if field.mode.upper() == 'REQUIRED']
    previous_filename = None
    previous_partition = None
    partition_format = "%Y%m"

    while date_to_get > end_date:
        log.info("Get date: %s" % date_to_get)
        # FMI: No more than 168.000000 hours allowed.
        date_to_get_begin = datetime(date_to_get.year, date_to_get.month, date_to_get.day, 0, 0, 0)
        date_to_get_end = datetime(date_to_get.year, date_to_get.month, date_to_get.day, 23, 59, 59)

        obs = download_stored_query("urban::observations::airquality::hourly::multipointcoverage",
                                    args=["bbox=%f,%f,%f,%f" % bounding_box,
                                          "starttime=%sZ" % date_to_get_begin.isoformat(timespec="seconds"),
                                          "endtime=%sZ" % date_to_get_end.isoformat(timespec="seconds")])
        if not obs.data:
            log.error("No data for %s! Skipping day." % date_to_get)
            date_to_get -= timedelta(days=1)
            continue

        latest_tstep = max(obs.data.keys())
        #point_name = list(obs.data[latest_tstep].keys())[0]

        filename = "fmi-airqual-%s.csv" % date_to_get.strftime(partition_format)
        if previous_filename and previous_filename != filename:
            load_data(previous_filename, previous_partition.strftime(partition_format), bigquery_client, table,
                      airquality_table_schema)

        previous_filename = filename
        # previous_partition = date(obs_time.year, obs_time.month, obs_time.day)
        previous_partition = date(date_to_get.year, date_to_get.month, 1)
        if not os.path.exists(filename):
            do_header = True
        else:
            do_header = False

        if obs.data:
            with open(filename, 'a', newline='') as csvfile:
                data_writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
                if do_header:
                    data_writer.writerow([field.name for field in airquality_table_schema])

                for measurement_ts in obs.data:
                    for point_name in obs.data[measurement_ts]:
                        for measurement in obs.data[measurement_ts][point_name]:
                            if numpy.isnan(obs.data[measurement_ts][point_name][measurement]['value']):
                                obs.data[measurement_ts][point_name][measurement]['value'] = None
                            if False:
                                # Output all the measurements for debugging purposes. Very noisy!
                                log.debug("%s: %s = %s" % (
                                    measurement_ts, measurement, obs.data[measurement_ts][point_name][measurement]))

                        record_out = (
                            measurement_ts,  # observation_time
                            point_name,  # stationid
                            obs.data[measurement_ts][point_name]['Sulphur dioxide']['value'],  # sulphurDioxide [µg/m3]
                            obs.data[measurement_ts][point_name]['Nitrogen monoxide']['value'],  # nitrogenMonoxide [µg/m3]
                            obs.data[measurement_ts][point_name]['Nitrogen dioxide']['value'],  # nitrogenDioxide [µg/m3]
                            obs.data[measurement_ts][point_name]['Ozone']['value'],  # ozone [µg/m3]
                            obs.data[measurement_ts][point_name]['Odorous sulphur compounds']['value'],
                            # odorousSulphurCompounds [µgS/m3]
                            obs.data[measurement_ts][point_name]['Carbon monoxide']['value'],  # carbonMonoxide [µg/m3]
                            obs.data[measurement_ts][point_name]['Particulate matter < 10 µm']['value'],
                            # particulateMatter10um [µg/m3]
                            obs.data[measurement_ts][point_name]['Particulate matter < 2.5 µm']['value'],
                            # particulateMatter2_5um [µg/m3]
                            obs.data[measurement_ts][point_name]['Air Quality Index']['value'],  # airQualityIndex [index]
                            obs.data[measurement_ts][point_name]['musta hiili PM2.5']['value'],  # mustaHiiliPM2_5 [µg/m3]
                        )
                        first_missing_column = None
                        for idx in required_columns:
                            if record_out[idx] is None or record_out[idx] == '':
                                first_missing_column = idx
                                break
                        if first_missing_column:
                            log.warning(
                                "Skipping record for %s (%s), not all required fields present. First missing data: %d) %s" % (
                                    measurement_ts, point_name, first_missing_column,
                                    airquality_table_schema[first_missing_column].name))
                            continue

                        data_writer.writerow(record_out)

        date_to_get -= timedelta(days=1)

    log.info("Done looping.")


def load_data(file_path: str, partition: str, bigquery_client: bigquery.Client, table: bigquery.Table, schema: list):
    """
    NOTE: This really doesn't work with free tier. Un-partitioned working ok.
    https://cloud.google.com/bigquery/docs/reference/rest/v2/Job
    https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.LoadJobConfig.html
    https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.LoadJob.html
    $ bq --location europe-north1 load --skip_leading_rows=1 --source_format=CSV --max_bad_records=0 'PWS_data.weather_data$201501' wunderground-201501.csv
    Upload complete.
    :param schema:
    :param partition:
    :param file_path:
    :param bigquery_client:
    :param table:
    :return:
    """
    job_config = bigquery.LoadJobConfig(
        schema=[{"name": field.name, "type": field.field_type, "mode": field.mode} for field in schema],
        autodetect=False,
        source_format=bigquery.SourceFormat.CSV,
        create_disposition="CREATE_IF_NEEDED",  # CREATE_NEVER
        write_disposition="WRITE_APPEND",  # WRITE_TRUNCATE will clear the table before loading
        field_delimiter=',',
        max_bad_records=0,
        skip_leading_rows=1,
    )

    # dest_table = bigquery_client.get_table("%s.%s.%s" % (bigquery_client.project, table.dataset_id, table.table_id))
    # dest_table = "%s.%s.%s$%s" % (bigquery_client.project, table.dataset_id, table.table_id, partition)
    dest_table = "%s.%s.%s" % (bigquery_client.project, table.dataset_id, table.table_id)
    with open(file_path, "rb") as source_file:
        job = bigquery_client.load_table_from_file(source_file, dest_table, job_config=job_config)

    result = job.result()  # Waits for the job to complete.
    # result = google.cloud.bigquery.job.load.LoadJob object
    out_table = bigquery_client.get_table("%s.%s.%s" % (
        bigquery_client.project, table.dataset_id, result.destination.table_id
    ))
    # log.debug("Job ended at:" % result.ended)
    log.info("Loaded %s rows, %d errors" % (result.output_rows, len(job.errors) if job.errors else 0))
    # log.info("%s now has %d rows" % (table.table_id, table.num_rows))
    if job.errors:
        log.error("Load errors:")
        for error in job.errors:
            log.error(error)
        log.error("Won't continue loading any further.")
        exit(1)

    log.info("Done loading. Table %s.%s now has %d rows" % (table.dataset_id, out_table.table_id, out_table.num_rows))


def main():
    parser = argparse.ArgumentParser(description='OpenDNSSEC BIND slave zone configurator')
    parser.add_argument('--bigquery-json-credentials',
                        metavar='GOOGLE-JSON-CREDENTIALS-FILE', required=True,
                        help='Mandatory. JSON-file with Google BigQuery API Service Account credentials.')
    parser.add_argument('--bigquery-dataset-id',
                        metavar='GOOGLE-BIGQUERY-DATASET-ID', required=True,
                        help='Mandatory. BigQuery dataset ID to store data into.')
    parser.add_argument('--load-csv-file', metavar="FILENAME",
                        help="Do a BigQuery CSV-load from given file")
    args = parser.parse_args()

    _setup_logger()
    log.info("Begin")

    # Init Google BigQuery
    bq_client, bq_dataset, bq_weather_table, bq_aq_table = init_bigquery(args.bigquery_json_credentials,
                                                                         args.bigquery_dataset_id)

    if False:
        # CSV-load?
        if args.load_csv_file:
            log.info("Loading CSV-file %s into BigQuery" % args.load_csv_file)
            load_data(args.load_csv_file, "", bq_client, bq_weather_table)
            log.info("Done loading CSV-file")
            exit(0)

        # Go import PWS data
        most_recent_date = get_most_recent_record(bq_client, bq_dataset)
        if most_recent_date:
            most_recent_date += timedelta(days=1)
        else:
            most_recent_date = date(2015, 1, 19)

        loop_weather_observation_days(most_recent_date, bq_client, bq_dataset, bq_weather_table)

    if True:
        # CSV-load?
        if args.load_csv_file:
            log.info("Loading CSV-file %s into BigQuery" % args.load_csv_file)
            load_data(args.load_csv_file, "", bq_client, bq_aq_table)
            log.info("Done loading CSV-file")
            exit(0)

        # Go import air quality data
        most_recent_date = datetime.utcnow().date()
        loop_airquality_observation_days(most_recent_date, bq_client, bq_dataset, bq_aq_table)

    log.info("Done.")


if __name__ == '__main__':
    main()
