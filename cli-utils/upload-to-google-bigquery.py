#!/usr/bin/env python3

# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

import os
import sys
import argparse
import logging
from datetime import datetime, date, timedelta
import csv
from google.cloud import bigquery  # pip3 install google-cloud-bigquery
from google.cloud.bigquery import SchemaField, TimePartitioningType
from libwunderground import PWS, Units

log = logging.getLogger(__name__)

# Data types: https://cloud.google.com/bigquery/docs/schemas
# Mode: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableFieldSchema.FIELDS.mode
# Possible values include NULLABLE, REQUIRED and REPEATED.
table_name = 'pws_weather_data'
table_schema = [
    SchemaField('observation_time', 'TIMESTAMP', mode='required'),
    SchemaField('stationid', 'STRING', mode='required'),
    SchemaField('humidityPercHigh', 'INT64', mode='required'),
    SchemaField('humidityPercLow', 'INT64', mode='required'),
    SchemaField('humidityPercAvg', 'INT64', mode='required'),
    SchemaField('tempHighC', 'INT64', mode='nullable'),
    SchemaField('tempLowC', 'INT64', mode='nullable'),
    SchemaField('tempAvgC', 'INT64', mode='nullable'),
    SchemaField('dewPointHighC', 'INT64', mode='nullable'),
    SchemaField('dewPointLowC', 'INT64', mode='nullable'),
    SchemaField('dewPointAvgC', 'INT64', mode='nullable'),
    SchemaField('winddirAvg', 'INT64', mode='nullable'),
    SchemaField('windspeedHigh', 'INT64', mode='required'),
    SchemaField('windspeedLow', 'INT64', mode='required'),
    SchemaField('windspeedAvg', 'INT64', mode='required'),
    SchemaField('windgustHigh', 'INT64', mode='required'),
    SchemaField('windgustLow', 'INT64', mode='required'),
    SchemaField('windgustAvg', 'INT64', mode='required'),
    SchemaField('pressureMaxMbar', 'NUMERIC', mode='required'),
    SchemaField('pressureMinMbar', 'NUMERIC', mode='required'),
    SchemaField('precipRateMM', 'NUMERIC', mode='required'),
    SchemaField('precipTotalMM', 'NUMERIC', mode='required'),
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

    log.debug("Confirm BigQuery table '%s'.'%s' existence" % (dataset_id, table_name))
    table_ref = dataset.table(table_name)
    table = bigquery.Table(table_ref, schema=table_schema)
    if False:
        # NOTE: This really doesn't work with free tier. Un-partitioned working ok.
        # Cannot load any data with Free tier
        table.time_partitioning = bigquery.table.TimePartitioning(type_=TimePartitioningType.DAY,
                                                                  # TimePartitioningType.MONTH
                                                                  field='observation_time')
    table = client.create_table(table, exists_ok=True)

    return client, dataset, table


def get_most_recent_record(bigquery_client: bigquery.Client, bigquery_dataset) -> date:
    query = """SELECT MAX(observation_time)
FROM `%s`""" % table_name

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


def loop_days(pws_client, start_date: date, bigquery_client: bigquery.Client, bigquery_dataset, table: bigquery.Table):
    date_to_get = start_date
    log.info("Start looping from date: %s" % date_to_get)
    now_date = datetime.utcnow().date()
    required_columns = [idx for idx, field in enumerate(table_schema) if field.mode.upper() == 'REQUIRED']
    previous_filename = None
    previous_partition = None
    # partition_format = "%Y%m%d"
    partition_format = "%Y%m"
    while date_to_get < now_date:
        log.info("Get date: %s" % date_to_get)
        records = pws_client.get_historical_data(date_to_get)
        if records:
            for record in records:
                if record['epoch'] > 9999999999:
                    obs_time = datetime.utcfromtimestamp(record['epoch'] / 1000)
                else:
                    obs_time = datetime.utcfromtimestamp(record['epoch'])
                # Time-zone difference. UTC-time can be prev/next of requested day.
                # Also there are cases where a record arrives so, it gets timestamped couple seconds on next day.
                allowed_dates = [date_to_get, date_to_get - timedelta(days=1), date_to_get + timedelta(days=1)]
                if obs_time.date() not in allowed_dates:
                    raise RuntimeError("Whaat! Expecting data for day %s, but got %s (%s)!" % (
                        date_to_get, obs_time, record['epoch']))

                record_out = (
                    obs_time,  # observation_time
                    record['stationID'],  # stationid
                    record['humidityHigh'],  # humidityPercHigh
                    record['humidityLow'],  # humidityPercLow
                    record['humidityAvg'],  # humidityPercAvg
                    record['metric']['tempHigh'],  # tempHighC
                    record['metric']['tempLow'],  # tempLowC
                    record['metric']['tempAvg'],  # tempAvgC
                    record['metric']['dewptHigh'],  # dewPointHighC
                    record['metric']['dewptLow'],  # dewPointLowC
                    record['metric']['dewptAvg'],  # dewPointAvgC
                    record['winddirAvg'],  # winddirAvg
                    record['metric']['windspeedHigh'],  # windspeedHigh
                    record['metric']['windspeedLow'],  # windspeedLow
                    record['metric']['windspeedAvg'],  # windspeedAvg
                    record['metric']['windgustHigh'],  # windgustHigh
                    record['metric']['windgustLow'],  # windgustLow
                    record['metric']['windgustAvg'],  # windspeedAvg
                    record['metric']['pressureMax'],  # pressureMaxMbar
                    record['metric']['pressureMin'],  # pressureMinMbar
                    record['metric']['precipRate'],  # precipRateMM
                    record['metric']['precipTotal'],  # precipTotalMM
                )
                first_missing_column = None
                for idx in required_columns:
                    if record_out[idx] is None or record_out[idx] == '':
                        first_missing_column = idx
                        break
                if first_missing_column:
                    log.warning("Skipping record for %s, not all required fields present. First missing data: %d) %s" % (
                    obs_time, first_missing_column, table_schema[first_missing_column].name))
                    continue

                filename = "wunderground-%s.csv" % obs_time.strftime(partition_format)
                if previous_filename and previous_filename != filename:
                    load_data(previous_filename, previous_partition.strftime(partition_format), bigquery_client, table)

                previous_filename = filename
                # previous_partition = date(obs_time.year, obs_time.month, obs_time.day)
                previous_partition = date(obs_time.year, obs_time.month, 1)
                if not os.path.exists(filename):
                    do_header = True
                else:
                    do_header = False
                with open(filename, 'a', newline='') as csvfile:
                    data_writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
                    if do_header:
                        data_writer.writerow([field.name for field in table_schema])
                    data_writer.writerow(record_out)

                # _insert_data(record_out, bigquery_client, bigquery_dataset, table)
        date_to_get += timedelta(days=1)

    log.info("Done looping.")


def load_data(file_path: str, partition: str, bigquery_client: bigquery.Client, table: bigquery.Table):
    """
    NOTE: This really doesn't work with free tier. Un-partitioned working ok.
    https://cloud.google.com/bigquery/docs/reference/rest/v2/Job
    https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.LoadJobConfig.html
    https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.LoadJob.html
    $ bq --location europe-north1 load --skip_leading_rows=1 --source_format=CSV --max_bad_records=0 'PWS_data.weather_data$201501' wunderground-201501.csv
    Upload complete.
    :param partition:
    :param file_path:
    :param bigquery_client:
    :param table:
    :return:
    """
    job_config = bigquery.LoadJobConfig(
        schema=[{"name": field.name, "type": field.field_type, "mode": field.mode} for field in table_schema],
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


def _insert_data(rows_to_insert: list, bigquery_client: bigquery.Client, bigquery_dataset, table: bigquery.Table):
    """
    Note: BigQuery free tier doesn't allow DML at all!
    google.api_core.exceptions.Forbidden: 403 Billing has not been enabled for this project.
    Enable billing at https://console.cloud.google.com/billing.
    DML queries are not allowed in the free tier. Set up a billing account to remove this restriction.
    :param rows_to_insert:
    :param bigquery_client:
    :param bigquery_dataset:
    :param table:
    :return:
    """
    if False:
        # Note: google.api_core.exceptions.Forbidden: 403 POST
        #       Access Denied: BigQuery BigQuery: Streaming insert is not allowed in the free tier
        for row_to_insert in rows_to_insert:
            errors = bigquery_client.insert_rows(table, [row_to_insert])
            if not errors == []:
                print(errors)
                raise RuntimeError('Failed to insert data into BigQuery!')

    job_config = bigquery.QueryJobConfig()
    job_config.default_dataset = bigquery_dataset

    fields = zip([(field.name, field.field_type) for field in table_schema], rows_to_insert)
    query_parameters = []
    for field in fields:
        param_name = field[0][0]
        param_type = field[0][1]
        param_value = field[1]
        param_out = bigquery.ScalarQueryParameter(param_name, param_type, param_value)
        query_parameters.append(param_out)

    job_config.query_parameters = query_parameters

    table_columns = [field.name for field in table_schema]
    query = "INSERT INTO `%s` (%s) VALUES (%s)" % (
        table_name, ','.join(table_columns), ','.join(['?' for field in table_schema])
    )
    job = bigquery_client.query(query, job_config=job_config)
    result = job.result()
    if result.total_rows == 0:
        raise RuntimeError("Ouch! No data inserted!")


def main():
    parser = argparse.ArgumentParser(description='OpenDNSSEC BIND slave zone configurator')
    parser.add_argument('--bigquery-json-credentials',
                        metavar='GOOGLE-JSON-CREDENTIALS-FILE', required=True,
                        help='Mandatory. JSON-file with Google BigQuery API Service Account credentials.')
    parser.add_argument('--bigquery-dataset-id',
                        metavar='GOOGLE-BIGQUERY-DATASET-ID', required=True,
                        help='Mandatory. BigQuery dataset ID to store data into.')
    parser.add_argument('--api-key', required=True,
                        help="Weather.com API-key")
    parser.add_argument('--pws-id', required=True,
                        help="Weather Underground PWS id")
    parser.add_argument('--load-csv-file', metavar="FILENAME",
                        help="Do a BigQuery CSV-load from given file")
    parser.add_argument('--load-csv-partition', metavar="PARTITION-ID",
                        help="(not used) When doing CSV-load, load it into partition with this id")
    args = parser.parse_args()

    _setup_logger()
    log.info("Begin")

    if not args.bigquery_json_credentials:
        print("Really need --bigquery-json-credentials to store data into Google BigQuery!")
        exit(2)
    if not args.bigquery_dataset_id:
        print("Really need --bigquery-dataset-id to store data into Google BigQuery!")
        exit(2)

    # Init Google BigQuery
    bq_client, bq_dataset, bq_table = init_bigquery(args.bigquery_json_credentials, args.bigquery_dataset_id)

    # CSV-load?
    if args.load_csv_file:
        log.info("Loading CSV-file %s into BigQuery" % args.load_csv_file)
        load_data(args.load_csv_file, args.load_csv_partition, bq_client, bq_table)
        log.info("Done loading CSV-file")
        exit(0)

        # Go import PWS data
    most_recent_date = get_most_recent_record(bq_client, bq_dataset)
    if most_recent_date:
        most_recent_date += timedelta(days=1)
    else:
        most_recent_date = date(2015, 1, 19)

    pws_client = PWS(api_key=args.api_key, pws_id=args.pws_id, unit=Units.METRIC)
    loop_days(pws_client, most_recent_date, bq_client, bq_dataset, bq_table)

    log.info("Done.")


if __name__ == '__main__':
    main()
