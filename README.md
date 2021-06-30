# Weather Underground API tools

This library and set of CLI-utilities is written for accessing Weather Underground
weather data.

For human interface, see: https://www.wunderground.com/

For API docs, see: https://weather.com/swagger-docs/call-for-code

## Prerequisites
* Weather Underground API-key
  * Anybody running a PWS can obtain one

## Import PWS data into Google BigQuery

Example:
```bash
$ PYTHONPATH=. ./cli-utils/upload-to-google-bigquery.py \
  --pws-id ImyPWSidHERE \
  --api-key 0123456789abcdef \
  --bigquery-dataset-id PWS_data \
  --bigquery-json-credentials creds.json
```
