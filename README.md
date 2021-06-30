# Weather Underground API tools

## Import PWS data into Google BigQuery

Example:
```bash
$ PYTHONPATH=. ./cli-utils/upload-to-google-bigquery.py \
  --pws-id ImyPWSidHERE \
  --api-key 0123456789abcdef \
  --bigquery-dataset-id PWS_data \
  --bigquery-json-credentials creds.json
```
