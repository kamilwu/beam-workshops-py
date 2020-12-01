import argparse

import apache_beam as beam


def parse_flags():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        default='bigquery-public-data.chicago_taxi_trips.taxi_trips')
    parser.add_argument(
        '--output',
        required=True,
        help='The name of the table specified as: "DATASET.TABLE" or '
             '"PROJECT:DATASET.TABLE"')
    return parser.parse_known_args()


def run():
    args, pipeline_args = parse_flags()
    with beam.Pipeline(argv=pipeline_args) as p:
        (p
         | beam.io.ReadFromBigQuery(
            query=f'SELECT unique_key, trip_seconds FROM {args.input} '
                  f'LIMIT 1000',
            use_standard_sql=True)
         | beam.io.WriteToBigQuery(
            table=args.output,
            schema='unique_key:STRING, trip_seconds:INTEGER',
            # describes what happens if the table does not exist
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            # describes what happens if the table is not empty
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))


# Reading from BigQuery requires --project and --temp_location to be provided.
if __name__ == '__main__':
    run()
