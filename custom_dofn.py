from datetime import date

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms import window


TABLE = 'bigquery-public-data.chicago_taxi_trips.taxi_trips '


class ExtractTimestamp(beam.DoFn):
    # the "process" is supposed to be a generator function.
    # Use "yield" instead of "return".
    def process(self, element):
        tm = element['trip_start_timestamp'].timestamp()
        yield window.TimestampedValue(element, int(tm))


class Print(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        payment_type, total = element
        print(f'{date.fromtimestamp(window.start)} - '
              f'{date.fromtimestamp(window.end)}: '
              f'{payment_type}, {total:.2f}')
        yield element


class GetPaymentTypeAndTripTotal(beam.DoFn):
    def process(self, element):
        if element['trip_total']:
            yield element['payment_type'], element['trip_total']


def run():
    options = PipelineOptions()
    # You need to set this flag if you are going to use modules imported in
    # __main__ AND run the pipeline on Dataflow.
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:
        (p
         | beam.io.ReadFromBigQuery(
            query=f'SELECT * FROM {TABLE} LIMIT 1000',
            use_standard_sql=True)
         | beam.ParDo(ExtractTimestamp())
         | beam.WindowInto(window.FixedWindows(3600 * 24))
         | beam.ParDo(GetPaymentTypeAndTripTotal())
         | beam.CombinePerKey(sum)
         | beam.ParDo(Print()))


# Reading from BigQuery requires --project and --temp_location to be provided.
if __name__ == '__main__':
    run()
