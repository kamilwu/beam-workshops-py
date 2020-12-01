import argparse

import apache_beam as beam
from apache_beam.transforms import combiners


def parse_flags():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        default='gs://apache-beam-samples/shakespeare/romeoandjuliet.txt')
    return parser.parse_known_args()


class CountAndPrint(beam.PTransform):
    def __init__(self, label):
        super().__init__()
        self.label = label

    def expand(self, pcoll):
        return (
            pcoll
            # Apply transforms with a specified label (each label must be
            # unique)
            | f'Count {self.label}' >> combiners.Count.Globally()
            | f'Print {self.label}' >> beam.Map(print))


def run():
    args, pipeline_args = parse_flags()
    with beam.Pipeline(argv=pipeline_args) as p:
        lines = p | beam.io.ReadFromText(args.input)

        # Print newline counts for the file
        lines | CountAndPrint('lines')

        # Print the number of words in the file
        (lines
         # `FlatMap` is like `Map`, but the output is then flattened into
         # a single PCollection.
         | beam.FlatMap(lambda line: (word for word in line.split(' ')))
         | CountAndPrint('words'))


if __name__ == '__main__':
    run()
