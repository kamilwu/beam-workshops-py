import argparse

import apache_beam as beam


def parse_flags():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        default='gs://apache-beam-samples/shakespeare/romeoandjuliet.txt')
    parser.add_argument('--output', required=True)
    return parser.parse_known_args()


def run():
    args, pipeline_args = parse_flags()
    with beam.Pipeline(argv=pipeline_args) as p:
        (p
         | beam.io.ReadFromText(args.input)
         | beam.io.WriteToText(args.output))


if __name__ == '__main__':
    run()
