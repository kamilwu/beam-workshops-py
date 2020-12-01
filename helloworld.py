import sys

import apache_beam as beam


def run():
    with beam.Pipeline(argv=sys.argv) as p:
        (p
         # create a PCollection from an iterable
         | beam.Create(['Hello world!'])
         # apply a callable to every element in PCollection
         | beam.Map(print))


if __name__ == '__main__':
    run()
