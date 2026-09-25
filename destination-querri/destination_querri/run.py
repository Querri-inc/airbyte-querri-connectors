import sys

from destination_querri import DestinationQuerri


def run():
    # Destinations use Destination.run (spec/check/write), not the source
    # entrypoint `airbyte_cdk.entrypoint.launch`, which has no `write` command.
    DestinationQuerri().run(sys.argv[1:])
