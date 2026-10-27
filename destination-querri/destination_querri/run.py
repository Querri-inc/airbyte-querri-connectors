import sys

from destination_querri import DestinationQuerri


def run():
    # Destination.run, not airbyte_cdk.entrypoint.launch: launch is the SOURCE
    # entrypoint, whose parser has no `write` command.
    DestinationQuerri().run(sys.argv[1:])
