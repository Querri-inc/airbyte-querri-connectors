import sys

from airbyte_cdk.entrypoint import launch

from destination_querri import DestinationQuerri


def run():
    destination = DestinationQuerri()
    launch(destination, sys.argv[1:])
