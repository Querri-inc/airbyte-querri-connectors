import sys

from airbyte_cdk.entrypoint import launch
from destination_querri import DestinationQuerri

if __name__ == "__main__":
    destination = DestinationQuerri()
    launch(destination, sys.argv[1:])
