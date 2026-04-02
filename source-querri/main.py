import sys

from airbyte_cdk.entrypoint import launch
from source_querri import SourceQuerri

if __name__ == "__main__":
    source = SourceQuerri()
    launch(source, sys.argv[1:])
