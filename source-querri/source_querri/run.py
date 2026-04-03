import sys

from airbyte_cdk.entrypoint import launch

from source_querri import SourceQuerri


def main():
    source = SourceQuerri()
    launch(source, sys.argv[1:])
