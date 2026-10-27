import sys

from destination_querri import DestinationQuerri


def run():
    # Airbyte reads stdout through a pipe, where Python block-buffers it: a STATE
    # would sit in the buffer until 8 KB of output piled up behind it, so the
    # platform could see no checkpoint until the sync ended. Flush every line.
    sys.stdout.reconfigure(line_buffering=True)
    # Destination.run, not airbyte_cdk.entrypoint.launch: launch is the SOURCE
    # entrypoint, whose parser has no `write` command.
    DestinationQuerri().run(sys.argv[1:])
