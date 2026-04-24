"""Top-level daemon entry point.

Equivalent to `flowbyte daemon` via CLI.
Can also be invoked directly: python -m flowbyte.daemon
"""
from flowbyte.scheduler.daemon import start_daemon


def main() -> None:
    start_daemon()


if __name__ == "__main__":
    main()
