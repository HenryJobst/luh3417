import json
from argparse import ArgumentParser, Namespace
from datetime import datetime
from os.path import join
from tempfile import TemporaryDirectory
from typing import Dict, Text

from luh3417.luhfs import Location, parse_location
from luh3417.luhsql import create_from_source
from luh3417.snapshot import copy_files
from luh3417.utils import make_doer, parse_wp_config, setup_logging

doing = make_doer("luh3417.snapshot")


def parse_args() -> Namespace:
    """
    Parse arguments fro the snapshot
    """

    parser = ArgumentParser(
        description=(
            "Takes a snapshot of a WordPress website remotely and "
            "stores it to either a local or a remote location. This "
            "requires rsync, php-cli and mysqldump"
        )
    )

    parser.add_argument(
        "source",
        help=(
            "Source directory for the WordPress installation dir. Syntax: "
            "`/var/www` or `user@host:/var/www`"
        ),
        type=parse_location,
    )
    parser.add_argument(
        "backup_dir", help="Directory to store the snapshot", type=parse_location
    )
    parser.add_argument(
        "-n",
        "--snapshot-base-name",
        help="Base name for the snapshot file. Defaults to DB name.",
    )
    parser.add_argument(
        "-t",
        "--file-name-template",
        help="Template for snapshot file name. Defaults to: `{base}_{time}.tar.gz`",
        default="{base}_{time}.tar.gz",
    )

    return parser.parse_args()


def make_dump_file_name(args: Namespace, wp_config: Dict, now: datetime) -> Location:
    """
    Generates the location name where to dump the file
    """

    if not args.snapshot_base_name:
        base_name = wp_config["db_name"]
    else:
        base_name = args.snapshot_base_name

    name = args.file_name_template.format(base=base_name, time=now.isoformat() + "Z")

    return args.backup_dir.child(name)


def dump_settings(args: Namespace, wp_config: Dict, now: datetime, file_path: Text):
    """
    Given the settings and various environmental data, dump them in a JSON file
    which will be embedded in the archive and can be used to guess things when
    restoring the snapshot.
    """

    args = dict(vars(args), source=f"{args.source}", backup_dir=f"{args.backup_dir}")

    content = {"args": args, "wp_config": wp_config, "time": now.isoformat() + "Z"}

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(content, f, indent=4)


def main():
    """
    Executes things in order
    """

    setup_logging()
    args = parse_args()
    now = datetime.utcnow()

    with doing("Parsing remote configuration"):
        wp_config = parse_wp_config(args.source)

    with TemporaryDirectory() as d:
        work_location = parse_location(d)

        with doing("Saving settings"):
            dump_settings(args, wp_config, now, join(d, "settings.json"))

        with doing("Copying database"):
            db = create_from_source(wp_config, args.source)
            db.dump_to_file(join(d, "dump.sql"))

        with doing("Copying files"):
            copy_files(args.source, work_location.child("wordpress"))

        with doing("Writing archive"):
            args.backup_dir.ensure_exists_as_dir()
            make_dump_file_name(args, wp_config, now).archive_local_dir(d)


if __name__ == "__main__":
    main()