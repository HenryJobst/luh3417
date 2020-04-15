import json
from json import JSONDecodeError
from typing import Dict, List, Optional, Text

from luh3417.luhfs import Location, parse_location
from luh3417.luhsql import LuhSql, create_root_from_source
from luh3417.record_set import RecordSet, Zone, parse_domain
from luh3417.serialized_replace import ReplaceMap
from luh3417.snapshot import sync_files
from luh3417.utils import LuhError, escape


def read_config(file_path: Text):
    """
    Read configuration from JSON file (extracted from the snapshot)
    """

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        raise LuhError("Configuration file is not valid JSON")
    except OSError as e:
        raise LuhError(f"Error while opening file: {e}")


def get_remote(config: Dict) -> Location:
    """
    Reads the configuration to extract the address of the remote
    """

    try:
        return parse_location(config["args"]["source"])
    except KeyError:
        raise LuhError("Configuration is incomplete, missing args.source")


def get_wp_config(config: Dict) -> Dict:
    """
    Reads the configuration to extract the database configuration
    """

    try:
        return config["wp_config"]
    except KeyError:
        raise LuhError("Configuration is incomplete, missing wp_config")


def restore_files(wp_root: Text, remote: Location):
    """
    Restores the file from the local wp_root to the remote location
    """

    local = parse_location(wp_root)
    sync_files(local, remote, delete=True)


def restore_db(db: LuhSql, dump_path: Text):
    """
    Restores the specified file into DB, using the wp config and remote
    location to connect the DB.
    """

    try:
        with open(dump_path, "r", encoding="utf-8") as f:
            db.restore_dump(f)
    except OSError as e:
        raise LuhError(f"Could not read SQL dump: {e}")


def run_queries(db: LuhSql, queries: List[Text]):
    """
    Runs all the queries from the config
    """

    for query in queries:
        db.run_query(query)


def make_replace_map(replace_in_dump: List[Dict[Text, Text]]) -> ReplaceMap:
    """
    Transforms the config/patch syntax into an internal ReplaceMap
    """

    return [
        (x["search"].encode("utf-8"), x["replace"].encode("utf-8"))
        for x in replace_in_dump
    ]


def ensure_db_exists(wp_config, mysql_root, source: Location, db_host: Text):
    """
    If a database is pre-existing, delete it. Then create a new one and create
    the appropriate user.
    """

    db = create_root_from_source(wp_config, mysql_root, source, db_host)

    name = escape(wp_config["db_name"], "`")
    user = escape(wp_config["db_user"], "`")
    host = escape(wp_config["db_host"], "`")

    used_db_host = host
    if db_host:
        used_db_host = db_host

    password = escape(wp_config["db_password"], "'")

    db.run_query(f"drop database if exists {name};")
    db.run_query(f"create database {name};")
    db.run_query(
        f"grant all privileges on {name}.* to {user}@{used_db_host} identified by {password};"
    )


def install_outer_files(outer_files: List[Dict], source: Location):
    """
    Given the list of outer files, sets the appropriate content to the
    appropriate location
    """

    for f in outer_files:
        location = source.child(f["name"])
        location.set_content(f["content"])


def run_post_install(post_install: List[Text], source: Location):
    """
    Running the post-install scripts
    """

    for script in post_install:
        source.run_script(script)


def configure_dns(dns: Dict):
    """
    Configures the DNS zones by loading all the zones into RecordSet and then
    calling RecordSet on all entries to configure them properly
    """

    zones = []

    for zone in dns["providers"]:
        zone["domain"] = parse_domain(zone["domain"])
        zones.append(Zone(**zone))

    rs = RecordSet(zones)

    for entry in dns["entries"]:
        if entry["type"] == "alias":
            func = rs.set_alias
        elif entry["type"] == "ips":
            func = rs.set_ips
        else:
            raise LuhError(f'Unknown entry type {entry["type"]}')

        func(**entry["params"])


def patch_config(
    config: Dict, patch_location: Optional[Location], allow_in_place: bool = False
) -> Dict:
    """
    Applies a configuration patch from the source patch file, which will
    alter the restoration process.

    Available options:

    - `wp_config` - WordPress configuration (database user and so on)
    - `source`
    - `owner` - Same syntax as chown owner, changes the ownership of restored
      files
    - `git` - A list of repositories to clone (cf below).
    - `setup_queries` - A list of SQL queries (as strings) that will be
      executed after restoring the DB
    - `php_define` - A dictionary of constant/value to be defined in wp-config
    - `replace_in_dump` - Replaces a list of values in the SQL dump
    - `mysql_root` - Method and options to become root of MySQL (see the
       README)
    - `outer_files` - Files to place on the host's filesystem
    - `post_install` - A list of Bash scripts to be executed when the install
      is done

    Example for the `git` value:

        "git": [
            {
                "location": "wp-content/themes/jupiter-child",
                "repo": "git@gitlab.com:your_company/jupiter_child.git",
                "version": "master"
            }
        ]

    Example for the `replace_in_dump` value:

        "replace_in_dump": [
            {
                "search": "https://old-domain.com",
                "replace": "https://new-domain.com"
            }
        ]

    Example for the `outer_files` value:

        "outer_files": [
            {
                "name": "robots.txt",
                "content": "User-agent: *\nDisallow: /\n"
            },
            {
                "name": "/etc/apache2/sites-available/my-host.conf",
                "content": "<VirtualHost> ..."
            }
        ]
    """

    base_config = {
        "owner": None,
        "git": [],
        "setup_queries": [],
        "php_define": {},
        "replace_in_dump": [],
        "mysql_root": None,
        "outer_files": [],
        "post_install": [],
        "dns": {},
    }

    for k, v in config.items():
        base_config[k] = v

    if patch_location:
        try:
            content = patch_location.get_content()
            patch = json.loads(content)
        except JSONDecodeError as e:
            raise LuhError(f"Could not decode patch file: {e}")
        else:
            if not allow_in_place:
                try:
                    assert patch["args"]["source"]
                except (AssertionError, KeyError):
                    raise LuhError(
                        "The patch did not override the source location "
                        "and the --allow-in-place flag is not set"
                    )

            for k, v in patch.items():
                base_config[k] = v
    elif not allow_in_place:
        raise LuhError(
            "If you do not set the --allow-in-place flag you must provide a "
            "patch which overrides the source location"
        )

    return base_config
