import subprocess
from dataclasses import dataclass
from subprocess import DEVNULL, PIPE
from typing import List, Optional, Text, TextIO

from luh3417.luhfs import LocalLocation, Location, SshLocation
from luh3417.luhssh import SshManager
from luh3417.serialized_replace import ReplaceMap, walk
from luh3417.utils import LuhError


def create_from_source(wp_config, source: Location, db_host: Text):
    """
    Using a Location object and the WP config, generates the appropriate LuhSql
    object
    """

    if isinstance(source, SshLocation):
        ssh_user = source.user
        ssh_host = source.host
        ssh_port = source.port
    elif isinstance(source, LocalLocation):
        ssh_user = None
        ssh_host = None
        ssh_port = None
    else:
        raise LuhError(f"Unknown source type: {source.__class__.__name__}")

    used_db_host = wp_config["db_host"]
    if db_host:
        used_db_host = db_host

    return LuhSql(
        host=used_db_host,
        user=wp_config["db_user"],
        password=wp_config["db_password"],
        db_name=wp_config["db_name"],
        ssh_user=ssh_user,
        ssh_host=ssh_host,
        ssh_port=ssh_port,
    )


def create_root_from_source(wp_config, mysql_root, source: Location, db_host: Text) -> "LuhSql":
    """
    Based on the regular DB accessor, create a root version which will be able
    to run DB manipulation queries
    """

    db = create_from_source(wp_config, source, db_host)
    method = mysql_root.get("method")
    options = mysql_root.get("options", {})

    try:
        if method == "socket":
            db.db_name = None
            db.user = options.get("mysql_user", "root")
            db.sudo_user = options.get("sudo_user")
            return db
        else:
            raise LuhError(f"Wrong MySQL root method: {method}")
    except KeyError as e:
        raise LuhError(f"Missing key for mysql_root: {e}")


def patch_sql_dump(source_path: Text, dest_path: Text, replace: ReplaceMap):
    """
    Patches the SQL dump found at source_path into a new SQL dump found in
    dest_path. It will use the replace map to replace values.

    Values are replaced in a holistic way so that PHP serialized values are
    not broken and escaped character are detected as such. This is by far not
    perfect but seems sufficient for most use cases.
    """

    try:
        with open(source_path, "rb") as i, open(dest_path, "wb") as o:
            for line in i:
                o.write(walk(line, replace))
    except OSError as e:
        raise LuhError(f"Could not open SQL dump: {e}")


@dataclass
class LuhSql:
    """
    A helper class to access MySQL locally or remotely through a SSH proxy
    """

    host: Text
    user: Text
    password: Text
    db_name: Text
    ssh_user: Optional[Text]
    ssh_host: Optional[Text]
    ssh_port: Optional[Text]
    sudo_user: Optional[Text] = None

    def ssh_args(self, args: List[Text]) -> List[Text]:
        """
        Appends SSH connection args if required
        """

        if self.ssh_host and self.ssh_user and self.ssh_port:
            return SshManager.instance(self.ssh_user, self.ssh_host, self.ssh_port).get_args(args)
        else:
            return args

    def sudo_args(self, args: List[Text]) -> List[Text]:
        """
        Appends sudo privilege escalation args if required
        """

        if self.sudo_user:
            return ["sudo", "-u", self.sudo_user] + list(args)
        else:
            return list(args)

    def mysql_args(
        self, command: Text, extra_args: Optional[List[Text]] = None
    ) -> List[Text]:
        """
        Generates the MySQL connection arguments depending on the connection
        method and so on
        """

        out = [command] + (extra_args if extra_args else [])

        if self.password:
            out += [f"-p{self.password}"]

        out += ["-u", self.user]
        out += ["-h", self.host]

        if self.db_name:
            out += [self.db_name]

        return out

    def args(
        self, command: Text, extra_args: Optional[List[Text]] = None
    ) -> List[Text]:
        """
        Generates the proper arguments for this command and the connection
        configuration
        """

        args = self.mysql_args(command, extra_args)
        args = self.sudo_args(args)
        args = self.ssh_args(args)

        return args

    def dump_to_file(self, file_path: Text):
        """
        Dumps the database into the specified file
        """

        with open(file_path, "w", encoding="utf-8") as f:
            p = subprocess.Popen(
                self.args("mysqldump", ["--hex-blob"]),
                stderr=PIPE,
                stdout=f,
                stdin=DEVNULL,
                encoding="utf-8",
            )

            _, err = p.communicate()

            if p.returncode:
                raise LuhError(f"Could not dump MySQL DB: {err}")

    def restore_dump(self, fp: TextIO):
        """
        Restores a dump into the DB, reading the dump from an input TextIO
        (which can be the stdout of another process or simply an open file, by
        example).
        """

        p = subprocess.Popen(
            self.args("mysql"), stderr=PIPE, stdout=DEVNULL, stdin=fp, encoding="utf-8"
        )

        _, err = p.communicate()

        if p.returncode:
            raise LuhError(f"Could not import MySQL DB: {err}")

    def run_query(self, query: Text):
        """
        Runs a single SQL query
        """

        p = subprocess.Popen(
            self.args("mysql"),
            stderr=PIPE,
            stdout=DEVNULL,
            stdin=PIPE,
            encoding="utf-8",
        )

        _, err = p.communicate(query)

        if p.returncode:
            raise LuhError(f"Could not run MySQL query: {err}")
