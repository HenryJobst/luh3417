import re
import subprocess
from dataclasses import dataclass, replace
from pathlib import Path
from posixpath import join
from shlex import quote
from subprocess import CompletedProcess, Popen
from typing import Text, Tuple

from luh3417.luhssh import SshManager
from luh3417.utils import LuhError

SSH_RE = re.compile(r"^([a-zA-Z0-9_-]+)@((?:[a-zA-Z0-9-]+\.)*(?:[a-zA-Z0-9-]+)):(\d*)(.*)$")


def parse_location(location: Text, compression: Text = "gzip") -> "Location":
    """
    Guess the location type and generates the appropriate object
    """

    sm = SSH_RE.match(location)

    if sm:
        port = sm.group(3)
        if port == '':
            port = None
        return SshLocation(user=sm.group(1), host=sm.group(2), port=port, path=sm.group(4),
                           compression_mode=compression)
    else:
        return LocalLocation(path=location, compression_mode=compression)


def get_compression_switch(compression_mode: Text):
    if compression_mode == "bzip2":
        return "-j"
    elif compression_mode == "xz":
        return "-J"
    elif compression_mode == "lzip":
        return "--lzip"
    else:  # default: gzip
        return "-z"


@dataclass
class Location:
    """
    Base location object. This abstraction allows to do very specific
    operations on local or remote file systems. This is not a generic file
    system implementation but rather a very specialized one so that every
    operation can be implemented the most possibly efficient way.
    """

    path: Text

    def get_content(self) -> Text:
        """
        Calling this returns the whole content of the file
        """

        raise NotImplementedError

    def set_compression_mode(self, compression_mode: Text):
        """
        Set compression mode
        """

        raise NotImplementedError

    def set_content(self, content) -> None:
        """
        Creates or overrides the file so it receives the provided content.
        The parent directory must exist and the location must not be a
        directory.
        """

        out, err, ret = self.run_script(
            f"echo -n {quote(content)} > {quote(self.path)}"
        )

        if ret:
            raise LuhError(f"Cannot set content: {err}")

    def ensure_exists_as_dir(self) -> None:
        """
        This ensures that the location is a directory and exists (as well as
        all parents)
        """

        raise NotImplementedError

    def archive_local_dir(self, local_path, doing) -> None:
        """
        Puts all the content of `local_path` into a TAR/GZ archive at the
        current location.

        Beware it's probably the opposite of what you imagined (:
        """

        raise NotImplementedError

    def extract_archive_to_dir(self, target_dir: Text) -> None:
        """
        If the file at this location is an archive, then extract its content
        into the specified target_dir. Otherwise raise an error.
        """

        raise NotImplementedError

    def run_script(self, script: Text) -> Tuple[Text, Text, int]:
        """
        Runs the provided script with bash, on the machine targeted by this
        location.
        """

        raise NotImplementedError

    def chown(self, owner: Text) -> None:
        """
        Chown recursively this location
        """

        raise NotImplementedError

    def exists(self):
        """
        Returns True if this location exists
        """

        out, err, ret = self.run_script(
            f"""
                if [ ! -e {quote(self.path)} ]
                then
                    exit 1
                fi
            """
        )

        return not ret

    def set_git_repo(self, repo: Text, version: Text):
        """
        Sets the current location to be a git repo at the given version. Any
        pre-existing file or directory at this location will be overridden.
        """

        location = self.path

        if location and location[-1] == "/":
            location = location[0:-1]

        out, err, ret = self.run_script(
            """
                git clone -b {version} {repo} {location}__ \\
                && mv {location} {location}___ \\
                && mv {location}__ {location} \\
                && rm -fr {location}___
            """.format(
                repo=quote(repo), version=quote(version), location=quote(location)
            )
        )

        if ret:
            raise LuhError(f"Could not clone repo: {err}")

    def child(self, file_name) -> "Location":
        """
        Generates the location object for a child file named file_name
        """

        return replace(self, path=join(self.path, file_name))

    def rsync_path(self, as_dir: bool = True):
        """
        Generates the equivalent rsync path for this location
        """

        path = f"{self}"

        if as_dir and (not path or path[-1] != "/"):
            path += "/"
        elif not as_dir and path and path[-1] == "/":
            path = path[0:-1]

        return path


@dataclass
class SshLocation(Location):
    """
    A file located on a remote server accessed through SSH
    """

    user: Text
    host: Text
    port: Text
    path: Text
    compression_mode: Text

    def __str__(self):
        return f"{self.user}@{self.host}:{self.port}{self.path}"

    @property
    def ssh_target(self):
        """
        Generates the target to give to SSH for this location
        """
        port = 22
        if self.port:
            port = self.port
        return f"{self.user}@{self.host}:{port}"

    def ssh_run(self, args, *p_args, **kwargs) -> CompletedProcess:
        """
        Runs a process remotely using subprocess.run(). This will enforce
        an UTF-8 encoding for stdin/out. Otherwise it's the same argument
        as run() and the SSH command is automatically appended to the args.
        """

        kwargs = dict(kwargs, encoding="utf-8")

        new_args = SshManager.instance(self.user, self.host, self.port).get_args(args)

        cp = subprocess.run(new_args, *p_args, **kwargs)

        if cp.returncode == 255:
            raise LuhError(
                f"SSH connection to {self.ssh_target} could not be established"
            )

        return cp

    def ssh_popen(self, args, *p_args, **kwargs) -> Popen:
        """
        Opens a process through SSH. It's the same arguments as Popen() except
        that the SSH command will be prepended to the args.
        """

        new_args = SshManager.instance(self.user, self.host, self.port).get_args(args)

        cp = subprocess.Popen(new_args, *p_args, **kwargs)

        if cp.returncode == 255:
            raise LuhError(
                f"SSH connection to {self.ssh_target} could not be established"
            )

        return cp

    def get_content(self) -> Text:
        """
        Uses a remote cat to get the content
        """

        cp = self.ssh_run(
            ["cat", self.path], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
        )

        if cp.returncode == 1:
            raise LuhError(
                f"The file {self} does not exist or you don't have "
                f"permissions to read it"
            )
        elif cp.returncode:
            raise LuhError(f"Unknown error while reading {self}")

        return cp.stdout

    def set_compression_mode(self, compression_mode: Text):
        self.compression_mode = compression_mode

    def ensure_exists_as_dir(self) -> None:
        """
        Runs mkdir -p remotely
        """

        cp = self.ssh_run(
            ["mkdir", "-p", self.path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )

        if cp.returncode:
            raise LuhError(f"Could not create {self} as a directory: {cp.stderr}")

    def archive_local_dir(self, local_path: Text, doing):
        """
        Generates the archive locally and pipe it to a remote dd to write it
        on disk on the other side
        """

        doing.logger.debug("Compression mode: %s", self.compression_mode)
        compression_switch = get_compression_switch(self.compression_mode)
        doing.logger.debug("Compression switch: %s", compression_switch)

        tar = subprocess.Popen(
            ["tar", "-C", local_path, "-c", compression_switch, "."],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        dd = self.ssh_popen(
            ["dd", f"of={self.path}"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            stdin=tar.stdout,
        )

        _, dd_err = dd.communicate()
        _, tar_err = tar.communicate()

        if dd.returncode:
            raise LuhError(f"Could not write remote archive: {dd_err}")

        if tar.returncode:
            raise LuhError(f"Could not create the archive: {tar_err}")

    def extract_archive_to_dir(self, target_dir: Text) -> None:
        """
        Cat the remote file and pipe it into tar
        """

        parse_location(target_dir, self.compression_mode).ensure_exists_as_dir()

        cat = self.ssh_popen(
            ["cat", self.path], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        tar = subprocess.Popen(
            ["tar", "-C", target_dir, "-x", get_compression_switch(self.compression_mode)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            stdin=cat.stdout,
        )

        _, tar_err = tar.communicate()
        _, cat_err = cat.communicate()

        if cat.returncode:
            raise LuhError(f"Error while reading the remote archive: {cat_err}")

        if tar.returncode:
            raise LuhError(f"Error while extracting the archive: {tar_err}")

    def chown(self, owner: Text) -> None:
        """
        Simply use chown
        """

        cp = self.ssh_run(
            ["chown", "-R", owner, self.path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )

        if cp.returncode:
            raise LuhError(f"Failed to chown: {cp.stderr[:1000]}")

    def run_script(self, script: Text) -> Tuple[Text, Text, int]:
        """
        Runs the script through SSH piping
        """

        cp = self.ssh_popen(
            ["bash"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
        )

        cp.stdin.write(script)

        out, err = cp.communicate()

        return out, err, cp.returncode


@dataclass
class LocalLocation(Location):
    """
    Runs everything locally, everything inside is pretty straightforward
    """

    path: Text
    compression_mode: Text

    def __str__(self):
        return self.path

    def get_content(self) -> Text:
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return f.read()
        except PermissionError:
            raise LuhError(f"You don't have the permission to read {self}")
        except FileNotFoundError:
            raise LuhError(f"The file {self} does not exist")
        except OSError:
            raise LuhError(f"Unknown error while opening {self}")

    def set_compression_mode(self, compression_mode: Text):
        self.compression_mode = compression_mode

    def ensure_exists_as_dir(self) -> None:
        try:
            Path(self.path).mkdir(parents=True, exist_ok=True)
        except PermissionError:
            raise LuhError(f"You don't have the permission the create {self}")
        except NotADirectoryError:
            raise LuhError(f"Some component of {self} is not a directory")
        except OSError:
            raise LuhError(f"Unknown error while creating {self}")

    def archive_local_dir(self, local_path, doing):
        doing.logger.debug("Compression mode: %s", self.compression_mode)
        compression_switch = get_compression_switch(self.compression_mode)
        doing.logger.debug("Compression switch: %s", compression_switch)
        cp = subprocess.run(
            ["tar", "-C", local_path, "-c", compression_switch, "-f", self.path, "."],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        if cp.returncode:
            raise LuhError(f"Could not create archive {self.path}")

    def extract_archive_to_dir(self, target_dir: Text) -> None:
        """
        Plain old local archive extraction
        """

        parse_location(target_dir, self.compression_mode).ensure_exists_as_dir()

        tar = subprocess.run(
            ["tar", "-C", target_dir, "-x", get_compression_switch(self.compression_mode), "-f", self.path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )

        if tar.returncode:
            raise LuhError(f"Error while extracting the archive: {tar.stderr}")

    def chown(self, owner: Text) -> None:
        """
        A simple local chown
        """

        cp = subprocess.run(
            ["chown", "-R", owner, self.path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )

        if cp.returncode:
            raise LuhError(f"Failed to chown: {cp.stderr[:1000]}")

    def run_script(self, script: Text) -> Tuple[Text, Text, int]:
        """
        Just pipe the script to bash
        """

        cp = subprocess.Popen(
            ["bash"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
        )

        cp.stdin.write(script)

        out, err = cp.communicate()

        return out, err, cp.returncode
