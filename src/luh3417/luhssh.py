from shlex import quote
from shutil import rmtree
from subprocess import DEVNULL, PIPE, Popen, TimeoutExpired
from tempfile import mkdtemp
from typing import Dict, Optional, Text, Tuple, Union

from luh3417.utils import make_doer

doing = make_doer("luh3417.luhssh")


def make_ssh_args(
    user: Text,
    host: Text,
    port: Union[int, Text, None] = None,
    options: Optional[Dict] = None,
    compress: bool = False,
    forward_agent: bool = False,
    nothing: bool = False,
):
    """
    Generates the appropriate SSH CLI args

    :param user: Username
    :param host: Host
    :param port: Port number (None to use default)
    :param options: SSH options (passed using -o)
    :param compress: Compress the connection
    :param forward_agent: Enable agent forwarding
    :param nothing: Just open the connection, don't run anything
    """

    out = ["ssh"]

    if options:
        for k, v in options.items():
            out.append("-o")
            out.append(f"{k}={v}")

    if port:
        out += ["-p", f"{port}"]

    if compress:
        out += ["-C"]

    if nothing:
        out += ["-N"]

    if forward_agent:
        out += ["-A"]

    return out + [f"{user}@{host}"]


class SshManager:
    """
    A manager of SSH connections. If you might open SSH connections, there is
    some cleanup to do and make sure that you eventually call shutdown().

    This allows you to multiplex SSH connections by opening a master connection
    first and then provides you SSH CLI arguments that use this multiplexed
    connection, which you can use with Popen() and so on.

    Use the following way:

    >>> try:
    >>>     manager = SshManager.instance('root', 'server.com', 'port')
    >>>     args = manager.get_args(['uptime'])
    >>>     p = Popen(args)
    >>>     p.wait()
    >>> finally:
    >>>     SshManager.shutdown()
    """

    _instances: Dict[Tuple, "SshManager"] = {}

    forward_agent = True
    compress = False

    def __init__(self, user: Text, host: Text, port: Text):
        """
        Dont call directly! Use instance() instead.
        """

        self.user: Text = user
        self.host: Text = host
        self.port: Text = port
        self.control_dir: Text = None
        self.process: Popen = None

    @property
    def control(self):
        """
        Generates the path to the control socket
        """

        return f"{self.control_dir}/control"

    def start(self):
        """
        Starting the master connection
        """

        doing.logger.debug(f"Connecting SSH to {self.user}@{self.host}:{self.port}")

        self.control_dir = mkdtemp()

        self.process = Popen(
            make_ssh_args(
                self.user,
                self.host,
                self.port,
                options={
                    "ControlPath": self.control,
                    "ControlMaster": "yes",
                    "ControlPersist": "no",
                },
                nothing=True,
                forward_agent=self.forward_agent,
                compress=self.compress,
            ),
            stdin=DEVNULL,
            stdout=DEVNULL,
            stderr=PIPE,
        )

        doing.logger.debug(f"Process {self.process.pid} created")

    def cleanup(self):
        """
        Cleaning up this particular connection
        """

        doing.logger.debug(f"Tearing down SSH connection to {self.user}@{self.host}:{self.port}")

        if self.process:
            try:
                self.process.terminate()
                self.process.communicate(timeout=10)
            except TimeoutExpired:
                self.process.kill()
                doing.logger.debug(f"Process {self.process.pid} killed")
            else:
                doing.logger.debug(f"Process {self.process.pid} terminated")

        if self.control_dir:
            rmtree(self.control_dir)

    def get_args(self, args):
        """
        Generating args for a child connection using this master
        """

        return make_ssh_args(
            self.user,
            self.host,
            self.port,
            options={"ControlPath": self.control, "ControlMaster": "no"},
            compress=self.compress,
            forward_agent=self.forward_agent,
        ) + [quote(a) for a in args]

    @classmethod
    def instance(cls, user, host, port=None) -> "SshManager":
        """
        Gets or create and start the manager instance for this user and host
        """

        if port is None:
            port = 22

        key = (user, host, port)

        if key not in cls._instances:
            cls._instances[key] = cls(user, host, port)
            cls._instances[key].start()

        return cls._instances[key]

    @classmethod
    def shutdown(cls):
        """
        Global shutdown of all manager instances
        """

        if cls._instances:
            doing.logger.debug("Shutting down SSH connections")

            to_delete = []

            for k, manager in cls._instances.items():
                manager.cleanup()
                to_delete.append(k)

            for k in to_delete:
                del cls._instances[k]
