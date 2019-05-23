"""
connection_interfaces
~~~~~~~~~~~~~~~~~~~~~
"""
import  abc
import socket
import socks
from enum import Enum, EnumMeta, IntEnum, OrderedDict, auto


# Interfaces #
####################################################################################################
class SftpConnectionInterface(metaclass=abc.ABCMeta):
    """Interface for creating a secure file transfer protocol"""

    @abc.abstractmethod
    def connect(self):
        """Connect via sftp"""
        pass

    @abc.abstractmethod
    def transfer_payload(self, filename: str, filepath: str, remote_sub_dir: str):
        pass


class SSHConnectionInterface(metaclass=abc.ABCMeta):
    """Interface for creating """

    @abc.abstractmethod
    def connect(self):
        """Connect via ssh"""
        pass
