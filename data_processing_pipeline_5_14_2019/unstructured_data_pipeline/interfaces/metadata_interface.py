"""
metadata_interface
~~~~~~~~~~~~~~~~~~
An interface for creating concrete metadata data frame classes.
"""
import abc
import pandas as pd


# Interfaces #
####################################################################################################
class MetaDataInterface(metaclass=abc.ABCMeta):
    """An interface for loading the metadata files."""

    @abc.abstractmethod
    def load_metadata(self, file_path: str):
        """Load in the metadata file(s)."""
        raise NotImplementedError

    @abc.abstractmethod
    def load_metadata_file(self, full_file_path: str) -> pd.DataFrame:
        """Load a specific metadata file.
        NOTE:
            Method should be used only to check output.
            Should not be used within the data piepline.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def load_backlogged_metadata(self, files_path: str, days: int):
        """Load in the backlogged metadata files."""
        raise NotImplementedError

    @abc.abstractmethod
    def update_metadata_log(self, file_path: str):
        """Update the metadata log file that keeps track of the
        last read delta file."""
        raise NotImplementedError

    @abc.abstractmethod
    def load_metadata_log(self, file_path: str):
        """Load in the metadata log."""
        raise NotImplementedError
