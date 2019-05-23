"""
emailer_interface
~~~~~~~~~~~~~~~~~
"""
import abc


# Emailer Interface #
####################################################################################################
class PipelineEmailerInterface(metaclass=abc.ABCMeta):
    """An email interface for creating customized emails"""

    @abc.abstractproperty
    def host___(self):
        pass

    @abc.abstractmethod
    @host___.setter
    def host___(self, host: str):
        """host of the email server"""
        pass

    @abc.abstractproperty
    def to___(self):
        pass

    @abc.abstractmethod
    @to___.setter
    def to___(self, *recipients):
        """Created a dictionary of recipients of the email"""
        pass

    @abc.abstractproperty
    def from___(self):
        pass

    @abc.abstractmethod
    @from___.setter
    def from___(self, sender: str):
        """Sender of the email"""
        pass

    @abc.abstractproperty
    def subject___(self):
        pass

    @abc.abstractmethod
    @subject___.setter
    def subject___(self, sub: str):
        """Subject of the email"""
        pass

    @abc.abstractproperty
    def body___(self):
        pass

    @abc.abstractmethod
    @body___.setter
    def body___(self, b: str):
        """Body fo the email"""
        pass

    @abc.abstractproperty
    def attachments___(self):
        pass

    @abc.abstractmethod
    @attachments___.setter
    def attachments___(self, *atmnts):
        """Add attachments to the email"""
        pass

    @abc.abstractmethod
    def send_email(self):
        """Send an email"""
