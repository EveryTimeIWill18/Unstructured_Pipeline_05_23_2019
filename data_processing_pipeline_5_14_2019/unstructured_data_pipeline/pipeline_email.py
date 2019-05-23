"""
pipeline_email
~~~~~~~~~~~~~~
"""
import smtplib
from email.mime.text import MIMEText
from itertools import chain

from unstructured_data_pipeline.interfaces.emailer_interface import PipelineEmailerInterface


# Concrete Emailer Implementation #
####################################################################################################
class PipelineEmailer(PipelineEmailerInterface):
    """Concrete implementation of the email interface"""

    def __init__(self):
        self._host = None
        self._to: list = []
        self._from: str = None
        self._body: str = None
        self._subject: str = None
        self._attachments: str = None
        self.mail_server:smtplib.SMTP = None

    @property
    def host___(self):
        return self._host
    @host___.setter
    def host___(self, host: str):
        self._host = host

    @property
    def to___(self):
        return self._to
    @to___.setter
    def to___(self, *to):
       self._to = list(chain.from_iterable(list(to)))

    @property
    def from___(self):
        return self._from
    @from___.setter
    def from___(self, f: str):
        self._from = f

    @property
    def subject___(self):
        return self._subject
    @subject___.setter
    def subject___(self, subject: str):
        self._subject = subject

    @property
    def body___(self):
        return self._body
    @body___.setter
    def body___(self, body: str):
        self._body = body

    @property
    def attachments___(self):
        return self._attachments
    @attachments___.setter
    def attachments___(self, *attachments):
        pass

    def send_email(self):
        """Send an email"""
        try:
            # start the email server
            self.mail_server = smtplib.SMTP(self.host___)


            COMMASPACE = ', '
            # create the message
            message = MIMEText(self.body___, 'html')
            message['From'] = self.from___
            message['To'] = COMMASPACE.join(self.to___)
            message['Subject'] = self.subject___

            # send the email
            self.mail_server.sendmail(message['From'], message['To'], message.as_string())
        except:
            print("email error")
        finally:
            # close the message server
            self.mail_server.quit()
