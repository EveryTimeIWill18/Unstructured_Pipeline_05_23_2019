"""
hive_server
~~~~~~~~~~~
"""
import os
import socks
import socket
import paramiko
from unstructured_data_pipeline.interfaces.connection_interfaces import SftpConnectionInterface
from unstructured_data_pipeline.interfaces.connection_interfaces import SSHConnectionInterface
from unstructured_data_pipeline.configs.config_dms_claims import dms_config
from unstructured_data_pipeline.configs.config_personal_umbrella import pu_config
from unstructured_data_pipeline.configs.config_sa_claims import sa_config
from unstructured_data_pipeline.concrete_file_preprocessors import d
# SSHConnection(Parent Class) #
####################################################################################################
class SftpConnection(object):
    """Creates an sftp connection"""
    def __init__(self, project: str):
        # Personal Umbrella setup
        self.sock: socks.socksocket = socks.socksocket()
        self.sftp: paramiko.Transport = None
        self.client: paramiko.SFTPClient.from_transport = None
        self.is_connected: bool = False
        self.sftp_connected: bool = False
        self.client_connected: bool = False

        if project == 'personal_umbrella':
            self.__dict__['project'] = 'personal_umbrella'
            self.__dict__['pickle_path'] = pu_config.get_pickle_path
            self.__dict__['mapping_path'] = pu_config.get_mapping_path
            self.__dict__['remote_paths'] = pu_config.get_remote_paths
            self.username = pu_config.get_sftp['username']
            self.password = pu_config.get_sftp['password']
            self.host = pu_config.get_sftp['host']
            self.port = pu_config.get_sftp['port']

        # DMS Claims setup
        elif project == 'dms_claims':
            self.__dict__['project'] = 'dms_claims'
            self.__dict__['pickle_path'] = dms_config.get_pickle_path
            self.__dict__['mapping_path'] = dms_config.get_mapping_path
            self.__dict__['remote_paths'] = dms_config.get_remote_paths
            self.username = dms_config.get_sftp['username']
            self.password = dms_config.get_sftp['password']
            self.host = dms_config.get_sftp['host']
            self.port = dms_config.get_sftp['port']

        # SA Claims setup
        elif project == 'sa_claims':
            self.__dict__['project'] = 'sa_claims'
            self.__dict__['pickle_path'] = sa_config.get_pickle_path
            self.__dict__['mapping_path'] = sa_config.get_mapping_path
            self.__dict__['remote_paths'] = sa_config.get_remote_paths
            self.username = sa_config.get_sftp['username']
            self.password = sa_config.get_sftp['password']
            self.host = sa_config.get_sftp['host']
            self.port = sa_config.get_sftp['port']

        else:
            raise ValueError(f'ValueError: {project} not a valid project name')


class SSHConnection(SSHConnectionInterface):
    """Creates an ssh connection"""
    def __init__(self):
        self.username = dms_config.get_sftp['username']
        self.password = dms_config.get_sftp['password']
        self.host = dms_config.get_sftp['host']
        self.port = dms_config.get_sftp['port']
        self.ssh = paramiko.SSHClient()
        self.connected = False

    def connect(self):
        """connect to a remote server via ssh"""
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        self.ssh.connect(
            hostname=self.host,
            username=self.username,
            password=self.password
        )
        if socket.gethostname():
            self.connected = True
            print(socket.gethostname())
            print(socket.getfqdn())
            print(socket.gethostbyaddr(self.host))






# Child Classes #
####################################################################################################
class RunRemoteScript(SSHConnection):
    def __init__(self):
        super().__init__()
        self.query_string: str = None

        # connect to the remote server
        self.connect()

    def run_script(self, path_to_script, script_name):
        """Run a remote python script"""
        try:
            if not self.connected:
                self.connect()

            script_ = path_to_script + '/' + script_name + '.py'
            cmd = f'/home/wmurphy/will_env/bin/python2.7 {script_}'

            # run the remote python script
            stdin_, stdout_, stderr_ = self.ssh.exec_command(cmd)
            stdout_.channel.recv_exit_status()
            lines = stdout_.readlines()
            # check query output
            for line in lines:
                print(line)
        except:
            print('An error has occurred.')

class SftpData(SftpConnectionInterface, SftpConnection):
    def __init__(self, project: str, file_ext: str, file_type: str):
        SftpConnection.__init__(self, project=project)
        self.file_ext: str = file_ext
        self.file_type: str = file_type

        print(f"Starting Sftp Data Transfer ...")

    def connect(self):
        """connect to the remote server"""
        try:
            print(f"IN SftpData")
            # set proxy connection values
            self.sock.set_proxy(
                proxy_type=None,
                addr=self.host,
                port=self.port,
                username=self.username,
                password=self.password
            )
            self.sock.connect((self.host, int(self.port)))
            if socket.gethostname():
                self.is_connected = True
                print("Connection Successful:")
                print(f"HOST: {socket.gethostname()}")
                print(f"HOST FULL QUALIFIED NAME: {socket.getfqdn()}")
                print(f"HOST ADDRESS LIST: {socket.gethostbyaddr(self.host)}")
                # create transport
                self.sftp = paramiko.Transport(self.sock)
                try:
                    # connect sftp transport
                    self.sftp.connect(
                        username=self.username,
                        password=self.password
                    )
                    # check if connection is live
                    if self.sftp.is_alive():
                        print("Transport is live")
                        self.sftp_connected = True
                        # create client and connect
                        try:
                            self.client = paramiko.SFTPClient.from_transport(self.sftp)
                            print(f"Client is: {self.client}")
                            self.client_connected = True

                            # BLOCK: Sftp the pickle file
                            if self.file_type == 'pickle':
                                os.chdir(self.__dict__['pickle_path'])
                                f = self.file_ext.title() + '_' + d + '.' + self.file_type
                                self.transfer_payload(
                                    filepath=self.__dict__['pickle_path'],
                                    filename=f,
                                    remote_sub_dir='remote_pickle_path'
                                )

                            # BLOCK: Sftp the mapping file
                            if self.file_type == 'csv':
                                os.chdir(self.__dict__['mapping_path'])
                                f = self.file_ext.title() + 'MappingFile' + '.' + self.file_type
                                self.transfer_payload(
                                    filepath=self.__dict__['mapping_path'],
                                    filename=f,
                                    remote_sub_dir='remote_mapping_path'
                                )
                        except Exception as e:
                            print('SftpTransferError: Could not sftp the file.')
                            print(e)
                except Exception as e:
                    print(e)
                    print("An sftp error has occurred.")
        except Exception as e:
            print(e)
            print("A socket error occurred.")
        finally:
            # close the sftp connections
            self.sftp.close()
            self.client.close()
            self.sock.close()


    def transfer_payload(self, filename: str, filepath: str, remote_sub_dir: str):
        """Transport data to the remote server"""
        if self.sftp_connected and self.client_connected:
            try:
                while True:
                    os.chdir(filepath)
                    payload = filename
                    destination = self.__dict__['remote_paths'][f'{remote_sub_dir}'] + '/' + filename

                    self.client.put(
                        localpath=payload,
                        remotepath=destination
                    )
                    print(f"Successfully loaded {filename} to {destination}")
                    break
            except Exception as e:
                print(e)
                print("An error occurred while transporting payload")
