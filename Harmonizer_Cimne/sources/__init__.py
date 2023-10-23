import json
import os
import pkgutil
import select
import subprocess
import sys
import tarfile
from tempfile import TemporaryFile

from kubernetes import client, config as kubeconfig
from kubernetes.stream import stream
from kubernetes.stream.ws_client import STDOUT_CHANNEL, STDERR_CHANNEL
from utils import utils
from datetime import datetime

from websocket import ABNF


class SourcePlugin(object):
    source_name = None

    def __init__(self, settings=None):
        self.settings = settings
        self.config = utils.read_config(settings.conf_file)
        self.config['source'] = self.source_name

    def gather(self, arguments):
        raise NotImplementedError

    def harmonizer_command_line(self, arguments):
        raise NotImplementedError

    def get_mapper(self, message):
        raise NotImplementedError

    def get_kwargs(self, message):
        raise NotImplementedError

    def get_store_table(self, message):
        raise NotImplementedError

    def transform_df(self, df):
        return df


class ManagedFolder(object):
    def __init__(self, config, namespace, deployment, auto, dest_path=None):
        if auto:
            self.dest_path = dest_path
            self.kubernetes = KubeManagement(config, deployment, namespace)
        else:
            self.kubernetes = None
        self.auto = auto

    def get_not_processed_path(self, path):
        if self.auto:
            self.kubernetes.download_folder(path, ManagedFile.NOT_PROCESSED, self.dest_path)
            subprocess.run(['tar', '-xf', f'{self.dest_path}/{ManagedFile.NOT_PROCESSED}.tar', '-C', self.dest_path])
            return f"{self.dest_path}/{ManagedFile.NOT_PROCESSED}"
        else:
            return path

    def makedirs(self, path):
        self.kubernetes.makedirs(path)

    def mv_file(self, file_old, file_new):
        self.kubernetes.mv_file(file_old, file_new)


class ManagedFile(object):
    NOT_PROCESSED = 'not_processed'
    PROCESSING = 'processing'
    PROCESSED = 'processed'
    FAILED = 'failed'

    @staticmethod
    def normalize_path(path):
        return path if not path.endswith('/') else path[:-1]

    def __init__(self, path, current_file, managed_folder):
        if managed_folder.auto:
            self.managed_file_path = f"{ManagedFile.normalize_path(path)}"
            self.local_file_path = f"{managed_folder.dest_path}/{ManagedFile.NOT_PROCESSED}"
        else:
            self.managed_file_path = ""
            self.local_file_path = path
        self.current_file = current_file
        self.local_file = current_file
        self.status = ManagedFile.NOT_PROCESSED
        self.managed_folder = managed_folder

    def get_info(self, namespace, _id, user):
        if not self.managed_folder.auto:
            return [namespace, _id, user]
        else:
            uri = self.current_file.split("#~#")[1].split('#')
            namespace = uri[0].replace("@", '/') + '#'
            element = uri[1]
            user = namespace.split(".")[0].split("https://")[1]
            return [namespace, element, user]

    def get_local_file(self):
        return f'{self.local_file_path}/{self.local_file}'

    def get_managed_file(self, status=None, new_file=None):
        return f'{self.managed_file_path}/{status if status else self.status}/' \
               f'{new_file if new_file else self.current_file}'

    def set_status(self, status):
        if not self.managed_folder.auto:
            return
        valid_changes = [(ManagedFile.NOT_PROCESSED, ManagedFile.PROCESSING),
                         (ManagedFile.PROCESSING, ManagedFile.PROCESSED), (ManagedFile.PROCESSING, ManagedFile.FAILED)]
        valid = False
        for old_s, new_s in valid_changes:
            if old_s == self.status and new_s == status:
                valid = True
        if not valid:
            raise Exception(f"File {self.current_file} status change is not valid")
        uid, node, created, processed, name = self.current_file.split("#~#")
        o_path = self.get_managed_file()
        if self.status == ManagedFile.PROCESSING:
            processed = str(int(datetime.now().timestamp()*1000))
        new_file = "#~#".join([uid, node, created, processed, name])
        n_path = self.get_managed_file(status, new_file)
        self.managed_folder.makedirs("/".join(n_path.split("/")[:-1]))
        self.managed_folder.mv_file(o_path, n_path)
        self.current_file = new_file
        self.status = status


class WSFileManager:
    """
    WS wrapper to manage read and write bytes in K8s WSClient
    """
    def __init__(self, ws_client):
        """
        :param wsclient: Kubernetes WSClient
        """
        self.ws_client = ws_client
    def read_bytes(self, timeout=0):
        """
        Read slice of bytes from stream
        :param timeout: read timeout
        :return: stdout, stderr and closed stream flag
        """
        stdout_bytes = None
        stderr_bytes = None
        if self.ws_client.is_open():
            if not self.ws_client.sock.connected:
                self.ws_client._connected = False
            else:
                r, _, _ = select.select(
                    (self.ws_client.sock.sock, ), (), (), timeout)
                if r:
                    op_code, frame = self.ws_client.sock.recv_data_frame(True)
                    if op_code == ABNF.OPCODE_CLOSE:
                        self.ws_client._connected = False
                    elif op_code == ABNF.OPCODE_BINARY or op_code == ABNF.OPCODE_TEXT:
                        data = frame.data
                        if len(data) > 1:
                            channel = data[0]
                            data = data[1:]
                            if data:
                                if channel == STDOUT_CHANNEL:
                                    stdout_bytes = data
                                elif channel == STDERR_CHANNEL:
                                    stderr_bytes = data
        return stdout_bytes, stderr_bytes, not self.ws_client._connected


class KubeManagement(object):
    def __init__(self, config, name, namespace):
        self.conf_dict = config['kubeconfig']
        kubeconfig.kube_config.load_kube_config_from_dict(self.conf_dict)
        self.k8s_core_v1 = client.CoreV1Api()
        for p in self.k8s_core_v1.list_namespaced_pod(namespace).items:
            p = p.to_dict()
            if p['metadata']['name'].startswith(name):
                pod = p['metadata']['name']
                break
        self.pod = pod
        self.namespace = namespace

    def download_folder(self, path, folder, dest_path):
        command = ['/bin/sh']
        stream1 = stream(self.k8s_core_v1.connect_get_namespaced_pod_exec,
                         self.pod, self.namespace,
                         command=command, stderr=True, stdin=True,
                         stdout=True, tty=False, _preload_content=False)
        command1 = f'cd {path}'
        stream1.write_stdin(command1 + "\n")
        command2 = f'tar -cf {folder}.tar {folder}'
        stream1.write_stdin(command2 + "\n")
        stream1.update(timeout=10)
        command3 = ["cat", f'{path}/{folder}.tar']
        resp = stream(self.k8s_core_v1.connect_get_namespaced_pod_exec,
                      self.pod, self.namespace,
                      command=command3, stderr=True, stdin=True,
                      stdout=True, tty=False, _preload_content=False)
        try:
            with open(f'{dest_path}/{folder}.tar', 'wb') as tar_buffer:
                reader = WSFileManager(resp)
                while True:
                    out, err, closed = reader.read_bytes()
                    if out:
                        tar_buffer.write(out)
                    elif err:
                        print(err)
                    if closed:
                        break
                resp.close()
                tar_buffer.flush()
        except Exception as e:
            print(e)
        command4 = f"rm {folder}.tar"
        stream1.write_stdin(command4 + "\n")
        stream1.close()

    def makedirs(self, path):
        command = ['mkdir', '-m', '777', path]
        stream1 = stream(self.k8s_core_v1.connect_get_namespaced_pod_exec,
                         self.pod, self.namespace,
                         command=command, stderr=True, stdin=True,
                         stdout=True, tty=False, _preload_content=False)
        stream1.update(timeout=10)
        stream1.close()

    def mv_file(self, file_old, file_new):
        command = ['mv', file_old, file_new]
        stream1 = stream(self.k8s_core_v1.connect_get_namespaced_pod_exec,
                         self.pod, self.namespace,
                         command=command, stderr=True, stdin=True,
                         stdout=True, tty=False, _preload_content=False)
        stream1.update(timeout=10)
        stream1.close()






