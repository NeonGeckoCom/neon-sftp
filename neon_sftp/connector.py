# NEON AI (TM) SOFTWARE, Software Development Kit & Application Framework
# All trademark and other rights reserved by their respective owners
# Copyright 2008-2022 Neongecko.com Inc.
# Contributors: Daniel McKnight, Guy Daniels, Elon Gasper, Richard Leeds,
# Regina Bloomstine, Casimiro Ferreira, Andrii Pernatii, Kirill Hrymailo
# BSD-3 License
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 1. Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
# 3. Neither the name of the copyright holder nor the names of its
#    contributors may be used to endorse or promote products derived from this
#    software without specific prior written permission.
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
# THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
# CONTRIBUTORS  BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
# OR PROFITS;  OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE,  EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
import io
import logging
import os
from typing import Union

from paramiko import SFTPClient, Transport

from .utils.format_utils import str_to_bytes_io

logger = logging.getLogger(__name__)


class NeonSFTPConnector:

    def __init__(self, host, username, passphrase, root_path: str = '/', port: int = 22):
        self.host = host
        self.username = username
        self.passphrase = passphrase
        self._connection = None
        self._transport = None
        self.port = port
        self.root_path = root_path

    @property
    def transport(self):
        if not (self._transport and self._transport.is_active()):
            self._transport = Transport((self.host, self.port))
            self._transport.connect(username=self.username, password=self.passphrase)
        return self._transport

    @property
    def connection(self) -> SFTPClient:
        """Establishes SFTP connection"""
        if not (self._connection and not self._connection.sock.closed):
            self._connection = SFTPClient.from_transport(self.transport)
            if self.root_path:
                self.change_dir(path='/')
                self.change_dir(path=os.path.expanduser(self.root_path))
        return self._connection

    def change_dir(self, path: str):
        """
            Checkout desired path
            :param path: path string to checkout (relative or absolute)
        """
        self.connection.chdir(path=path)

    def get_file(self, get_from: str, save_to: str, prefetch: bool = True) -> bool:
        """Gets file from remote host and stores it locally"""
        remote_path = self.root_path + '/' + get_from
        try:
            self.connection.get(remotepath=remote_path, localpath=save_to, prefetch=prefetch)
            operation_success = True
        except FileNotFoundError:
            logger.error(f'Failed to get file from remote path: "{remote_path}" (remote file not found)')
            operation_success = False
        return operation_success

    def get_file_object(self, get_from: str,
                        file_object: io.BytesIO = None,
                        callback_function: callable = None,
                        prefetch: bool = True) -> io.BytesIO:
        """
            Gets file from remote host and stores it into file object

            :param get_from: source location
            :param file_object: desired streaming object to fulfill (creates new one if not provided)
            :param callback_function: function of type foo(int, int) that handles sequential data population
            :param prefetch: whether prefetching should be performed

            :returns fulfilled buffer
        """
        if not file_object:
            file_object = io.BytesIO()
        remote_path = self.root_path + '/' + get_from
        try:
            num_bytes = self.connection.getfo(remotepath=remote_path,
                                              fl=file_object,
                                              callback=callback_function,
                                              prefetch=prefetch)
            if num_bytes == 0:
                logger.warning(f'Empty result buffer for remote_path="{remote_path}"')
        except FileNotFoundError:
            logger.error(f'Failed to get file from remote path: "{remote_path}" (file not found)')
        file_object.seek(0)
        return file_object

    def make_dir(self, new_path: os.PathLike):
        """ Makes dir in remote file system if not exists """
        try:
            self.connection.mkdir(path=new_path)
        except FileExistsError:
            logger.info('Remote dir already exists')

    def put_file(self, get_from: os.PathLike, save_to: os.PathLike):
        """Gets file from local host and stores it remotely"""
        try:
            remote_path = self.root_path + '/' + save_to
            stats = self.connection.put(remotepath=remote_path, localpath=get_from)
            logger.info(f'Successfully stored file object to remote_path: {remote_path}, stats - {stats}')
            return stats
        except FileNotFoundError:
            logger.error(f'Failed to get file from local path: "{get_from}" (file not found)')

    def put_file_object(self, file_object: Union[io.BytesIO, str], save_to: os.PathLike):
        """
            Stores file object from local host and stores it remotely

            :param file_object: source bytes io buffer or base64 encoded string (in case string type detected - creates buffer out of it)
            :param save_to: remote path to save buffered value to

            :returns SFTPAttributes if buffer is not empty, None otherwise
        """
        if isinstance(file_object, str):
            file_object = str_to_bytes_io(file_object)
        if file_object.getbuffer().nbytes > 0:
            remote_path = self.root_path + '/' + save_to
            stats = self.connection.putfo(fl=file_object, remotepath=remote_path)
            logger.info(f'Successfully stored file object to remote_path: {remote_path}, stats - {stats}')
            return stats
        else:
            logger.warning('Empty buffer provided, saving operation aborted')

    def remove(self, remote_path: os.PathLike) -> bool:
        """ Removes file from remote path """
        try:
            remote_path = self.root_path + '/' + remote_path
            self.connection.remove(path=remote_path)
            operation_success = True
        except FileNotFoundError:
            logger.error(f'Failed to delete file "{remote_path}"')
            operation_success = False
        return operation_success

    def shut_down(self):
        """ Gracefully shuts down SFTP connection """
        self.transport.close()
        self.connection.close()
