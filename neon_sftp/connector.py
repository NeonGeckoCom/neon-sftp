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
import os

from paramiko import SFTPClient, Transport


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
        if not self._transport:
            self._transport = Transport((self.host, self.port))
            self._transport.connect(username=self.username, password=self.passphrase)
        return self._transport

    @property
    def connection(self) -> SFTPClient:
        """Establishes SFTP connection"""
        if not self._connection:
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

    def get_file(self, get_from: str, save_to: str, prefetch: bool = True):
        """Gets file from remote host and stores it locally"""
        self.connection.get(remotepath=self.root_path + '/' + get_from, localpath=save_to, prefetch=prefetch)

    def get_file_object(self, get_from: str,
                        file_object: str = io.BytesIO(),
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
        self.connection.getfo(remotepath=self.root_path + '/' + get_from,
                              fl=file_object,
                              callback=callback_function,
                              prefetch=prefetch)
        return file_object

    def put_file(self, get_from: str, save_to: str):
        """Gets file from local host and stores it remotely"""
        self.connection.put(remotepath=self.root_path + '/' + save_to, localpath=get_from)
