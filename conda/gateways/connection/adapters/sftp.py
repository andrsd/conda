# Copyright (C) 2012 Anaconda, Inc
# SPDX-License-Identifier: BSD-3-Clause

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import absolute_import, division, print_function, unicode_literals

import pysftp
from io import BytesIO
from logging import getLogger

from .. import BaseAdapter, Response, dispatch_hook
from ....common.url import urlparse

log = getLogger(__name__)


class SFTPAdapter(BaseAdapter):
    """A Requests Transport Adapter that handles SFTP urls."""
    def __init__(self):
        super(SFTPAdapter, self).__init__()

        # Build a dictionary keyed off the methods we support in upper case.
        # The values of this dictionary should be the functions we use to
        # send the specific queries.
        self.func_table = {'LIST': self.list,
                           'RETR': self.retr,
                           'STOR': self.stor,
                           'NLST': self.nlst,
                           'GET': self.retr}

    def send(self, request, **kwargs):
        """Sends a PreparedRequest object over FTP. Returns a response object."""
        # Get the authentication from the prepared request, if any.
        parsed = urlparse(request.url)

        path = parsed.path
        # If there is a slash on the front of the path, chuck it.
        if path[0] == '/':
            path = path[1:]

        port = parsed.port or 22
        # Establish the connection and login if needed.
        self.conn = pysftp.Connection(parsed.hostname, port=port, username=parsed.username, password=parsed.password)

        # Get the method and attempt to find the function to call.
        resp = self.func_table[request.method](path, request)

        # Return the response.
        return resp

    def close(self):
        """Dispose of any internal state."""
        # Currently this is a no-op.
        pass

    def list(self, path, request):
        """Executes the FTP LIST command on the given path."""
        raise SystemExit("LIST not implemented yet")

    def retr(self, path, request):
        """Executes the FTP RETR command on the given path."""
        data = BytesIO()

        # To ensure the BytesIO gets cleaned up, we need to alias its close
        # method. See self.list().
        data.release_conn = data.close

        try:
            self.conn.getfo(path, data)
            response = build_binary_response(request, data, "226")
        except (OSError, IOError) as e:
            log.warn("Failed to GET file '%s', errno = %d", path, e.errno)
            response = None

        # Close the connection.
        self.conn.close()

        return response

    def stor(self, path, request):
        """Executes the FTP STOR command on the given path."""
        raise SystemExit("STOR not implemented yet")

    def nlst(self, path, request):
        """Executes the FTP NLST command on the given path."""
        raise SystemExit("NLST not implemented yet")


def build_binary_response(request, data, code):
    """Build a response for data whose encoding is unknown."""
    return build_response(request, data, code,  None)


def build_response(request, data, code, encoding):
    """Builds a response object from the data returned by ftplib, using the
    specified encoding."""
    response = Response()

    response.encoding = encoding

    # Fill in some useful fields.
    response.raw = data
    response.url = request.url
    response.request = request
    response.status_code = get_status_code_from_code_response(code)

    # Make sure to seek the file-like raw object back to the start.
    response.raw.seek(0)

    # Run the response hook.
    response = dispatch_hook('response', request.hooks, response)
    return response


def get_status_code_from_code_response(code):
    """
    The idea is to handle complicated code response (even multi lines).
    We get the status code in two ways:
    - extracting the code from the last valid line in the response
    - getting it from the 3 first digits in the code
    After a comparison between the two values,
    we can safely set the code or raise a warning.
    Examples:
        - get_status_code_from_code_response('200 Welcome') == 200
        - multi_line_code = '226-File successfully transferred\n226 0.000 seconds'
          get_status_code_from_code_response(multi_line_code) == 226
        - multi_line_with_code_conflicts = '200-File successfully transferred\n226 0.000 seconds'
          get_status_code_from_code_response(multi_line_with_code_conflicts) == 226
    For more detail see RFC 959, page 36, on multi-line responses:
        https://www.ietf.org/rfc/rfc959.txt
        "Thus the format for multi-line replies is that the first line
         will begin with the exact required reply code, followed
         immediately by a Hyphen, "-" (also known as Minus), followed by
         text.  The last line will begin with the same code, followed
         immediately by Space <SP>, optionally some text, and the Telnet
         end-of-line code."
    """
    last_valid_line_from_code = [line for line in code.split('\n') if line][-1]
    status_code_from_last_line = int(last_valid_line_from_code.split()[0])
    status_code_from_first_digits = int(code[:3])
    if status_code_from_last_line != status_code_from_first_digits:
        log.warning(
            'FTP response status code seems to be inconsistent.\n'
            'Code received: %s, extracted: %s and %s',
            code,
            status_code_from_last_line,
            status_code_from_first_digits
        )
    return status_code_from_last_line
