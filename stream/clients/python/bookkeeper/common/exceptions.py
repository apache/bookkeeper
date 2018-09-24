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

"""Exceptions raised by bookkeeper clients.
This module provides base classes for all errors raised by libraries based
on :mod:`bookkeeper.common`.
"""

from __future__ import absolute_import
from __future__ import unicode_literals
from bookkeeper.proto.storage_pb2 import BAD_REQUEST
from bookkeeper.proto.storage_pb2 import INTERNAL_SERVER_ERROR

import six

try:
    import grpc
except ImportError:  # pragma: NO COVER
    grpc = None

# Lookup tables for mapping exceptions from gRPC transports.
# Populated by _APICallErrorMeta
_GRPC_CODE_TO_EXCEPTION = {}


class BKError(Exception):
    """Base class for all exceptions raised by bookkeeper Clients."""
    pass


@six.python_2_unicode_compatible
class RetryError(BKError):
    """Raised when a function has exhausted all of its available retries.
    Args:
        message (str): The exception message.
        cause (Exception): The last exception raised when retring the
            function.
    """
    def __init__(self, message, cause):
        super(RetryError, self).__init__(message)
        self.message = message
        self._cause = cause

    @property
    def cause(self):
        """The last exception raised when retrying the function."""
        return self._cause

    def __str__(self):
        return '{}, last exception: {}'.format(self.message, self.cause)


class _BKGRpcCallErrorMeta(type):
    """Metaclass for registering BKGRpcCallError subclasses."""
    def __new__(mcs, name, bases, class_dict):
        cls = type.__new__(mcs, name, bases, class_dict)
        if cls.grpc_status_code is not None:
            _GRPC_CODE_TO_EXCEPTION.setdefault(cls.grpc_status_code, cls)
        return cls


@six.python_2_unicode_compatible
@six.add_metaclass(_BKGRpcCallErrorMeta)
class BKGrpcCallError(BKError):
    """Base class for exceptions raised by calling API methods.
    Args:
        message (str): The exception message.
        errors (Sequence[Any]): An optional list of error details.
        response (Union[requests.Request, grpc.Call]): The response or
            gRPC call metadata.
    """

    grpc_status_code = None
    """Optional[grpc.StatusCode]: The gRPC status code associated with this
    error.
    This may be ``None`` if the exception does not match up to a gRPC error.
    """

    bk_status_code = None
    """Optional[bookkeeper.proto.StatusCode]: The bookkeeper storage status code
    associated with this error.
    This may be ```None` if the exception is a gRPC channel error.
    """

    def __init__(self, message, errors=(), response=None):
        super(BKGrpcCallError, self).__init__(message)
        self.message = message
        """str: The exception message."""
        self._errors = errors
        self._response = response

    def __str__(self):
        return '{} {}'.format(self.code, self.message)

    @property
    def errors(self):
        """Detailed error information.
        Returns:
            Sequence[Any]: A list of additional error details.
        """
        return list(self._errors)

    @property
    def response(self):
        """Optional[Union[requests.Request, grpc.Call]]: The response or
        gRPC call metadata."""
        return self._response


class ClientError(BKGrpcCallError):
    """Base class for all client error responses."""


class BadRequest(ClientError):
    """Exception mapping a ``400 Bad Request`` response."""
    code = BAD_REQUEST


class ServerError(BKGrpcCallError):
    """Base for 5xx responses."""


class InternalServerError(ServerError):
    """Exception mapping a ``500 Internal Server Error`` response. or a
    :attr:`grpc.StatusCode.INTERNAL` error."""
    code = INTERNAL_SERVER_ERROR
    grpc_status_code = grpc.StatusCode.INTERNAL if grpc is not None else None


def exception_class_for_grpc_status(status_code):
    """Return the exception class for a specific :class:`grpc.StatusCode`.
    Args:
        status_code (grpc.StatusCode): The gRPC status code.
    Returns:
        :func:`type`: the appropriate subclass of :class:`BKGrpcCallError`.
    """
    return _GRPC_CODE_TO_EXCEPTION.get(status_code, BKGrpcCallError)


def from_grpc_status(status_code, message, **kwargs):
    """Create a :class:`BKGrpcCallError` from a :class:`grpc.StatusCode`.
    Args:
        status_code (grpc.StatusCode): The gRPC status code.
        message (str): The exception message.
        kwargs: Additional arguments passed to the :class:`BKGrpcCallError`
            constructor.
    Returns:
        BKGrpcCallError: An instance of the appropriate subclass of
            :class:`BKGrpcCallError`.
    """
    error_class = exception_class_for_grpc_status(status_code)
    error = error_class(message, **kwargs)

    if error.grpc_status_code is None:
        error.grpc_status_code = status_code

    return error


def from_grpc_error(rpc_exc):
    """Create a :class:`BKGrpcCallError` from a :class:`grpc.RpcError`.
    Args:
        rpc_exc (grpc.RpcError): The gRPC error.
    Returns:
        BKGrpcCallError: An instance of the appropriate subclass of
            :class:`BKGrpcError`.
    """
    if isinstance(rpc_exc, grpc.Call):
        return from_grpc_status(
            rpc_exc.code(),
            rpc_exc.details(),
            errors=(rpc_exc,),
            response=rpc_exc)
    else:
        return BKGrpcCallError(
            str(rpc_exc), errors=(rpc_exc,), response=rpc_exc)
