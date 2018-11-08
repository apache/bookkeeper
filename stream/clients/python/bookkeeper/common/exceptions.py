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
from bookkeeper.proto import storage_pb2

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
        return 'grpc_status_code = {}, bk_status_code = {} : {}'\
            .format(self.grpc_status_code, self.bk_status_code, self.message)

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
    code = storage_pb2.BAD_REQUEST
    grpc_status_code =\
        grpc.StatusCode.FAILED_PRECONDITION if grpc is not None else None


class IllegalOpError(ClientError):
    """Exception mapping a ``403 Illegal Op`` response."""
    code = storage_pb2.ILLEGAL_OP
    grpc_status_code =\
        grpc.StatusCode.FAILED_PRECONDITION if grpc is not None else None


class ServerError(BKGrpcCallError):
    """Base for 5xx responses."""


class StorageContainerNotFoundError(BKGrpcCallError):
    """Exception raised when storage container is not found"""
    code = None
    grpc_status_code =\
        grpc.StatusCode.NOT_FOUND if grpc is not None else None


class InternalServerError(ServerError):
    """Exception mapping a ``500 Internal Server Error`` response. or a
    :attr:`grpc.StatusCode.INTERNAL` error."""
    code = storage_pb2.INTERNAL_SERVER_ERROR
    grpc_status_code =\
        grpc.StatusCode.INTERNAL if grpc is not None else None


class UnimplementedError(ServerError):
    code = storage_pb2.NOT_IMPLEMENTED
    grpc_status_code =\
        grpc.StatusCode.UNIMPLEMENTED if grpc is not None else None


class UnexpectedError(BKGrpcCallError):
    code = storage_pb2.UNEXPECTED
    grpc_status_code =\
        grpc.StatusCode.UNKNOWN if grpc is not None else None


class StorageError(BKGrpcCallError):
    grpc_status_code = None


class FailureError(StorageError):
    code = storage_pb2.FAILURE


class BadVersionError(StorageError):
    code = storage_pb2.BAD_VERSION


class BadRevisionError(StorageError):
    code = storage_pb2.BAD_REVISION


class NamespaceError(StorageError):
    code = storage_pb2.UNEXPECTED


class InvalidNamespaceNameError(NamespaceError):
    code = storage_pb2.INVALID_NAMESPACE_NAME


class NamespaceNotFoundError(NamespaceError):
    code = storage_pb2.NAMESPACE_NOT_FOUND


class NamespaceExistsError(NamespaceError):
    code = storage_pb2.NAMESPACE_EXISTS


class StreamError(StorageError):
    code = storage_pb2.UNEXPECTED


class InvalidStreamNameError(StreamError):
    code = storage_pb2.INVALID_STREAM_NAME


class StreamNotFoundError(StreamError):
    code = storage_pb2.STREAM_NOT_FOUND


class StreamExistsError(StreamError):
    code = storage_pb2.STREAM_EXISTS


class TableError(StorageError):
    code = storage_pb2.UNEXPECTED


class InvalidKeyError(TableError):
    code = storage_pb2.INVALID_KEY


class KeyNotFoundError(TableError):
    code = storage_pb2.KEY_NOT_FOUND


class KeyExistsError(TableError):
    code = storage_pb2.KEY_EXISTS


_BK_CODE_TO_EXCEPTION_ = {
    storage_pb2.FAILURE: FailureError,
    storage_pb2.BAD_REQUEST: BadRequest,
    storage_pb2.ILLEGAL_OP: IllegalOpError,
    storage_pb2.INTERNAL_SERVER_ERROR: InternalServerError,
    storage_pb2.NOT_IMPLEMENTED: UnimplementedError,
    storage_pb2.UNEXPECTED: UnexpectedError,
    storage_pb2.BAD_VERSION: BadVersionError,
    storage_pb2.BAD_REVISION: BadRevisionError,
    storage_pb2.INVALID_NAMESPACE_NAME: InvalidNamespaceNameError,
    storage_pb2.NAMESPACE_EXISTS: NamespaceExistsError,
    storage_pb2.NAMESPACE_NOT_FOUND: NamespaceNotFoundError,
    storage_pb2.INVALID_STREAM_NAME: InvalidStreamNameError,
    storage_pb2.STREAM_EXISTS: StreamExistsError,
    storage_pb2.STREAM_NOT_FOUND: StreamNotFoundError,
    storage_pb2.INVALID_KEY: InvalidKeyError,
    storage_pb2.KEY_EXISTS: KeyExistsError,
    storage_pb2.KEY_NOT_FOUND: KeyNotFoundError
}


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
    elif isinstance(rpc_exc, grpc.RpcError):
        return from_grpc_error(
            rpc_exc.code(),
            rpc_exc.details(),
            errors=(rpc_exc,),
            response=rpc_exc
        )
    else:
        return BKGrpcCallError(
            str(rpc_exc), errors=(rpc_exc,), response=rpc_exc)


def exception_class_for_bk_status_code(status_code):
    return _BK_CODE_TO_EXCEPTION_.get(status_code, BKGrpcCallError)


def from_bk_status(status_code, message, **kwargs):
    error_class = exception_class_for_bk_status_code(status_code)
    error = error_class(message, **kwargs)

    if error.bk_status_code is None:
        error.bk_status_code = status_code

    return error


def from_table_rpc_response(rpc_resp):
    routing_header = rpc_resp.header
    status_code = routing_header.code
    if storage_pb2.SUCCESS == status_code:
        return rpc_resp
    else:
        raise from_bk_status(
            status_code,
            "",
            errors=(rpc_resp,),
            response=rpc_resp)


def from_root_range_rpc_response(rpc_resp):
    status_code = rpc_resp.code
    if storage_pb2.SUCCESS == status_code:
        return rpc_resp
    else:
        raise from_bk_status(
            status_code,
            "",
            errors=(rpc_resp,),
            response=rpc_resp)
