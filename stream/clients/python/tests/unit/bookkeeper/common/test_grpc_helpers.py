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

import grpc
import mock
import pytest

from bookkeeper.common import exceptions
from bookkeeper.common import grpc_helpers


def test__patch_callable_name():
    callable = mock.Mock(spec=['__class__'])
    callable.__class__ = mock.Mock(spec=['__name__'])
    callable.__class__.__name__ = 'TestCallable'

    grpc_helpers._patch_callable_name(callable)

    assert callable.__name__ == 'TestCallable'


def test__patch_callable_name_no_op():
    callable = mock.Mock(spec=['__name__'])
    callable.__name__ = 'test_callable'

    grpc_helpers._patch_callable_name(callable)

    assert callable.__name__ == 'test_callable'


class RpcErrorImpl(grpc.RpcError, grpc.Call):
    def __init__(self, code):
        super(RpcErrorImpl, self).__init__()
        self._code = code

    def code(self):
        return self._code

    def details(self):
        return None


def test_wrap_unary_errors():
    grpc_error = RpcErrorImpl(grpc.StatusCode.INTERNAL)
    callable_ = mock.Mock(spec=['__call__'], side_effect=grpc_error)

    wrapped_callable = grpc_helpers._wrap_unary_errors(callable_)

    with pytest.raises(exceptions.InternalServerError) as exc_info:
        wrapped_callable(1, 2, three='four')

    callable_.assert_called_once_with(1, 2, three='four')
    assert exc_info.value.response == grpc_error


def test_wrap_stream_okay():
    expected_responses = [1, 2, 3]
    callable_ = mock.Mock(spec=[
        '__call__'], return_value=iter(expected_responses))

    wrapped_callable = grpc_helpers._wrap_stream_errors(callable_)

    got_iterator = wrapped_callable(1, 2, three='four')

    responses = list(got_iterator)

    callable_.assert_called_once_with(1, 2, three='four')
    assert responses == expected_responses


def test_wrap_stream_iterable_iterface():
    response_iter = mock.create_autospec(grpc.Call, instance=True)
    callable_ = mock.Mock(spec=['__call__'], return_value=response_iter)

    wrapped_callable = grpc_helpers._wrap_stream_errors(callable_)

    got_iterator = wrapped_callable()

    callable_.assert_called_once_with()

    # Check each aliased method in the grpc.Call interface
    got_iterator.add_callback(mock.sentinel.callback)
    response_iter.add_callback.assert_called_once_with(mock.sentinel.callback)

    got_iterator.cancel()
    response_iter.cancel.assert_called_once_with()

    got_iterator.code()
    response_iter.code.assert_called_once_with()

    got_iterator.details()
    response_iter.details.assert_called_once_with()

    got_iterator.initial_metadata()
    response_iter.initial_metadata.assert_called_once_with()

    got_iterator.is_active()
    response_iter.is_active.assert_called_once_with()

    got_iterator.time_remaining()
    response_iter.time_remaining.assert_called_once_with()

    got_iterator.trailing_metadata()
    response_iter.trailing_metadata.assert_called_once_with()


def test_wrap_stream_errors_invocation():
    grpc_error = RpcErrorImpl(grpc.StatusCode.INTERNAL)
    callable_ = mock.Mock(spec=['__call__'], side_effect=grpc_error)

    wrapped_callable = grpc_helpers._wrap_stream_errors(callable_)

    with pytest.raises(exceptions.InternalServerError) as exc_info:
        wrapped_callable(1, 2, three='four')

    callable_.assert_called_once_with(1, 2, three='four')
    assert exc_info.value.response == grpc_error


class RpcResponseIteratorImpl(object):
    def __init__(self, exception):
        self._exception = exception

    def next(self):
        raise self._exception

    __next__ = next


def test_wrap_stream_errors_iterator():
    grpc_error = RpcErrorImpl(grpc.StatusCode.INTERNAL)
    response_iter = RpcResponseIteratorImpl(grpc_error)
    callable_ = mock.Mock(spec=['__call__'], return_value=response_iter)

    wrapped_callable = grpc_helpers._wrap_stream_errors(callable_)

    got_iterator = wrapped_callable(1, 2, three='four')

    with pytest.raises(exceptions.InternalServerError) as exc_info:
        next(got_iterator)

    callable_.assert_called_once_with(1, 2, three='four')
    assert exc_info.value.response == grpc_error


@mock.patch('bookkeeper.common.grpc_helpers._wrap_unary_errors')
def test_wrap_errors_non_streaming(wrap_unary_errors):
    callable_ = mock.create_autospec(grpc.UnaryUnaryMultiCallable)

    result = grpc_helpers.wrap_errors(callable_)

    assert result == wrap_unary_errors.return_value
    wrap_unary_errors.assert_called_once_with(callable_)


@mock.patch('bookkeeper.common.grpc_helpers._wrap_stream_errors')
def test_wrap_errors_streaming(wrap_stream_errors):
    callable_ = mock.create_autospec(grpc.UnaryStreamMultiCallable)

    result = grpc_helpers.wrap_errors(callable_)

    assert result == wrap_stream_errors.return_value
    wrap_stream_errors.assert_called_once_with(callable_)
