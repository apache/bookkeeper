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

import pytest

from bookkeeper.common import protobuf_helpers
from bookkeeper.proto import common_pb2
from bookkeeper.proto import kv_pb2

from google.protobuf import any_pb2
from google.protobuf import message
from google.protobuf import source_context_pb2
from google.protobuf import struct_pb2
from google.protobuf import timestamp_pb2
from google.protobuf import type_pb2


def test_from_any_pb_success():
    in_message = common_pb2.Endpoint(port=5181)
    in_message_any = any_pb2.Any()
    in_message_any.Pack(in_message)
    out_message =\
        protobuf_helpers.from_any_pb(common_pb2.Endpoint, in_message_any)

    assert in_message == out_message


def test_from_any_pb_failure():
    in_message = any_pb2.Any()
    in_message.Pack(common_pb2.Endpoint(port=5181))

    with pytest.raises(TypeError):
        protobuf_helpers.from_any_pb(kv_pb2.KeyValue, in_message)


def test_check_protobuf_helpers_ok():
    assert protobuf_helpers.check_oneof() is None
    assert protobuf_helpers.check_oneof(foo='bar') is None
    assert protobuf_helpers.check_oneof(foo='bar', baz=None) is None
    assert protobuf_helpers.check_oneof(foo=None, baz='bacon') is None
    assert (protobuf_helpers.check_oneof(foo='bar', spam=None, eggs=None)
            is None)


def test_check_protobuf_helpers_failures():
    with pytest.raises(ValueError):
        protobuf_helpers.check_oneof(foo='bar', spam='eggs')
    with pytest.raises(ValueError):
        protobuf_helpers.check_oneof(foo='bar', baz='bacon', spam='eggs')
    with pytest.raises(ValueError):
        protobuf_helpers.check_oneof(foo='bar', spam=0, eggs=None)


def test_get_messages():
    kv = protobuf_helpers.get_messages(kv_pb2)

    # Ensure that Date was exported properly.
    assert kv['KeyValue'] is kv_pb2.KeyValue

    # Ensure that no non-Message objects were exported.
    for value in kv.values():
        assert issubclass(value, message.Message)


def test_get_dict_absent():
    with pytest.raises(KeyError):
        assert protobuf_helpers.get({}, 'foo')


def test_get_dict_present():
    assert protobuf_helpers.get({'foo': 'bar'}, 'foo') == 'bar'


def test_get_dict_default():
    assert protobuf_helpers.get({}, 'foo', default='bar') == 'bar'


def test_get_dict_nested():
    assert protobuf_helpers.get({'foo': {'bar': 'baz'}}, 'foo.bar') == 'baz'


def test_get_dict_nested_default():
    assert protobuf_helpers.get({}, 'foo.baz', default='bacon') == 'bacon'
    assert (
        protobuf_helpers.get({'foo': {}}, 'foo.baz', default='bacon') ==
        'bacon')


def test_get_msg_sentinel():
    msg = timestamp_pb2.Timestamp()
    with pytest.raises(KeyError):
        assert protobuf_helpers.get(msg, 'foo')


def test_get_msg_present():
    msg = timestamp_pb2.Timestamp(seconds=42)
    assert protobuf_helpers.get(msg, 'seconds') == 42


def test_get_msg_default():
    msg = timestamp_pb2.Timestamp()
    assert protobuf_helpers.get(msg, 'foo', default='bar') == 'bar'


def test_invalid_object():
    with pytest.raises(TypeError):
        protobuf_helpers.get(object(), 'foo', 'bar')


def test_set_dict():
    mapping = {}
    protobuf_helpers.set(mapping, 'foo', 'bar')
    assert mapping == {'foo': 'bar'}


def test_set_msg():
    msg = timestamp_pb2.Timestamp()
    protobuf_helpers.set(msg, 'seconds', 42)
    assert msg.seconds == 42


def test_set_dict_nested():
    mapping = {}
    protobuf_helpers.set(mapping, 'foo.bar', 'baz')
    assert mapping == {'foo': {'bar': 'baz'}}


def test_set_invalid_object():
    with pytest.raises(TypeError):
        protobuf_helpers.set(object(), 'foo', 'bar')


def test_setdefault_dict_unset():
    mapping = {}
    protobuf_helpers.setdefault(mapping, 'foo', 'bar')
    assert mapping == {'foo': 'bar'}


def test_setdefault_dict_falsy():
    mapping = {'foo': None}
    protobuf_helpers.setdefault(mapping, 'foo', 'bar')
    assert mapping == {'foo': 'bar'}


def test_setdefault_dict_truthy():
    mapping = {'foo': 'bar'}
    protobuf_helpers.setdefault(mapping, 'foo', 'baz')
    assert mapping == {'foo': 'bar'}


def test_field_mask_singular_field_diffs():
    original = type_pb2.Type(name='name')
    modified = type_pb2.Type()
    assert (protobuf_helpers.field_mask(original, modified).paths ==
            ['name'])

    original = type_pb2.Type(name='name')
    modified = type_pb2.Type()
    assert (protobuf_helpers.field_mask(original, modified).paths ==
            ['name'])

    original = None
    modified = type_pb2.Type(name='name')
    assert (protobuf_helpers.field_mask(original, modified).paths ==
            ['name'])

    original = type_pb2.Type(name='name')
    modified = None
    assert (protobuf_helpers.field_mask(original, modified).paths ==
            ['name'])


def test_field_mask_message_diffs():
    original = type_pb2.Type()
    modified = type_pb2.Type(source_context=source_context_pb2.SourceContext(
                            file_name='name'))
    assert (protobuf_helpers.field_mask(original, modified).paths ==
            ['source_context.file_name'])

    original = type_pb2.Type(source_context=source_context_pb2.SourceContext(
                             file_name='name'))
    modified = type_pb2.Type()
    assert (protobuf_helpers.field_mask(original, modified).paths ==
            ['source_context'])

    original = type_pb2.Type(source_context=source_context_pb2.SourceContext(
                             file_name='name'))
    modified = type_pb2.Type(source_context=source_context_pb2.SourceContext(
                             file_name='other_name'))
    assert (protobuf_helpers.field_mask(original, modified).paths ==
            ['source_context.file_name'])

    original = None
    modified = type_pb2.Type(source_context=source_context_pb2.SourceContext(
                             file_name='name'))
    assert (protobuf_helpers.field_mask(original, modified).paths ==
            ['source_context.file_name'])

    original = type_pb2.Type(source_context=source_context_pb2.SourceContext(
                             file_name='name'))
    modified = None
    assert (protobuf_helpers.field_mask(original, modified).paths ==
            ['source_context'])


def test_field_mask_repeated_diffs():
    original = struct_pb2.ListValue()
    modified = struct_pb2.ListValue(values=[struct_pb2.Value(number_value=1.0),
                                    struct_pb2.Value(number_value=2.0)])
    assert protobuf_helpers.field_mask(original, modified).paths == ['values']

    original = struct_pb2.ListValue(values=[struct_pb2.Value(number_value=1.0),
                                    struct_pb2.Value(number_value=2.0)])
    modified = struct_pb2.ListValue()
    assert protobuf_helpers.field_mask(original, modified).paths == ['values']

    original = None
    modified = struct_pb2.ListValue(values=[struct_pb2.Value(number_value=1.0),
                                    struct_pb2.Value(number_value=2.0)])
    assert protobuf_helpers.field_mask(original, modified).paths == ['values']

    original = struct_pb2.ListValue(values=[struct_pb2.Value(number_value=1.0),
                                    struct_pb2.Value(number_value=2.0)])
    modified = None
    assert protobuf_helpers.field_mask(original, modified).paths == ['values']

    original = struct_pb2.ListValue(values=[struct_pb2.Value(number_value=1.0),
                                    struct_pb2.Value(number_value=2.0)])
    modified = struct_pb2.ListValue(values=[struct_pb2.Value(number_value=2.0),
                                    struct_pb2.Value(number_value=1.0)])
    assert protobuf_helpers.field_mask(original, modified).paths == ['values']


def test_field_mask_map_diffs():
    original = struct_pb2.Struct()
    modified = struct_pb2.Struct(
            fields={'foo': struct_pb2.Value(number_value=1.0)})
    assert protobuf_helpers.field_mask(original, modified).paths == ['fields']

    original = struct_pb2.Struct(
            fields={'foo': struct_pb2.Value(number_value=1.0)})
    modified = struct_pb2.Struct()
    assert protobuf_helpers.field_mask(original, modified).paths == ['fields']

    original = None
    modified = struct_pb2.Struct(
            fields={'foo': struct_pb2.Value(number_value=1.0)})
    assert protobuf_helpers.field_mask(original, modified).paths == ['fields']

    original = struct_pb2.Struct(
            fields={'foo': struct_pb2.Value(number_value=1.0)})
    modified = None
    assert protobuf_helpers.field_mask(original, modified).paths == ['fields']

    original = struct_pb2.Struct(
            fields={'foo': struct_pb2.Value(number_value=1.0)})
    modified = struct_pb2.Struct(
            fields={'foo': struct_pb2.Value(number_value=2.0)})
    assert protobuf_helpers.field_mask(original, modified).paths == ['fields']

    original = struct_pb2.Struct(
            fields={'foo': struct_pb2.Value(number_value=1.0)})
    modified = struct_pb2.Struct(
            fields={'bar': struct_pb2.Value(number_value=1.0)})
    assert protobuf_helpers.field_mask(original, modified).paths == ['fields']
