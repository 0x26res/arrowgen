import unittest

import pyarrow


class LearningTest(unittest.TestCase):
    def test_binary_behavior(self):
        bytes_array = pyarrow.array([b"", b"foo"], type=pyarrow.binary())
        print(bytes_array.type)

    def test_offset_behavior(self):
        array = pyarrow.ListArray.from_arrays([0, 1, 6], [0, 1, 2, 3, 4, 5, 6])
        self.assertEqual(len(array), 2)
        self.assertEqual(len(array[0]), 1)
        self.assertEqual(len(array[1]), 5)

    def test_struct_array_from_array_behavior(self):
        array = pyarrow.StructArray.from_arrays(
            [[None, 1], [None, "foo"]],
            fields=[
                pyarrow.field("col1", pyarrow.int64()),
                pyarrow.field("col2", pyarrow.string()),
            ],
        )
        self.assertEqual(array.null_count, 0)

    def test_struct_array_from_tuple_behavior(self):
        array = pyarrow.array(
            [None, (1, "foo")],
            type=pyarrow.struct(
                [
                    pyarrow.field("col1", pyarrow.int64()),
                    pyarrow.field("col2", pyarrow.string()),
                ]
            ),
        )
        self.assertEqual(array.null_count, 1)

    def test_struct_array_from_buffer_behavior(self):
        struct_type = pyarrow.struct(
            [
                pyarrow.field("col1", pyarrow.int64()),
                pyarrow.field("col2", pyarrow.string()),
            ]
        )
        col1 = pyarrow.array([None, 1])
        col2 = pyarrow.array([None, "foo"])
        validity_mask = pyarrow.array([False, True])
        validity_bitmask = validity_mask.buffers()[1]
        struct_array = pyarrow.StructArray.from_buffers(
            struct_type, len(col1), [validity_bitmask], children=[col1, col2]
        )
        self.assertEqual(struct_array.null_count, 1)
        self.assertEqual(struct_array.is_null().tolist(), [True, False])

    def test_list_array_pyarrow_array(self):
        list_of_struct = pyarrow.list_(
            pyarrow.struct([pyarrow.field("foo", pyarrow.string())])
        )
        array = pyarrow.array(
            [[("hello",), ("World",)], [], None, [None, ("foo",), ("bar",)]],
            type=list_of_struct,
        )
        print(array)
        print(array.to_pandas().to_markdown())

    def test_list_array_from_arrays(self):
        struct_type = pyarrow.struct([pyarrow.field("foo", pyarrow.string())])
        foo = pyarrow.array(["hello", "World", None, "foo", "bar"])
        validity_mask = pyarrow.array([True, True, False, True, True])
        validity_bitmask = validity_mask.buffers()[1]
        struct_array = pyarrow.StructArray.from_buffers(
            struct_type, len(foo), [validity_bitmask], children=[foo]
        )
        list_array = pyarrow.ListArray.from_arrays(
            offsets=[0, 2, 2, 2, 5], values=struct_array
        )
        print(list_array)
        print(list_array.to_pandas().to_markdown())
        print(len(list_array.buffers()))

    def test_list_array_from_buffers(self):
        struct_type = pyarrow.struct([pyarrow.field("foo", pyarrow.string())])
        foo_values = pyarrow.array(["hello", "World", None, "foo", "bar"])
        struct_validity_mask = pyarrow.array([True, True, False, True, True])
        struct_validity_bitmask = struct_validity_mask.buffers()[1]
        struct_array = pyarrow.StructArray.from_buffers(
            struct_type,
            len(foo_values),
            [struct_validity_bitmask],
            children=[foo_values],
        )

        list_validity_mask = pyarrow.array([True, True, False, True])
        list_validity_buffer = list_validity_mask.buffers()[1]
        list_offsets_buffer = pyarrow.array([0, 2, 2, 2, 5], pyarrow.int32()).buffers()[
            1
        ]

        list_array = pyarrow.ListArray.from_buffers(
            type=pyarrow.list_(struct_type),
            length=4,
            buffers=[list_validity_buffer, list_offsets_buffer],
            children=[struct_array],
        )
        self.assertEqual(list_array.type, pyarrow.list_(struct_type))
        print(list_array)
        print(list_array.to_pandas().to_markdown())

    def test_list_array_from_buffers_guess_type(self):
        struct_type = pyarrow.struct([pyarrow.field("foo", pyarrow.string())])
        struct_array = pyarrow.StructArray.from_arrays(
            [pyarrow.array([], type=pyarrow.string())], fields=list(struct_type)
        )

        list_validity_mask = pyarrow.array([], type=pyarrow.bool_())
        list_validity_buffer = list_validity_mask.buffers()[1]
        list_offsets_buffer = pyarrow.array([], pyarrow.int32()).buffers()[1]

        list_array = pyarrow.ListArray.from_buffers(
            type=pyarrow.list_(struct_type),
            length=0,
            buffers=[list_validity_buffer, list_offsets_buffer],
            children=[struct_array],
        )
        self.assertEqual(0, len(list_array))
