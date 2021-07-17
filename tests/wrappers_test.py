import unittest

from arrowgen import wrappers
from tests.test_utils import get_descriptor


class WrappersTestCase(unittest.TestCase):
    def test_timestamp(self):
        descriptor = get_descriptor("WithTimestamp")
        wrapper = wrappers.MessageWrapper(descriptor)
        self.assertTrue(list(wrapper.reader_fields())[0].is_timestamp())
