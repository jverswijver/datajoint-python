from nose.tools import assert_equal
from datajoint import hash


def test_hash():
    assert_equal(hash.uuid_from_buffer(b"abc").hex, "af5da9f45af7a300e3aded972f8ff687")
    assert_equal(hash.uuid_from_buffer(b"").hex, "d41d8cd98f00b204e9800998ecf8427e")
