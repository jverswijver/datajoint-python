import hashlib
import uuid
import io
import multiprocessing as mp
from pathlib import Path


class BuffIter:
    """
    BufferedReader's next() returns on newlines not chunks, very annoying
    I need it to iterate over user-defined chunks for multiprocessing
    """

    def __init__(self, chunk_size: int, buffer):
        self.chunk_size = chunk_size
        self.buffer = buffer

    def __iter__(self):
        return self

    def __next__(self):
        binary = self.buffer.read(self.chunk_size)
        if binary:
            return binary
        else:
            raise StopIteration


# Used for multiprocessing
def compute_md5_block(data):
    return hashlib.md5(data).digest()


def key_hash(mapping):
    """
    32-byte hash of the mapping's key values sorted by the key name.
    This is often used to convert a long primary key value into a shorter hash.
    For example, the JobTable in datajoint.jobs uses this function to hash the primary key of autopopulated tables.
    """
    hashed = hashlib.md5()
    for k, v in sorted(mapping.items()):
        hashed.update(str(v).encode())
    return hashed.hexdigest()


def uuid_from_stream(stream):
    """
    :return: 16-byte digest of stream data
    :stream: stream object or open file handle
    :init_string: string to initialize the checksum
    """
    pool = mp.Pool(mp.cpu_count())
    md5list = pool.map(compute_md5_block, BuffIter(524288000, stream))
    pool.close()
    pool.join()
    md5 = hashlib.md5(b"".join(md5list)).digest()
    return uuid.UUID(bytes=md5)


def uuid_from_buffer(buffer):
    return uuid_from_stream(io.BytesIO(buffer))


def uuid_from_file(filepath):
    return uuid_from_stream(Path(filepath).open("rb"))
