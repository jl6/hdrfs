#!/usr/bin/python3
#
# HDRFS: High Data Retention Filesystem.
# Copyright (C) 2017 J Taylor
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import logging, os, glob, sys, sqlite3, struct, time, stat, hashlib, errno, shutil, zlib, pickle, uuid, datetime, copy
from optparse import OptionParser
from functools import reduce
from fusepy import LoggingMixIn, FuseOSError, Operations, FUSE, fuse_get_context

# Set up logging.

hdrfslog = logging.getLogger('hdrfs')
hdrfslog.setLevel(logging.INFO)
fh = logging.StreamHandler()
formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
fh.setFormatter(formatter)
hdrfslog.addHandler(fh)

#fuselog = logging.getLogger('fuse.log-mixin')
#fuselog.addHandler(fh)
#fuselog.setLevel(logging.DEBUG)

MAX_TIMEPOINT = 9223372036854775807     
HDRFS_VERSION = "0.1.0"

class LogParserException(Exception):
    """Represents a generic error that may occur when parsing a log.

    volpos and logpos define the START of the block in which the error
    occurred. This is critical in the operation of the nuclear-fsck option as
    it defines the point from which the Log will be truncated.

    """
    def __init__(self, message, volpos=None, logpos=None):
        self.message = message
        self.volpos = volpos
        self.logpos = logpos
    
    def __repr__(self):
        return self.message + " (volume {}, offset {})".format(self.volpos, self.logpos)

class NonContiguityError(Exception):
    """Represents an error that may be encountered when rebuilding the Index from the Log.

    """ 
    def __init__(self, missing_vol_number):
        self.missing_vol_number = missing_vol_number


class BlockHeader(object):
    """Represents the header of a Log"""
    magic_id = b"\xD3HDRFS\x0D\x0A\x1A\x0A\x00HDRFS\x00"
    version = b"\x00"
    crc_algo = b"\x00"
    hash_algo = b"\x00"

    def __init__(self, fs_id, prev_vol_hash, vol_seq_no):
        self.fs_id = fs_id
        self.prev_vol_hash = prev_vol_hash
        self.vol_seq_no = vol_seq_no

    def __bytes__(self):
        header  = self.magic_id
        header += self.version
        header += self.fs_id
        header += self.crc_algo
        header += self.hash_algo
        header += struct.pack("<Q", self.vol_seq_no)
        header += self.prev_vol_hash

        crc = struct.pack("<L", zlib.crc32(header))
        return header + crc

    def __repr__(self):
        return ("BlockHeader(length={}, magic_id={}, version={}, fs_id={}, "
                "crc_algo={}, hash_algo={}, vol_seq_no={}, prev_vol_hash={})"
               ).format(len(bytes(self)), self.magic_id, self.version,
                        self.fs_id, self.crc_algo, self.hash_algo,
                        self.vol_seq_no, self.prev_vol_hash)

class BlockNull(object):
    """Represent a run of zeroes. A zero-length run represents the absence of a block."""
    block_id = b'\x00'
    def __init__(self, length=0):
        self.length = length

    def __repr__(self):
        return "BlockNull(length={})".format(self.length)

        
class BlockInode(object):
    """Represent an inode.

    All supplied arguments must be ints, except extent_list which may be None
    or a list, and linktarget, which must be a string.

    """
    block_id = b'\x01'
    def __init__(self, inode_number, log_timestamp, st_mode, st_uid, st_gid, st_atime, st_mtime, st_ctime,
            st_btime, extent_list=[], linktarget="", st_size=None, st_nlink=1, refcount=0):
        self.inode_number = inode_number
        self.log_timestamp = log_timestamp
        self.st_mode = st_mode
        self.st_uid = st_uid
        self.st_gid = st_gid
        self.st_atime = st_atime
        self.st_mtime = st_mtime
        self.st_ctime = st_ctime
        self.st_btime = st_btime
        self.extent_list = extent_list
        self.linktarget = linktarget
        self.st_nlink = st_nlink        # Note this is not serialized - it's a derived item.
        self.refcount = refcount        # Note this is not serialized - it's only used when an inode is open
        self.dirty = False              # Set to true if a change must be persisted upon fsync
        if st_size is not None:
            self.st_size = st_size
        else:
            if stat.S_ISREG(self.st_mode): 
                if len(extent_list) > 0:
                    self.st_size = extent_list[-1].logical_end_offset
                else:
                    self.st_size = 0
            elif stat.S_ISLNK(self.st_mode):
                self.st_size = len(self.linktarget.encode())
            else:
                self.st_size = 75

    def __bytes__(self):
        linktarget_binary = self.linktarget.encode()
        s = self.block_id
        s += struct.pack("<Q", self.inode_number)       # little-endian unsigned long long (8 bytes)
        s += struct.pack("<Q", self.log_timestamp)      # little-endian unsigned long long (8 bytes)
        s += struct.pack("<H", self.st_mode)            # little-endian unsigned short     (2 bytes)
        s += struct.pack("<H", self.st_uid)             # little-endian unsigned short     (2 bytes)
        s += struct.pack("<H", self.st_gid)             # little-endian unsigned short     (2 bytes)
        s += struct.pack("<Q", self.st_atime)           # little-endian unsigned long long (8 bytes)
        s += struct.pack("<Q", self.st_mtime)           # little-endian unsigned long long (8 bytes)
        s += struct.pack("<Q", self.st_ctime)           # little-endian unsigned long long (8 bytes)
        s += struct.pack("<Q", self.st_btime)           # little-endian unsigned long long (8 bytes)
        s += struct.pack("<Q", self.st_size)            # little-endian unsigned long long (8 bytes)
        if stat.S_ISREG(self.st_mode): 
            t = b''
            for m in self.extent_list:
                t += struct.pack("<Q", m.volume)
                t += struct.pack("<Q", m.physical_start_offset)
                t += struct.pack("<Q", m.block_size)
                t += m.block_multiplicity
                t += struct.pack("<Q", m.block_count)
                t += struct.pack("<Q", m.pre_truncate)
                t += struct.pack("<Q", m.post_truncate)
                t += struct.pack("<Q", m.logical_start_offset)
            s += struct.pack("<Q", len(t))
            s += t
        elif stat.S_ISLNK(self.st_mode):
            s += struct.pack("<Q", len(linktarget_binary))
            s += linktarget_binary
        else:
            s += struct.pack("<Q", 0)
        crc = struct.pack("<L", zlib.crc32(s))
        return s + crc

    def __repr__(self):
        return ("BlockInode(length={}, inode_number={}, log_timestamp={}, st_mode={}, st_uid={}, st_gid={}, "
                "st_atime={}, st_mtime={}, st_ctime={}, st_btime={}, st_size={}, extent_list={}, linktarget={}, "
                "st_nlink={}, refcount={}").format(len(bytes(self)),
                                                   self.inode_number,
                                                   self.log_timestamp,
                                                   self.st_mode,
                                                   self.st_uid,
                                                   self.st_gid,
                                                   self.st_atime,
                                                   self.st_mtime,
                                                   self.st_ctime,
                                                   self.st_btime,
                                                   self.st_size,
                                                   self.extent_list,
                                                   self.linktarget,
                                                   self.st_nlink,
                                                   self.refcount)

class BlockLink(object):
    block_id = b'\x02'

    def __init__(self, log_timestamp, child_inode, parent_inode, objname):
        self.log_timestamp = log_timestamp
        self.child_inode = child_inode
        self.parent_inode = parent_inode
        self.objname = objname

    def __bytes__(self):
        objname_binary = self.objname.encode()
        s = self.block_id
        s += struct.pack("<Q", self.log_timestamp)
        s += struct.pack("<Q", self.child_inode)
        s += struct.pack("<Q", self.parent_inode)
        s += struct.pack("<H", len(objname_binary))     # Kernel enforces a max path length of 4095
        s += objname_binary
        crc = struct.pack("<L", zlib.crc32(s))
        return s + crc

    def __repr__(self):
        return "BlockLink(length={}, log_timestamp={}, child_inode={}, parent_inode={}, objname={})".format(
                len(bytes(self)), self.log_timestamp, self.child_inode, self.parent_inode, self.objname)


class BlockUnlink(object):
    block_id = b'\x03'

    def __init__(self, log_timestamp, child_inode, parent_inode, name):
        self.log_timestamp = log_timestamp
        self.child_inode = child_inode
        self.parent_inode = parent_inode
        self.objname = name

    def __bytes__(self):
        objname_binary = self.objname.encode()
        s = self.block_id
        s += struct.pack("<Q", self.log_timestamp)
        s += struct.pack("<Q", self.child_inode)
        s += struct.pack("<Q", self.parent_inode)
        s += struct.pack("<H", len(objname_binary))
        s += objname_binary
        crc = struct.pack("<L", zlib.crc32(s))
        return s + crc

    def __repr__(self):
        return "BlockUnlink(length={}, log_timestamp={}, child_inode={}, parent_inode={}, objname={})".format(
                len(bytes(self)), self.log_timestamp, self.child_inode, self.parent_inode, self.objname)

class BlockXattr(object):
    block_id = b'\x04'

    def __init__(self, log_timestamp, inode_number, name, value):
        """For some reason fusepy gives us a str for a name and bytes for value."""
        assert type(log_timestamp) == int
        assert type(inode_number) == int
        assert type(name) == str
        assert type(value) == bytes
        self.log_timestamp = log_timestamp
        self.inode_number = inode_number
        self.name = name
        self.value = value

    def __bytes__(self):
        name_binary = self.name.encode("utf-8")
        s = self.block_id
        s += struct.pack("<Q", self.log_timestamp)
        s += struct.pack("<Q", self.inode_number)
        s += struct.pack("<B", len(name_binary))            # Max length 2^8  =   255 imposed by the kernel
        s += struct.pack("<H", len(self.value))             # Max length 2^16 = 65536 imposed by the kernel
        s += name_binary
        s += self.value
        crc = struct.pack("<L", zlib.crc32(s))
        return s + crc

    def __repr__(self):
        return "BlockXattr(length={}, log_timestamp={}, inode_number={}, name={}, value={})".format(
                len(bytes(self)), self.log_timestamp, self.inode_number, self.name, self.value)

class BlockRemovedXattr(object):
    block_id = b'\x05'

    def __init__(self, log_timestamp, inode_number, name):
        assert type(log_timestamp) == int
        assert type(inode_number) == int
        assert type(name) == str
        self.log_timestamp = log_timestamp
        self.inode_number = inode_number
        self.name = name

    def __bytes__(self):
        name_binary = self.name.encode("utf-8")
        s = self.block_id
        s += struct.pack("<Q", self.log_timestamp)
        s += struct.pack("<Q", self.inode_number)
        s += struct.pack("<B", len(name_binary))
        s += name_binary
        crc = struct.pack("<L", zlib.crc32(s))
        return s + crc

    def __repr__(self):
        return "BlockRemovedXattr(length={}, inode_number={}, log_timestamp={}, name={})".format(
                len(bytes(self)), self.log_timestamp, self.inode_number, self.name)

class BlockData(object):
    block_id = b'\x06'

    def __init__(self, log_timestamp, payload):
        """A log_timestamp should be the same for all blocks created by a single write() call"""
        self.log_timestamp = log_timestamp
        self.payload = payload

    def __bytes__(self):
        s = self.block_id
        s += struct.pack("<Q", self.log_timestamp)
        s += struct.pack("<Q", len(self.payload))       # In typical usage this length will be <=131072. Future kernels may allow higher.
        s += self.payload                               # and nothing prevents an HDRFS Log file containing larger extents.
        crc = struct.pack("<L", zlib.crc32(s))
        return s + crc

    def __repr__(self):
        return "BlockData(length={}, log_timestamp={}, payload_length={})".format(
                len(bytes(self)), self.log_timestamp, len(self.payload))

class BlockRename(object):
    block_id = b'\x07'

    def __init__(self, log_timestamp, oldpath, newpath):
        self.log_timestamp = log_timestamp
        self.oldpath = oldpath
        self.newpath = newpath

    def __bytes__(self):
        oldpath_binary = self.oldpath.encode('UTF-8')
        newpath_binary = self.newpath.encode('UTF-8')
        s = self.block_id
        s += struct.pack("<Q", self.log_timestamp)
        s += struct.pack("<H", len(oldpath_binary))     # Kernel enforces a max path length of 4095
        s += struct.pack("<H", len(newpath_binary))     # Kernel enforces a max path length of 4095
        s += oldpath_binary
        s += newpath_binary
        crc = struct.pack("<L", zlib.crc32(s))
        return s + crc

    def __repr__(self):
        return "BlockRename(length={}, log_timestamp={}, oldpath={}, newpath={})".format(
                len(bytes(self)), self.log_timestamp, self.oldpath, self.newpath)

class BlockLinkTable(object):
    """TODO: Wise to store all links in memory? Potential for huge memory use on large filesystems?

    This block holds no timestamp as it is always written as the first block in a new volume and
    not at the request of any POSIX operation."""
    block_id = b'\x08'

    def __init__(self, links):
        self.links = links

    def __bytes__(self):
        s = self.block_id
        s += struct.pack("<Q", len(self.links))
        for a in self.links:
            child, parent, name = a
            name_binary = name.encode('UTF-8')
            s += struct.pack("<Q", child)
            s += struct.pack("<Q", parent)
            s += struct.pack("<H", len(name_binary))
            s += name_binary
        crc = struct.pack("<L", zlib.crc32(s))
        return s + crc

    def __repr__(self):
        return "BlockLinkTable(length={}, entries)".format(
                len(bytes(self)), len(self.links))
            

class Extent(object):
    """
    pre_truncate and post_truncate may both be any integer up to and including block_size*block_count
    This lets us easily trim extents without doing too much arithmetic

    Note the ugliness of calculate_properties(). Users of this class have to call this function after every modification of 
    an Extent object. In return, we get a ~10% speed boost over using dynamically calculated @properties. We mitigate the risk
    of an un-re-calculated attribute with extensive tests.
    """
    __slots__ = ('volume', 'physical_start_offset', 'block_size', 'block_multiplicity', 'block_count', 'pre_truncate', 'post_truncate',
            'logical_start_offset', 'length', 'logical_end_offset')

    def __init__(self, volume, physical_start_offset, block_size, block_multiplicity, block_count, pre_truncate, post_truncate,
            logical_start_offset):
        assert type(volume) == int
        assert volume >= 0
        assert type(physical_start_offset) == int
        assert physical_start_offset >= 0
        assert type(block_size) == int
        assert block_size >= 0
        assert block_multiplicity in (b'C', b'R')     #   C = Count. R = Repeat.
        assert type(block_count) == int
        assert block_count >= 0
        assert type(pre_truncate) == int
        assert pre_truncate >= 0
        assert type(post_truncate) == int
        assert post_truncate >= 0
        assert type(logical_start_offset) == int
        assert logical_start_offset >= 0
        self.volume = volume
        self.physical_start_offset = physical_start_offset
        self.block_size = block_size
        self.block_multiplicity = block_multiplicity
        self.block_count = block_count              #   Used to reference a sequence of blocks
        self.pre_truncate = pre_truncate
        self.post_truncate = post_truncate
        self.logical_start_offset = logical_start_offset
        self.calculate_properties()


    def calculate_properties(self):
        self.length = (self.block_size * self.block_count) - self.pre_truncate - self.post_truncate
        self.logical_end_offset = self.logical_start_offset + self.length

    def __eq__(self, other):
        """http://stackoverflow.com/questions/9843569/if-duck-typing-in-python-should-you-test-isinstance"""
        if isinstance(other, self.__class__):
            if (self.volume == other.volume
             and self.physical_start_offset == other.physical_start_offset
             and self.block_size == other.block_size
             and self.block_multiplicity == other.block_multiplicity
             and self.block_count == other.block_count
             and self.pre_truncate == other.pre_truncate
             and self.post_truncate == other.post_truncate
             and self.logical_start_offset == other.logical_start_offset):
                return True
        else:
            return NotImplemented

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return "Extent({})".format(", ".join([str(getattr(self, s)) for s in self.__slots__]))

    @staticmethod               # Rationale for a staticmethod rather than a top-level function - grouping utility functions
    def truncate(extent_list, length):
        newmapping = []
        for e in extent_list:
            if e.logical_end_offset <= length:
                newmapping.append(e)
            elif e.logical_start_offset < length < e.logical_end_offset:
                e.post_truncate += e.logical_end_offset - length
                e.calculate_properties()
                newmapping.append(e)
            elif length <= e.logical_start_offset:
                pass    #    In this case, drop the extent from the new mapping
        return newmapping


    @staticmethod
    def is_extent_list_ordered(extent_list):
        """Returns True if the extents in the list are in logical order. Note the use
        of <= less than or equal - a list may not have two extents claiming to start at the same
        logical position."""
        logical_position = -1
        for e in extent_list:
            if e.logical_start_offset <= logical_position:
                return False
            logical_position = e.logical_start_offset
        return True


    # The problem is now how to implement splicing of an Extent into a list of other extents.

    # A volume with many blocks:
    #
    # |---------|------------------|--|--|--|--|--|------------|
    #      (---------------------------------)
    #      An extent runs from here... to here
    #
    # The list must be ordered by logical_start_offset
    #
    # A new extent will have a logical start offset. Need to iterate over the extent list looking for where it should start.

    @staticmethod
    def splice(cur, new):
        "Given an ordered list of Extents, and a new Extent, splice the latter into the former"
        assert Extent.is_extent_list_ordered(cur)
        assert type(new) == Extent

        # Simplest case: a new file with no current extents
        if len(cur) == 0:
            return [new]
                
        # Block repeat detection
        last_extent = cur[-1]
        if (last_extent.logical_end_offset == new.logical_start_offset and
            last_extent.physical_start_offset == new.physical_start_offset and
            last_extent.volume == new.volume and
            last_extent.block_size == new.block_size):
            last_extent.block_multiplicity = b'R'
            last_extent.block_count += 1
            last_extent.calculate_properties()
            assert Extent.is_extent_list_ordered(cur)
            return cur

        # First check for the special case of new extending exactly from the last element of cur
        # The block size must match, and they must be on the same volume.
        # ISSUE: This case is not reached if writing a new sequence of blocks into the middle of the file.
        #        When might that be the case? Probably rare? Need some real-world usage to determine.
        if (last_extent.logical_end_offset == new.logical_start_offset and
            last_extent.block_size == new.block_size and
            last_extent.volume == new.volume and
            last_extent.physical_start_offset+(17+last_extent.block_size+4)*last_extent.block_count == new.physical_start_offset):            # This condition checks that the new block really does follow on from the last
            last_extent.block_count += new.block_count      
            last_extent.calculate_properties()
            assert Extent.is_extent_list_ordered(cur)
            return cur

        # Otherwise, continue with a general extent splicing algorithm
        for i, e in enumerate(cur):
           logical_start = e.logical_start_offset
           logical_end = logical_start + e.length
           # New may either be strictly before the current element, overlap it, or be strictly after it.
           # If it's strictly before, insert it into the list before the current element - and we're done.
           # If it's strictly after, continue iterating
           # Otherwise, splice
           # Case 1: new is strictly before. We already know if must be strictly after the previous e to have
           # gotten here.
           if new.logical_end_offset < e.logical_start_offset:
               cur.insert(i, new)
               assert Extent.is_extent_list_ordered(cur)
               return cur
           # Case 2: strictly after
           elif new.logical_start_offset > e.logical_end_offset:
               continue
           # Case 3: It must overlap
           else:
               # In general, when new overlaps e, there will be a portion of an existing e before it (possibly zero!)
               # then new, then a portion of an existing e after it. Need to start a new loop through cur to find the
               # end of new.

               # First work out how much of the current e needs to be retained
               post_truncation_amount = e.logical_end_offset - new.logical_start_offset
               assert post_truncation_amount >= 0
               

               # Now loop through the remainder of cur, looking for the end of new, and index that location as j
               # [a,b,c,d,e]
               #      i
               #  0 1 2 3 4 
               #  len() = 5
               # 5-2 = 3
               # range(3) = [0,1,2]
        
               # In case there are no other elements, j = i.

               for j in range(i, len(cur)):
                   f = cur[j]
                   if f.logical_end_offset >= new.logical_end_offset:
                       break

               # There are another two cases now. Either j>i, in which case we
               # pre-truncate f, or j=i in which case we MAY need to create a new
               # Extent representing the trailing portion of e, which logically
               # starts where new ends
               if j>i:
                   pre_truncation_amount = new.logical_end_offset - f.logical_start_offset
                   assert pre_truncation_amount >= 0
                   f.pre_truncate += pre_truncation_amount
                   f.logical_start_offset = new.logical_end_offset
                   f.calculate_properties()
                   trailer = False
               else:
                   if e.logical_end_offset > new.logical_end_offset:
                       trailer = True
                       trailer_pretruncation_amount = e.pre_truncate + (new.logical_end_offset - e.logical_start_offset)
                       trailer_extent = Extent(e.volume, e.physical_start_offset, e.block_size, e.block_multiplicity, e.block_count,
                               trailer_pretruncation_amount, e.post_truncate, new.logical_end_offset)
                   else:
                       trailer = False

               # Now that we've done the main length logic, we can apply the post-truncation.
               e.post_truncate += post_truncation_amount
               e.calculate_properties()


               # We now know the first (e, at index i) and last (f, at index j) extents which overlap new. They may be the same!
               # We need to remove all elements within this range, and optionally the endpoints too,
               # if their lengths have dropped to zero due to truncation.

               # There are 4 cases: i=j with trailer, i=j with no trailer, j=i+1, j>i+1
               # The correct order for each of these is:
               # i=j with trailer:
               #  [..., e, new, trailer, ...]
               # i=j with no trailer:
               #  [..., e, new, ...]
               # j=i+1:
               #  [..., e, new, f, ...]
               # j>i+1:
               #  [..., e, new, f, ...] <--- Difference is that for i < x < j all x have been removed
               #
               # In each of these cases, e and f may need deleting if they are zero-length.
               #
               # Work from the right hand end of the list first to keep indices stable.

               if i==j:
                   if trailer:
                       cur.insert(i+1, trailer_extent)
                   cur.insert(i+1, new)

               elif j==i+1:
                   if f.length <= 0:
                       del cur[j]
                   cur.insert(j, new)

               elif j > i+1:
                   if f.length <= 0:
                       del cur[j]
                   del cur[i+1:j]
                   cur.insert(i+1, new)

               else:
                   hdrfslog.error("Impossible?!")
                   raise FuseOSError(errno.EIO)

               if e.length <= 0:
                   del cur[i]


               assert Extent.is_extent_list_ordered(cur)
               return cur


        # If we've got this far without returning, then the current extent must
        # never exceed new, and therefore it just gets appended.
        cur.append(new)
        assert Extent.is_extent_list_ordered(cur)
        return cur


class HDRFS(Operations):


    def __init__(self, options):
        """
        This class controls all HDRFS-related functionality. It does not handle
        startup, shutdown, user interface or FUSE operations.

        11-case startup

        """

        self.options = options

        self.readonly = False

        # Short-circuiting or sometimes considered an anti-pattern.
        # But option values are always a string so we will not be surprised by falsy other types.
        self.indexdir = options["indexdir"] or options["datadir"]
        self.logdir = options["logdir"] or options["datadir"]
        self.dedupdir = options["dedupdir"] or options["datadir"]

        self.dedup_file = os.path.join(self.dedupdir, 'dedup.hdrfs')
        if os.path.exists(self.dedup_file):
            with open(self.dedup_file, 'rb') as f:
                self.dedup = pickle.loads(f.read())
        else:
            self.dedup = {}

        db_file = os.path.join(self.indexdir, "index.hdrfs")
        db_file_already_exists = os.path.exists(db_file)
        matches = glob.glob(os.path.join(self.logdir, "L????????????????.hdrfs"))
    
        if len(matches) == 0 and not db_file_already_exists:
            # Case 11
            self.I = HDRFSIndex(db_file, create_new=True)
            self.L = HDRFSLog(self.logdir, options, create_new=True, I=self.I)
            self._initialise_root_dir()

        elif len(matches) == 0 and db_file_already_exists:
            # Case 10 - read-only with access to metadata only.
            self.readonly = True
            self.I = HDRFSIndex(db_file)
            self.L = HDRFSLog(self.logdir, options)
        else:
            # Log files exist, so start up a Log instance and use it to determine exact startup case.
            self.L = HDRFSLog(self.logdir, options)
            log_head = self.L.get_head_pointer()
            if self.L.degraded:
                if db_file_already_exists:
                    self.I = HDRFSIndex(db_file)
                    index_head = self.I.get_head_pointer()
                    if log_head > index_head:
                        # Case 5
                        self._rebuild_index_from_log(catchup=True)
                    elif log_head == index_head:
                        pass # Case 6. No action required. This is normal startup, albeit with a degraded Log.
                    elif log_head < index_head:
                        # Case 7
                        hdrfslog.warning("Mounting read-only as the index is not in sync with the log")
                        self.readonly = True
                    else:
                        sys.exit("Impossible?")
                else:
                    # Case 8
                    sys.exit("Cannot start with no index and missing volumes.")
            else:
                if db_file_already_exists:
                    self.I = HDRFSIndex(db_file)
                    index_head = self.I.get_head_pointer()
                    if log_head > index_head:
                        # Case 1
                        self._rebuild_index_from_log(catchup=True)
                    elif log_head == index_head:
                        pass # Case 2. No action required - this is normal startup.
                    elif log_head < index_head:
                        # Case 3
                        hdrfslog.warning("Mounting read-only as the index is not in sync with the log")
                        self.readonly = True
                else:
                    # Case 4
                    self.I = HDRFSIndex(db_file, create_new=True)
                    self._rebuild_index_from_log()

        self.open_handles = {} 
                              # {filehandle: (inode_number, timepoint)}

        self.open_inodes = {} # A cache shared between all open file handles

        self._writebuffer = {"fh": None, "offset": None, "data": None}  # 

        # Give the Log a reference to the Index. Essential for dumping the links table during new volume creation
        self.L.register_index(self.I)

        # TODO: Some startup sanity checks on the index? Like ensuring no rows for inodes with no links?

        if options.get("dump_log_to_text_file") is not None:
            self.L._dump_log_to_text_file(options["dump_log_to_text_file"])
            sys.exit()
    
        if options.get("startup_notification"):
            print("STARTUP COMPLETE")
            sys.stdout.flush()

    @staticmethod
    def _mode2type(st_mode):
        """Translate st_mode values into one-character codes"""
        if stat.S_ISDIR(st_mode):
            return 'd'
        if stat.S_ISCHR(st_mode):
            return 'c'
        if stat.S_ISBLK(st_mode):
            return 'b'
        if stat.S_ISREG(st_mode):
            return 'f'
        if stat.S_ISFIFO(st_mode):
            return 'p'
        if stat.S_ISLNK(st_mode):
            return 'l'
        if stat.S_ISSOCK(st_mode):
            return 's'
        else:
            raise Exception("Unknown file mode {}!".format(st_mode))

    def _initialise_root_dir(self):
        # The first thing we write to the data file is the root directory.
        # Note the pattern of writing to the log first, then to the Index.
        # The log is never inconsistent. The worst that could happen is 
        # having an incomplete extent written to it.
        timepoint = int(time.time()*1000000)
        volpos, logpos = self.L.initialise_root_dir(timepoint)
        self.I.initialise_root_dir(volpos, logpos, timepoint)

    def _allocate_new_handle(self):
        current_handles = set(self.open_handles.keys())
        possible_handles = set(range(0,len(self.open_handles)+1))
        return min(possible_handles - current_handles)
        
    def _parse_path(self, path):
        """Parses a path to determine if it encodes a timepoint. Returns either
           the parsed timepoint, or None, along with the remainder of the
           path"""

        if path.startswith('/'+self.options["history_directory"]) and len(path) > len(self.options["history_directory"])+1:         # /history doesn't get timestamp-parsed. /history/ANYTHING does
            parsed = path.split('/', 3)
            if len(parsed) < 3:
                raise Exception("Impossible?")
            else:
                timestamp = parsed[2]
                if len(parsed) == 3:
                    remainder = '/'
                elif len(parsed) > 3:
                    remainder = '/' + parsed[3]

            try:
                timepoint = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f%z')
            except ValueError:
                raise FuseOSError(errno.EINVAL)
            return int(timepoint.timestamp()*1000000), remainder    # Timepoints are always microsecond integers
        else:
            return None, path

    def _cache_inode(self, inode_number, timepoint):
        # Upon open, build a data structure which caches metadata, including the logical->physical mapping of the file data.
        # This doesn't get written back to the database until an fsync is called, or the file is closed.
        # No action if the inode is already cached
        if self.open_inodes.get((inode_number, timepoint)) is None:
            self.open_inodes[(inode_number, timepoint)] = self.I.get_inode(inode_number, timepoint) # refcount will default to zero
        else:
            pass
    

    def _closeall(self):
        "Close all open resources, to aid a clean shutdown and eliminate ResourceWarnings"
        self.L.closeall()
        self.I.closeall()

    def _rebuild_index_from_log(self, catchup=True):
        hdrfslog.info("""Rebuilding index from logs."""
                      """ This involves reading every part of every log file so rebuilding a"""
                      """ large filesystem requires patience.""")
        # The general strategy is to parse a stream of Block objects.
        if catchup:
            volpos, logpos = self.I.get_head_pointer()
        else:
            volpos = 0
            logpos = 0

        maxinode = 0        # TODO: Should we check for each block that it references no inodes higher than the maxinode seen so far?

        last_log_timestamp = 0      # Track and confirm that blocks are in time order.
        try:
            for (volcursor, block_start), block in self.L._parse_log(volcursor=volpos, logcursor=logpos):
                if type(block) not in (BlockHeader, BlockNull, BlockLinkTable):         # These blocks do not have log_timestamp fields
                    if block.log_timestamp < last_log_timestamp:
                        e = LogParserException("Out of order block: {} (prev was {})".format(block.log_timestamp, last_log_timestamp),
                             volcursor, block_start)
                        if self.options["ignore_metadata_errors"]:
                            hdrfslog.warning(repr(e))
                        else:
                            raise e 
                    last_log_timestamp = block.log_timestamp
                if type(block) in (BlockHeader, BlockNull, BlockLinkTable, BlockData):
                    pass
                elif type(block) == BlockInode:
                    if block.inode_number == 0:
                        self.I.initialise_root_dir(0, 0, block.log_timestamp)   # TODO: does it matter that we don't use a real value for volpos and logpos?
                    else:
                        self.I.flush_inodes([block], block.log_timestamp)    # Syncs only one inode
                    maxinode = max(block.inode_number, maxinode)
                elif type(block) == BlockLink:
                    self.I.write_link(block.child_inode, block.parent_inode, block.objname, timepoint=block.log_timestamp)
                    path = self.I.get_path_for_inode(block.parent_inode, block.log_timestamp)
                    inode = self.I.get_inode(block.child_inode, block.log_timestamp)
                    self.I.write_path(os.path.join(path, block.objname), block.child_inode, self._mode2type(inode.st_mode), block.log_timestamp)
                elif type(block) == BlockUnlink:
                    # Look up the path of the parent inode.
                    # It will always be unique because the parent must be a directory and directories cannot be hard linked!
                    parent_path = self.I.get_path_for_inode(block.parent_inode, block.log_timestamp)
                    self.I.unlink(os.path.join(parent_path, block.objname), block.log_timestamp)
                elif type(block) == BlockXattr:
                    self.I.setxattr(block)
                elif type(block) == BlockRemovedXattr:
                    self.I.removexattr(block)
                elif type(block) == BlockRename:
                    self.I.rename(block.oldpath, block.newpath, block.log_timestamp)
                else:
                    sys.exit("Impossible?!")

            
        except NonContiguityError as e:
            hdrfslog.error("Attempted to rebuild log but missing volume {}".format(e.missing_vol_number))
        except LogParserException as e:
            if not self.options["nuclear_fsck"]:
                hdrfslog.error("Error attempting to rebuild index: {}. Now would be a good time to restore this volume from backup."
                               " Use the --ignore-data-errors or --ignore-metadata-errors options to continue processing the Log,"
                               " though this may or may not result in a usable filesystem. Alternatively, truncate the Log back to the"
                               " last non-corrupt block using the --nuclear-fsck option, but NOTE THAT THIS IS A DESTRUCTIVE OPERATION.".format(e.message))
                os.remove(self.I.db_file)
                sys.exit()
            else:
                # Truncate the log from the offending block.
                self.L.truncate_log(e.volpos, e.logpos)
                if (e.volpos, e.logpos) == (0,0):       # If we've truncated all the way back to zero, delete the index - a whole new FS is required.
                    os.remove(self.I.db_file)
                    sys.exit("Corrupt data from block 0. Removing whole filesystem including index.")
                hdrfslog.info("Log truncated.")
                
            

        new_volpos, new_logpos = self.L.get_head_pointer()
        self.I.update_istat(new_volpos, new_logpos)
        self.I.set_maxinode(maxinode)
        self.I.commit()
        hdrfslog.info("""Index rebuild complete.""")

    def _flushwritebuffer(self):
        if self._writebuffer["fh"] is not None:
            self.write(None, self._writebuffer["data"], self._writebuffer["offset"], self._writebuffer["fh"], buffered=False)
            self._writebuffer["offset"] = None
            self._writebuffer["data"] = None
            self._writebuffer["fh"] = None

    def read(self, path, size, offset, fh):
        "Ignore path - use file handle"
        assert type(path) == str
        assert type(size) == int
        assert type(offset) == int
        assert type(fh) == int
        try:
            inode_number, timepoint = self.open_handles[fh]
        except KeyError:
            raise FuseOSError(errno.EBADF)

        # Check to see if the writebuffer has any data pending for this inode.
        if self._writebuffer["fh"] is not None:
            pending_inode_number, _ = self.open_handles[self._writebuffer["fh"]]
            if pending_inode_number == inode_number:
                self._flushwritebuffer()

        self._cache_inode(inode_number, timepoint)                              # Likely to already be cached, but may have been uncached
                                                                                # by a metadata write.

        extent_list = self.open_inodes[(inode_number, timepoint)].extent_list
        st_size = self.open_inodes[(inode_number, timepoint)].st_size
        buf = self.L.read(extent_list, size, offset, st_size)
        if len(buf) > 0 and self.options["atime"] and timepoint is None:        # Don't update atime when reading historical files
            now = int(time.time()*1000000)
            self.open_inodes[(inode_number, timepoint)].st_atime = now
            self.open_inodes[(inode_number, timepoint)].dirty = True
                                                                                            
        return buf

    def readdir(self, path, offset):
        dirents = ['.', '..']
        if path == '/':
            dirents.append(self.options["history_directory"])
        if path == '/' + self.options["history_directory"]:
            granularity = self.options["history_directory_granularity"]
            return dirents + self.I.get_historical_timepoints(granularity)

        timepoint, path = self._parse_path(path)

        return dirents + self.I.readdir(path, offset, timepoint)

    def destroy(self, path):
        "path is always /. Cleans up as part of a clean shutdown of the filesystem."
        self.fsync()
        with open(self.dedup_file, 'wb') as f:
            pickle.dump(self.dedup, f)
        self._closeall()

    def mkdir(self, path, st_mode):
        """Write to Log, Write to Index.

        st_mode is an int representing only the permission bits! 0o777, not 0100777 - and not a tuple either

        """
        if self.readonly:
            raise FuseOSError(errno.EROFS)

        parent_dir, this_dir = os.path.split(path)
        parent_dir_inode_number = self.I.get_inode_for_path(parent_dir)
        path_inode = self.I.get_inode_for_path(path)


        if path_inode is not None:
            raise FuseOSError(errno.EEXIST)
        elif parent_dir_inode_number is None:
            raise FuseOSError(errno.ENOENT)
        else:
            self._flushwritebuffer()        # Always clear the write buffer before writing metadata to the Log
            new_inode_number = self.I.get_next_inode()
            now = int(time.time()*1000000)
            st_uid, st_gid, _ = fuse_get_context()
            
            # POSIX requires that the mtime and ctime of the parent directory are updated
            parent_dir_inode = self.I.get_inode(parent_dir_inode_number, now)     # Don't need to worry about consulting the cache - dirs are never cached
            parent_dir_inode.st_mtime = now
            parent_dir_inode.st_ctime = now
            self.L.write_metadata(parent_dir_inode)

            new_inode = BlockInode(new_inode_number, now, stat.S_IFDIR | st_mode, st_uid, st_gid, now, now, now, now)
            self.L.write_metadata(new_inode)

            block_link = BlockLink(now, new_inode_number, parent_dir_inode_number, this_dir)
            self.L.write_metadata(block_link)

            volpos, logpos = self.L.get_head_pointer()
            self.I.flush_inodes([parent_dir_inode], now)
            self.I.write_inode(new_inode)
            self.I.write_link(new_inode_number, parent_dir_inode_number, this_dir)
            self.I.write_path(path, new_inode_number, 'd')
            self.I.update_istat(volpos, logpos)
            self.I.commit()
            return 0

    def create(self, path, mode):           
        if self.readonly:
            raise FuseOSError(errno.EROFS)
        return self.open(path, (mode,))    

    def mknod(self, path, mode, rdev):
        "Note no return value. TODO: Do something with rdev?"
        self.open(path, (mode,))

    def open(self, path, st_mode, create=True):
        "Mode is passed as a 1-tuple. By default will create the file, but this may not be desirable e.g. for hard links"
        timepoint, path = self._parse_path(path)
        inode_number = self.I.get_inode_for_path(path, timepoint)

        if inode_number is None:                        # No existing inode ==> create it
            if timepoint is not None:                   # But refuse to create a file if querying historically
                raise FuseOSError(errno.EROFS)
            if create == False:                         # And don't create a file if the user expects not to be creating one
                hdrfslog.error("Attempted to open a path that doesn't exist. Not creating.")
                raise FuseOSError(errno.ENOENT)
            # If none found, create a new one. Use the next inode number after the previous highest.
            new_inode_number = self.I.get_next_inode()
            parentpath, fname = os.path.split(path)
            parent_dir_inode_number = self.I.get_inode_for_path(parentpath)
            if parent_dir_inode_number is None:
                #                not exist.""".format(path, parentpath))
                raise FuseOSError(errno.ENOENT)
            else:
                self._flushwritebuffer()        # Always clear the write buffer before writing metadata to the Log
                now = int(time.time()*1000000)

                # POSIX requires that the mtime and ctime of the parent directory are updated
                parent_dir_inode = self.I.get_inode(parent_dir_inode_number, now)     # Don't need to worry about consulting the cache - dirs are never cached
                parent_dir_inode.st_mtime = now
                parent_dir_inode.st_ctime = now
                self.L.write_metadata(parent_dir_inode)

                st_uid, st_gid, _ = fuse_get_context()
                new_inode = BlockInode(new_inode_number, now, st_mode[0], st_uid, st_gid, now, now, now, now)
                self.L.write_metadata(new_inode)
                new_link = BlockLink(now, new_inode_number, parent_dir_inode_number, fname)
                self.L.write_metadata(new_link)
                volpos, logpos = self.L.get_head_pointer()
                self.I.flush_inodes([parent_dir_inode], now)
                self.I.write_inode(new_inode)
                self.I.write_link(new_inode_number, parent_dir_inode_number, fname)
                self.I.write_path(path, new_inode_number, 'f')
                self.I.update_istat(volpos, logpos)
                self.I.commit()
                inode_number = new_inode_number

        # Now we either have an exist inode_number, or a newly created one. Next step is to allocate and return a file handle.
        fh = self._allocate_new_handle()
        self.open_handles[fh] = (inode_number, timepoint)

        self._cache_inode(inode_number, timepoint)
        self.open_inodes[(inode_number, timepoint)].refcount += 1
        return fh

    def _uncache_inode(self, inode_number, timepoint, now):
        """Removes an inode from the cache after flushing it (if it's dirty). Does nothing if the inode isn't in cache."""
        icache = self.open_inodes.get((inode_number,timepoint))

        if icache is not None:
            if icache.dirty:
                inode_as_list = [icache]
                self.L.flush_inodes(inode_as_list, now)
                self.I.flush_inodes(inode_as_list, now)
                volpos, logpos = self.L.get_head_pointer()
                self.I.update_istat(volpos, logpos)
                self.I.commit()
            del self.open_inodes[(inode_number,timepoint)]

    def utimens(self, path, times=None):
        'Times is a (atime, mtime) tuple. If None use current time.'
        if times is None:
            now = int(time.time()*1000000)
            times = (now, now)
        else:
            times = (int(times[0]*1000000), int(times[1]*1000000))
        timepoint, path = self._parse_path(path)
        if self.readonly or timepoint is not None:
            raise FuseOSError(errno.EROFS)

        inode_number = self.I.get_inode_for_path(path, timepoint)

        self._flushwritebuffer()                                                # Always clear the write buffer before writing metadata to the Log

        now = int(time.time()*1000000)
        self._uncache_inode(inode_number, timepoint, now)

        inode = self.I.get_inode(inode_number, now)
        inode.st_atime = times[0]
        inode.st_mtime = times[1]


        inode_as_list = [inode]
        self.L.flush_inodes(inode_as_list, now)
        self.I.flush_inodes(inode_as_list, now)
        volpos, logpos = self.L.get_head_pointer()
        self.I.update_istat(volpos, logpos)
        self.I.commit()

    def flush(self, path, fh):
        self._flushwritebuffer()
        return 0

    def release(self, path, fh):
        "Ignores the path. Release is called once for every open."
        #print("Releasing path {} fh {}".format(path, fh))
        now = int(time.time()*1000000)
        self._flushwritebuffer()
        inode_number, timepoint = self.open_handles[fh]
        icache = self.open_inodes.get((inode_number,timepoint))
        if icache is not None:
            icache.refcount -= 1
            if icache.refcount == 0:
                self._uncache_inode(inode_number, timepoint, now)
        del self.open_handles[fh]

    def getattr(self, path, fh=None):
        """If we are supplied a file handle, it will reference the inode of an open
file, so we can get the stat data from the cache (avoiding a db query)"""
        #print("getattr for path {}.".format(path))
        if path == '/'+self.options["history_directory"]:
            now = int(time.time())
            return {'st_ino': 1, 'st_atime': now, 'st_gid': os.getgid(), 'st_ctime': now, 'st_btime': now, 'st_uid': os.getuid(), 'st_size': 0, 'st_mode': 16895, 'st_mtime': now, 'st_nlink': 1}
        if fh is not None:
            try:
                inode_number, timepoint = self.open_handles[fh]
            except KeyError:
                raise FuseOSError(errno.EBADF)
        else:
            timepoint, path = self._parse_path(path)
            inode_number = self.I.get_inode_for_path(path, timepoint)

        icache = self.open_inodes.get((inode_number, timepoint))
        if icache is not None:
            #print("Inode found in cache. mtime is {}".format(icache.st_mtime))
            return {'st_ino': icache.inode_number,
                    'st_atime': icache.st_atime/1000000.0,      # TODO: Standardise order
                    'st_gid': icache.st_gid,
                    'st_ctime': icache.st_ctime/1000000.0,
                    'st_btime': icache.st_btime/1000000.0,
                    'st_uid': icache.st_uid,
                    'st_size': icache.st_size,
                    'st_mode': icache.st_mode,
                    'st_blksize': 131072,
                    'st_mtime': icache.st_mtime/1000000.0,
                    'st_nlink': icache.st_nlink}
        else:
            #print("Inode not found in cache, getting from Index".format(path))
            return self.I.getattr(path=path, timepoint=timepoint)        
    
    def write(self, path, data, offset, fh, buffered=True):
        if path is not None:
            timepoint, path = self._parse_path(path)
            if timepoint is not None:
                raise FuseOSError(errno.EROFS)

        if self.readonly:
            raise FuseOSError(errno.EROFS)

        inode_number = self.open_handles[fh][0]
        self._cache_inode(inode_number, None)
        icache = self.open_inodes[(inode_number, None)]

        if buffered:
            if self._writebuffer["fh"] is None:
                self._writebuffer["fh"] = fh
                self._writebuffer["offset"] = offset
                self._writebuffer["data"] = data
            elif self._writebuffer["fh"] == fh:
                if self._writebuffer["offset"] + len(self._writebuffer["data"]) == offset:
                    if len(self._writebuffer["data"]) < 131072:
                        self._writebuffer["data"] += data
                    else:
                        self.write(None, self._writebuffer["data"], self._writebuffer["offset"], self._writebuffer["fh"], buffered=False)
                        self._writebuffer["offset"] = offset
                        self._writebuffer["data"] = data
                else:
                    self.write(None, self._writebuffer["data"], self._writebuffer["offset"], self._writebuffer["fh"], buffered=False)
                    self._writebuffer["offset"] = offset
                    self._writebuffer["data"] = data
            else:
                self.write(None, self._writebuffer["data"], self._writebuffer["offset"], self._writebuffer["fh"], buffered=False)
                self._writebuffer["offset"] = offset
                self._writebuffer["data"] = data
                self._writebuffer["fh"] = fh

            # It is important to keep the cache up to date in order to provide write-to-read consistency of write()/getattr()
            icache.st_size = max(icache.st_size, offset+len(data))
            icache.dirty = True
            return len(data)

        prev = None
        if self.options["deduplication"]:
            m = hashlib.sha256()
            m.update(data)
            prev = self.dedup.get(m.digest())   
            if prev is None:
                new_extents = self.L.write_data(data, offset)
                self.dedup[m.digest()] = copy.deepcopy(new_extents) # Important to take a COPY of the (mutable) list for use in the dedup dict.
            else:
                # The previous occurence of this block may not have been at the same logical start offset. Shift it.
                prev = copy.deepcopy(prev)         # Important to take a COPY of the (mutable) list as we'll be modifying it - we don't want to modify the dedup dictionary
                diff = prev[0].logical_start_offset - offset
                for e in prev:
                    e.logical_start_offset -= diff
                    e.calculate_properties()
                new_extents = prev
        else:
            new_extents = self.L.write_data(data, offset)
        
        new_mapping = reduce(Extent.splice, [icache.extent_list] + new_extents)
        icache.extent_list = new_mapping

        # Update the size of the file if it's grown. (It might not have).
        icache.st_size = max(icache.st_size, new_mapping[-1].logical_end_offset)

        icache.dirty = True

        return len(data)

    def fsync(self, path=None, datasync=None, fh=None):
        """TODO: Is there any point in fsyncing individual files? This function currently ignores all arguments and just syncs everything.."""
        if self.readonly:
            raise FuseOSError(errno.EROFS)
        self._flushwritebuffer()        # Always clear the write buffer before writing metadata to the Log
        syncable_inodes = [b for (i, t), b in self.open_inodes.items() if t is None and b.dirty]
        now = int(time.time()*1000000)
        self.L.flush_inodes(syncable_inodes, now)
        self.I.flush_inodes(syncable_inodes, now)
        volpos, logpos = self.L.get_head_pointer()
        self.I.update_istat(volpos, logpos)
        self.I.commit()
        self.L.fsync()
        self.I.fsync()

        for v in self.open_inodes.values():
            v.dirty = False

    def truncate(self, path, length):
        """1. Open the path
           2. Set st_size.
           3. Truncate any extents that lie beyond that size."""
        timepoint, path = self._parse_path(path)
        if self.readonly or timepoint is not None:
            raise FuseOSError(errno.EROFS)

        self._flushwritebuffer()                                                # Always clear the write buffer before writing metadata to the Log

        now = int(time.time()*1000000)

        inode_number = self.I.get_inode_for_path(path)
        self._uncache_inode(inode_number, timepoint, now)

        inode = self.I.get_inode(inode_number, now)

        # Truncate is only valid for regular files - check this is a regular file
        if stat.S_ISDIR(inode.st_mode):
            raise FuseOSError(errno.EISDIR)
        if not stat.S_ISREG(inode.st_mode):
            hdrfslog.error("Attempt to truncate a non-file")    # There seems to be no specific error message for this
            raise FuseOSError(errno.EIO)

        inode.st_size = length
        inode.extent_list = Extent.truncate(inode.extent_list, length)
        inode.st_ctime = now

        inode_as_list = [inode]
        self.L.flush_inodes(inode_as_list, now)
        self.I.flush_inodes(inode_as_list, now)
        volpos, logpos = self.L.get_head_pointer()
        self.I.update_istat(volpos, logpos)
        self.I.commit()

    
    def link(self, target, source, allow_dir=False):
        """allow_dir=True allows hard-linking of directories, on the understanding that this is part of a multi-part action
          which involves immediately unlinking the source directory so as not to break POSIX expectation of no loops in filesystems."""
        #print("Link target {} source {}".format(target, source))
        if source == '/'+self.options["history_directory"]:
            raise FuseOSError(errno.EROFS)
        timepoint, target = self._parse_path(target)
        if self.readonly or timepoint is not None:
            raise FuseOSError(errno.EROFS)
        
        now = int(time.time()*1000000)

        self._flushwritebuffer()        # Always clear the write buffer before writing metadata to the Log
        
        inode_number = self.I.get_inode_for_path(source)
        self._uncache_inode(inode_number, timepoint, now)
        inode = self.I.get_inode(inode_number, now)

        source_mode = inode.st_mode
        inode_type = self._mode2type(source_mode)
        if not allow_dir and inode_type=='d':
            hdrfslog.error("Cannot hard link directories")
            raise FuseOSError(errno.EISDIR)

        parentpath, targetname = os.path.split(target)
        parent_dir_inode_number = self.I.get_inode_for_path(parentpath)
        if parent_dir_inode_number is None:
            hdrfslog.error("HDRFS link: ERROR: Trying to open file {} in directory {} but this directory does not exist.".format(targetname, parentpath))
            raise FuseOSError(errno.ENOENT)
        else:

            # POSIX requires that the mtime and ctime of the parent directory are updated
            parent_dir_inode = self.I.get_inode(parent_dir_inode_number, now)     # Don't need to worry about consulting the cache - dirs are never cached
            parent_dir_inode.st_mtime = now
            parent_dir_inode.st_ctime = now
            self.L.write_metadata(parent_dir_inode)

            # No need to update st_nlink as it's derived every time.
            new_link = BlockLink(now, inode_number, parent_dir_inode_number, targetname)
            self.L.write_metadata(new_link)
            volpos, logpos = self.L.get_head_pointer()
            self.I.flush_inodes([parent_dir_inode], now)
            self.I.write_link(inode_number, parent_dir_inode_number, targetname, now)
            self.I.write_path(target, inode_number, inode_type, now)
            self.I.update_istat(volpos, logpos)
            self.I.commit()

    def unlink(self, path):
        if path == '/'+self.options["history_directory"]:
            raise FuseOSError(errno.EROFS)
        timepoint, path = self._parse_path(path)
        if self.readonly or timepoint is not None:
            raise FuseOSError(errno.EROFS)
        parentpath, fname= os.path.split(path)
        inode_number = self.I.get_inode_for_path(path)
        parent_dir_inode_number = self.I.get_inode_for_path(parentpath)
        if inode_number is None or parent_dir_inode_number is None:
            raise FuseOSError(errno.ENOENT)

        now = int(time.time()*1000000)

        self._flushwritebuffer()        # Always clear the write buffer before writing metadata to the Log
        self._uncache_inode(inode_number, timepoint, now)

        # POSIX requires that the mtime and ctime of the parent directory are updated
        parent_dir_inode = self.I.get_inode(parent_dir_inode_number, now)     # Don't need to worry about consulting the cache - dirs are never cached
        parent_dir_inode.st_mtime = now
        parent_dir_inode.st_ctime = now

        # Update Log        
        self.L.write_metadata(parent_dir_inode)
        self.L.write_metadata(BlockUnlink(now, inode_number, parent_dir_inode_number, fname))
        volpos, logpos = self.L.get_head_pointer()

        # Update Index
        self.I.flush_inodes([parent_dir_inode], now)
        self.I.unlink(path, now)
        self.I.update_istat(volpos, logpos)
        self.I.commit()


    def rename(self, old, new):
        if old == '/'+self.options["history_directory"]:
            raise FuseOSError(errno.EROFS)
        timepoint1, old = self._parse_path(old)
        timepoint2, new = self._parse_path(new)
        if self.readonly or timepoint1 is not None or timepoint2 is not None:
            raise FuseOSError(errno.EROFS)

        inode_number = self.I.get_inode_for_path(new)

        now = int(time.time()*1000000)

        self._flushwritebuffer()        # Always clear the write buffer before writing metadata to the Log
        self._uncache_inode(inode_number, None, now)
        
        if inode_number is not None:
            # Delete the target path if it exists already
            self.I.unlink(new, now)                                     # Do not Log the unlink - renames are atomic.
                                                                        # TODO: man 2 rename asks us to guarantee that new will remain in place in
                                                                        # the event of the rename operation failing for any reason.


        # POSIX requires that the mtime and ctime of the parent directory are updated (both if old and new are in different dirs)
        # Note that the parent dir time updates don't get written to the log - they are implicit in the BlockRename.
        # Index.rename takes care of updating parent dir times.

        # Update Log
        self.L.write_metadata(BlockRename(now, old, new))
        volpos, logpos = self.L.get_head_pointer()

        # Update Index
        self.I.rename(old, new, now)
        self.I.update_istat(volpos, logpos)
        self.I.commit()

    def chmod(self, path, mode):
        """Uses the cache in order to pick up any changes that might exist only in cache"""
        if path == '/'+self.options["history_directory"]:
            raise FuseOSError(errno.EROFS)
        timepoint, path = self._parse_path(path)
        if self.readonly or timepoint is not None:
            raise FuseOSError(errno.EROFS)
        inode_number = self.I.get_inode_for_path(path)
        if inode_number is None:
            raise FuseOSError(errno.ENOENT)

        now = int(time.time()*1000000)

        self._flushwritebuffer()                                                # Always clear the write buffer before writing metadata to the Log
        self._uncache_inode(inode_number, timepoint, now)
        inode = self.I.get_inode(inode_number, now)

        inode.st_mode = mode
        inode.st_ctime = now

        inode_as_list = [inode]
        self.L.flush_inodes(inode_as_list, now)
        self.I.flush_inodes(inode_as_list, now)
        volpos, logpos = self.L.get_head_pointer()
        self.I.update_istat(volpos, logpos)
        self.I.commit()

    def chown(self, path, uid, gid):
        """Uses the cache in order to pick up any changes that might exist only in cache"""
        if path == '/'+self.options["history_directory"]:
            raise FuseOSError(errno.EROFS)
        timepoint, path = self._parse_path(path)
        if self.readonly or timepoint is not None:
            raise FuseOSError(errno.EROFS)
        inode_number = self.I.get_inode_for_path(path)
        if inode_number is None:
            raise FuseOSError(errno.ENOENT)

        now = int(time.time()*1000000)

        self._flushwritebuffer()                                                # Always clear the write buffer before writing metadata to the Log
        self._uncache_inode(inode_number, timepoint, now)
        inode = self.I.get_inode(inode_number, now)

        if uid != -1:
            inode.st_uid = uid
        if gid != -1:
            inode.st_gid = gid
        if uid != -1 or gid != -1:
            inode.st_ctime = now


        inode_as_list = [inode]
        self.L.flush_inodes(inode_as_list, now)
        self.I.flush_inodes(inode_as_list, now)
        volpos, logpos = self.L.get_head_pointer()
        self.I.update_istat(volpos, logpos)
        self.I.commit()

    def rmdir(self, path):
        if path == '/'+self.options["history_directory"]:
            raise FuseOSError(errno.EROFS)
        timepoint, path = self._parse_path(path)
        if self.readonly or timepoint is not None:
            raise FuseOSError(errno.EROFS)
        parentpath, dname = os.path.split(path)
        inode_number = self.I.get_inode_for_path(path)
        parent_dir_inode_number = self.I.get_inode_for_path(parentpath)
        if inode_number is None or parent_dir_inode_number is None:
            raise FuseOSError(errno.ENOENT)
        if len(self.I.readdir(path)) > 0:
            raise FuseOSError(errno.ENOTEMPTY)
        now = int(time.time()*1000000)

        self._flushwritebuffer()        # Always clear the write buffer before writing metadata to the Log

        # POSIX requires that the mtime and ctime of the parent directory are updated
        parent_dir_inode = self.I.get_inode(parent_dir_inode_number, now)     # Don't need to worry about consulting the cache - dirs are never cached
        parent_dir_inode.st_mtime = now
        parent_dir_inode.st_ctime = now
        self.L.write_metadata(parent_dir_inode)

        self.L.write_metadata(BlockUnlink(now, inode_number, parent_dir_inode_number, dname))
        volpos, logpos = self.L.get_head_pointer()
        self.I.flush_inodes([parent_dir_inode], now)
        self.I.rmdir(path, inode_number)
        self.I.update_istat(volpos, logpos)
        self.I.commit()
        
    def symlink(self, target, source):
        timepoint, target = self._parse_path(target)
        if self.readonly or timepoint is not None:
            raise FuseOSError(errno.EROFS)
        if self.I.get_inode_for_path(target) is not None:
            raise FuseOSError(errno.EEXIST)
        inode_number = self.I.get_next_inode()
        parentpath, fname = os.path.split(target)
        parent_dir_inode_number = self.I.get_inode_for_path(parentpath)
        if parent_dir_inode_number is None:
            hdrfslog.error("""HDRFS.symlink: Trying to open file {} in directory {} but this directory does not exist""".format(fname, parentpath))
            raise FuseOSError(errno.ENOENT)
        else:
            self._flushwritebuffer()        # Always clear the write buffer before writing metadata to the Log

            now = int(time.time()*1000000)

            # POSIX requires that the mtime and ctime of the parent directory are updated
            parent_dir_inode = self.I.get_inode(parent_dir_inode_number, now)     # Don't need to worry about consulting the cache - dirs are never cached
            parent_dir_inode.st_mtime = now
            parent_dir_inode.st_ctime = now
            self.L.write_metadata(parent_dir_inode)

            st_uid, st_gid, _ = fuse_get_context()  
            inode = BlockInode(inode_number, now, stat.S_IFLNK | 0o777, st_uid, st_gid,
                               now, now, now, now, None, source)
            self.L.write_inode(inode)
            new_link = BlockLink(now, inode_number, parent_dir_inode_number, fname)
            self.L.write_metadata(new_link)
            volpos, logpos = self.L.get_head_pointer()
            self.I.flush_inodes([parent_dir_inode], now)
            self.I.write_inode(inode)
            self.I.write_link(inode_number, parent_dir_inode_number, fname)
            self.I.write_path(target, inode_number, 'l')
            self.I.update_istat(volpos, logpos)
            self.I.commit()

    def readlink(self, path):
        timepoint, path = self._parse_path(path)
        return self.I.readlink(path, timepoint)

    def setxattr(self, path, name, value, size):
        "Seems like this function is passed a Bytes object for value. TODO: What is size used for?"
        if path == '/'+self.options["history_directory"]:
            raise FuseOSError(errno.EROFS)
        timepoint, path = self._parse_path(path)
        if self.readonly or timepoint is not None:
            raise FuseOSError(errno.EROFS)
        inode_number = self.I.get_inode_for_path(path)
        if inode_number is None:
            raise FuseOSError(errno.ENOENT)

        self._flushwritebuffer()        # Always clear the write buffer before writing metadata to the Log
        timepoint = int(time.time()*1000000)
        block = BlockXattr(timepoint, inode_number, name, value)
        self.L.write_metadata(block)
        volpos, logpos = self.L.get_head_pointer()
        self.I.setxattr(block)
        self.I.update_istat(volpos, logpos)
        self.I.commit()
        return 0

    def listxattr(self, path):
        timepoint, path = self._parse_path(path)
        return self.I.listxattr(path, timepoint) 

    def getxattr(self, path, name):
        if path == '/'+self.options["history_directory"]:
            raise FuseOSError(errno.ENODATA)
        timepoint, path = self._parse_path(path)
        return self.I.getxattr(path, name, timepoint)

    def removexattr(self, path, name):
        timepoint, path = self._parse_path(path)
        if self.readonly or timepoint is not None:
            raise FuseOSError(errno.EROFS)
        inode_number = self.I.get_inode_for_path(path)
        if inode_number is None:
            raise FuseOSError(errno.ENOENT)

        self._flushwritebuffer()        # Always clear the write buffer before writing metadata to the Log
        timepoint = int(time.time()*1000000)
        block = BlockRemovedXattr(timepoint, inode_number, name)
        self.L.write_metadata(block)
        volpos, logpos = self.L.get_head_pointer()
        self.I.removexattr(block)
        self.I.update_istat(volpos, logpos)
        self.I.commit()
        return 0

    def statfs(self, path):
        """Return useful information about the filesystem for commands such as df.

        HDRFS cheats a little here. Filesystem size is not limited to a fixed number of blocks, 
        so it just reports the filesystem size as the amount of free space available to the parent
        folder of its mount point.

        Neither does HDRFS have a fixed block size. Here we pretend it is 128K in order to encourage
        applications to use this as the default read and write size.

        The total number of inodes is given as the theoretical maximum. While technically correct, this does
        push the column widths of df output far wider than will be familiar.

        """
        s = os.statvfs(self.options["mountpoint"]+'/..')     # Query the directory in which the mountpoint exists
        inode_count = self.I.statvfs()
        log_size = self.L.statvfs()
        return {'f_bsize':   131072,
                'f_frsize':  131072,
                'f_blocks':  int(s.f_bavail * s.f_bsize / 131072),
                'f_bfree':   int((s.f_bavail * s.f_bsize - log_size) / 131072),
                'f_bavail':  int((s.f_bavail * s.f_bsize - log_size) / 131072), 
                'f_files':   2**63 - 1,
                'f_ffree':   2**63 - 1 - inode_count,
                'f_favail':  2**63 - 1 - inode_count,
                'f_flag':    0,
                'f_namemax': 1024}
        

class HDRFSIndex(object):
    def __init__(self, db_file, create_new=False):
        """db_already_initialised=False causes this function to create empty tables"""
        self.db_file = db_file
        self.db = sqlite3.connect(db_file)
        # self.db.text_factory = bytes  # All TEXT types returned from sqlite will be cast into bytes objects, not str objects.
                                        # 2017-06-10 - No. fusepy enforces utf-8 so we'll have to go with that.
        cursor = self.db.cursor()
        cursor.execute("""
        PRAGMA synchronous = NORMAL;
        """)
        if create_new:
            cursor.execute("""
            PRAGMA journal_mode = WAL;
            """)
            # The inode table stores the stat data for the object
            cursor.execute("""
                CREATE TABLE inode (
                    inode INTEGER,
                    st_mode INTEGER,
                    st_uid INTEGER,
                    st_gid INTEGER,
                    st_atime INTEGER,
                    st_mtime INTEGER,
                    st_ctime INTEGER,
                    st_btime INTEGER,
                    st_size INTEGER,
                    opendate INTEGER,
                    closedate INTEGER
                )
            """)

            cursor.execute("""
                CREATE TABLE xattr (
                    inode INTEGER,
                    name TEXT,
                    value TEXT,
                    opendate INTEGER,
                    closedate INTEGER
                )
            """)
            # The inode_extents table maps inodes to physical locations
            # Note that this structure gives us sparse files for free! The logical file size is the maximum
            # logical_end_byte, and holes are intervals for which no mapping exists in this table.
            cursor.execute("""
                CREATE TABLE inode_extents (
                    inode INTEGER,
                    volume INTEGER,
                    physical_start_offset INTEGER,
                    block_size INTEGER,
                    block_multiplicity TEXT,
                    block_count INTEGER,
                    pre_truncate INTEGER,
                    post_truncate INTEGER,
                    logical_start_offset INTEGER,
                    opendate INTEGER,
                    closedate INTEGER
                )
            """)

            # The inode_symlinks table stores the target paths for inodes which are symlinks. 
            cursor.execute("""
                CREATE TABLE inode_symlinks (
                    inode INTEGER,
                    target_path TEXT,
                    opendate INTEGER,
                    closedate INTEGER
                )
            """)
            # The links table maps objects to parent objects (directories), giving the name under which they appear
            cursor.execute("""
                CREATE TABLE links (
                    child INTEGER,
                    parent INTEGER,
                    name TEXT,
                    opendate INTEGER,
                    closedate INTEGER
                )
            """)
            # The path table maps paths to inodes - it is a convenience cache of information held already in the inode and links tables.
            # type represents one of: file, directory, symlink, pipe, device, socket
            cursor.execute("""
                CREATE TABLE path (
                    path TEXT,
                    inode INTEGER,
                    type CHAR(1),
                    opendate INTEGER,
                    closedate INTEGER
                )
            """)
            # We need to keep track of some stats:
            # - the highest inode number used so far
            # - the highest volume that the Index represents
            # - the highest offset within the volume that the Index represents.
            cursor.execute("""
                CREATE TABLE istat (
                    maxinode INTEGER,
                    volpos INTEGER,
                    logpos INTEGER
                )
            """)

    def statvfs(self):
        """Return some information to be reported in the statvfs() call.

        Here we consider the highest inode number to be the count of used inodes,
        because an inode number cannot be reused.
        """
        cursor = self.db.cursor()
        cursor.execute("""SELECT maxinode FROM istat""")
        inode_count = cursor.fetchone()[0]

        return inode_count

    def highest_volume(self):
        "Returns the highest volume number that the Index knows about"
        cursor = self.db.cursor()
        cursor.execute("""
            SELECT max(volume) FROM inode_extents;
        """)
        row = cursor.fetchone()
        return row[0]

    def get_current_links(self):
        """Returns current (child, parent, name) tuples. Why ordered? 1) It's deterministic, 2) Lower entropy = better compression
           3) Something about it making recovery more transparent if you can see which ranges have been stored"""
        cursor = self.db.cursor()
        cursor.execute("""
            SELECT child, parent, name
            FROM links
            WHERE closedate=?
            ORDER by child, parent, name;
        """, (MAX_TIMEPOINT,))
        rows = cursor.fetchall()
        return rows

    def closeall(self):
        self.db.close()

    def flush_inodes(self, inodes, timepoint):
        """Updates the given set of inodes in the DB. timepoint is used to override whatever is in the BlockInodes' log_timstamp.

        inodes should be a list of BlockInodes. If empty, this function does nothing.
        """
        assert type(inodes) == list
        assert type(timepoint) == int
        cursor = self.db.cursor()
        for k in inodes:
            cursor.execute("""
                UPDATE inode SET closedate=? WHERE inode=? AND closedate=?
            """, (timepoint, k.inode_number, MAX_TIMEPOINT))
            cursor.execute("""
                INSERT INTO inode VALUES (?, ?, ?, ? ,? ,? ,? ,?, ?, ?, ?) 
            """, (k.inode_number,
                  k.st_mode,
                  k.st_uid,
                  k.st_gid,
                  k.st_atime,
                  k.st_mtime,
                  k.st_ctime,
                  k.st_btime,
                  k.st_size,
                  timepoint,
                  MAX_TIMEPOINT))
            assert Extent.is_extent_list_ordered(k.extent_list)
            cursor.execute("""
                UPDATE inode_extents SET closedate=? WHERE inode=? AND closedate=?
            """, (timepoint, k.inode_number, MAX_TIMEPOINT))
            cursor.executemany("""INSERT INTO inode_extents VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", [(k.inode_number,
                e.volume, e.physical_start_offset, e.block_size, e.block_multiplicity, e.block_count, e.pre_truncate, e.post_truncate,
                e.logical_start_offset, timepoint, MAX_TIMEPOINT) for e in k.extent_list])

    def fsync(self):
        cursor = self.db.cursor()
        cursor.execute("""PRAGMA wal_checkpoint;""")

    def getattr(self, path=None, inode=None, timepoint=None):
        "Look up stat attributes by path or by inode"

        if timepoint is None:
            timepoint = int(time.time()*1000000)

        cursor = self.db.cursor()
        if path is not None:
            cursor.execute("""SELECT inode FROM path WHERE path=:path
                                AND opendate <= :timepoint AND :timepoint < closedate
            """, {'path': path,
                  'timepoint': timepoint})
            row = cursor.fetchone()
            if row is None:
                raise FuseOSError(errno.ENOENT)
            else:
                inode = row[0]
        elif inode is not None:
            pass
        else:
            raise Exception("Index getattr: no arguments provided")

        cursor.execute("""
            SELECT count(*) FROM links WHERE child = :inode
                AND opendate <= :timepoint AND :timepoint < closedate
        """, {'inode': inode,
              'timepoint': timepoint})
        st_nlink = cursor.fetchone()[0] if path != '/' else 1       # Root dir not listed in the links table as it has no parent
        #print("getattr() finding nlink set to {}".format(st_nlink))


        cursor.execute("""
            SELECT * FROM inode WHERE inode = :inode
                AND opendate <= :timepoint AND :timepoint < closedate
        """, {'inode': inode,
              'timepoint': timepoint})

        row = cursor.fetchone()
        if row is None:
            raise FuseOSError(errno.ENOENT)
        else:
            # The stat structure always has proper UNIX timestamps; everywhere else in HDRFS uses integer microseconds.
            s = {'st_ino': row[0],
                 'st_mode': row[1],
                 'st_uid': row[2],
                 'st_gid': row[3],
                 'st_atime': row[4]/1000000.0,
                 'st_mtime': row[5]/1000000.0,
                 'st_ctime':row[6]/1000000.0,
                 'st_btime':row[7]/1000000.0,
                 'st_size': row[8],
                 'st_blksize': 131072,
                 'st_nlink': 1 if stat.S_ISDIR(row[1]) else st_nlink}                # Follows btrfs convention that st_nlink is always 1 for directories
            return s

    def get_inode(self, inode_number, timepoint=None):
        """Given an inode number and a timepoint, return a BlockInode representing the inode, using data from the Index"""
        if timepoint is None:
            timepoint = int(time.time()*1000000)
        cursor = self.db.cursor()
        cursor.execute("""
            SELECT volume, physical_start_offset, block_size, block_multiplicity, block_count, pre_truncate,
                   post_truncate, logical_start_offset
            FROM inode_extents 
            WHERE inode=:inode
              AND opendate <= :timepoint AND :timepoint < closedate 
            ORDER BY logical_start_offset
        """, {'inode': inode_number,
              'timepoint': timepoint})
        
        rows = cursor.fetchall()
        if rows is not None:
            extent_list = [Extent(*r) for r in rows]
        else:
            extent_list = []

        cursor.execute("""
            SELECT target_path FROM inode_symlinks WHERE inode = :inode_number
                AND opendate <= :timepoint AND :timepoint < closedate;
        """, {'inode_number': inode_number,
              'timepoint': timepoint})
        row = cursor.fetchone()
        if row is None:
            linktarget = ""
        else:
            linktarget = row[0]

        cursor.execute("""
            SELECT count(*) FROM links WHERE child = :inode_number
                AND opendate <= :timepoint AND :timepoint < closedate
        """, {'inode_number': inode_number,
              'timepoint': timepoint})
        st_nlink = cursor.fetchone()[0]

        cursor.execute("""
            SELECT * FROM inode WHERE inode = :inode_number
                AND opendate <= :timepoint AND :timepoint < closedate
        """, {'inode_number': inode_number,
              'timepoint': timepoint})

        row = cursor.fetchone()
        if row is None:
            raise FuseOSError(errno.ENOENT)
        else:
            return BlockInode(inode_number, timepoint,
                              st_mode=  row[1],
                              st_uid=   row[2],
                              st_gid=   row[3],
                              st_atime= row[4],
                              st_mtime= row[5],
                              st_ctime= row[6],
                              st_btime= row[7],
                              st_size=  row[8],
                              st_nlink= 1 if stat.S_ISDIR(row[1]) else st_nlink,  # Follows btrfs convention that st_nlink is always 1 for directories
                              extent_list=extent_list,
                              linktarget=linktarget)
        
    def get_extent_list(self, inode, timepoint=None):
        "Empty result set is valid. For example, in the case of a directory."
        if timepoint is None:
            timepoint = int(time.time()*1000000)
        cursor = self.db.cursor()
        cursor.execute("""
            SELECT volume, physical_start_offset, block_size, block_multiplicity, block_count, pre_truncate,
                   post_truncate, logical_start_offset
            FROM inode_extents 
            WHERE inode=:inode
              AND opendate <= :timepoint AND :timepoint < closedate 
            ORDER BY logical_start_offset
        """, {'inode': inode,
              'timepoint': timepoint})
        
        rows = cursor.fetchall()
        if rows is not None:
            return [Extent(*r) for r in rows]
        else:
            return []

    def initialise_root_dir(self, volpos, logpos, timepoint):
        cursor = self.db.cursor()
        # Insert a path for the root dir
        cursor.execute("""
            INSERT INTO path VALUES (?, 0, 'd', ?, ?)
        """, ('/', timepoint, MAX_TIMEPOINT))
        # Insert an inode for the root dir. Note that the size of all directories is 75 bytes due to the 
        # fixed size of an inode!
        cursor.execute("""
            INSERT INTO inode VALUES (0, :st_mode, :uid, :gid, :now, :now, :now, :now, 75, :now, :max_timepoint)
        """, {'st_mode': stat.S_IFDIR | 0o0777,
              'uid': os.getuid(),
              'gid': os.getgid(),
              'now': timepoint,
              'max_timepoint': MAX_TIMEPOINT})
        # Note that maxinode is set to 1. Zero is the root dir and One is the history dir.
        cursor.execute("""
            INSERT INTO istat VALUES (1, :volpos, :logpos);
        """, {'logpos': logpos,'volpos': volpos})
        self.db.commit()


    def get_head_pointer(self):
        cursor = self.db.cursor()
        cursor.execute("""
            SELECT volpos, logpos FROM istat;
        """)
        row = cursor.fetchone()
        if row is None:
            return (0,0)
        else:
            return row

    def get_path_for_inode(self, inode, timepoint=None):
        "A reverse lookup used when rebuilding the index"
        if timepoint is None:
            timepoint = int(time.time()*1000000)
        cursor = self.db.cursor()
        cursor.execute("""
            SELECT path FROM path WHERE inode=:inode
                AND opendate <= :timepoint AND :timepoint < closedate
        """, {'inode': inode,
              'timepoint': timepoint})
        row = cursor.fetchone()
        if row is None:
            return None
        else:
            return row[0]

    def get_inode_for_path(self, path, timepoint=None):
        if timepoint is None:
            timepoint = int(time.time()*1000000)
        cursor = self.db.cursor()
        cursor.execute("""
            SELECT inode FROM path WHERE path=:path
                AND opendate <= :timepoint AND :timepoint < closedate
        """, {'path': path,
              'timepoint': timepoint})
        row = cursor.fetchone()
        if row is None:
            return None
        else:
            return row[0]

    def get_next_inode(self):
        cursor = self.db.cursor()
        cursor.execute("""
            UPDATE istat SET maxinode=maxinode+1
        """)
        cursor.execute("""
            SELECT maxinode FROM istat
        """)
        return cursor.fetchone()[0]

    def rmdir(self, path, inode_number=None, timepoint=None):
        "Only path is strictly required to execute this function, but if a calling function has already looked up inode, allow it to be passed here"
        return self.unlink(path, timepoint)

    def readdir(self, path, offset=None, timepoint=None):
        if timepoint is None:
            timepoint = int(time.time()*1000000)


        cursor = self.db.cursor()
        cursor.execute("""
            SELECT inode FROM path WHERE path = :path
                AND opendate <= :timepoint AND :timepoint < closedate
        """, {'path': path,
              'timepoint': timepoint})
        inode_row = cursor.fetchone()
        if inode_row is None:
            raise FuseOSError(errno.ENOENT)
        else:
            inode = inode_row[0]
        cursor.execute("""
            SELECT name FROM links WHERE parent = :inode
                AND opendate <= :timepoint AND :timepoint < closedate
        """, {'inode': inode,
              'timepoint': timepoint})
        rows = cursor.fetchall()
        dir_entries = [r[0] for r in rows]
        return dir_entries

    def get_historical_timepoints(self, granularity):
        assert type(granularity) == str
        cursor = self.db.cursor()
        cursor.execute("""
            SELECT DISTINCT opendate FROM 
                (SELECT opendate FROM links
                UNION ALL
                SELECT opendate FROM path
                UNION ALL
                SELECT opendate FROM inode
                UNION ALL
                SELECT opendate FROM inode_extents
                UNION ALL
                SELECT opendate FROM inode_symlinks
                UNION ALL
                SELECT opendate FROM xattr
                )
                ORDER BY opendate
        """)
        rows = cursor.fetchall()
        # Logic: Show the first and last entries with full microsecond granularity.
        #        Then, truncate as per granularity option.
        #        Filter out any truncated dates that fall before or after the first or last entry respectively.
        timepoints = [datetime.datetime.fromtimestamp(r[0]/1000000.0, datetime.timezone.utc).astimezone(tz=None) for r in rows]
        if len(timepoints) in (1,2):            # It will always be at least 1 because we'll have the creation event for the root directory
            formatted_timepoints = [t.strftime('%Y-%m-%dT%H:%M:%S.%f%z') for t in timepoints]
        else:
            first_t = timepoints[0]
            last_t = timepoints[-1]
            if granularity == "microsecond":
                truncated_timepoints = set(timepoints[1:-1])
            elif granularity == "second":
                truncated_timepoints = set([t.replace(microsecond=0) for t in timepoints[1:-1]])
            elif granularity == "minute":
                truncated_timepoints = set([t.replace(microsecond=0, second=0) for t in timepoints[1:-1]])
            elif granularity == "hour":
                truncated_timepoints = set([t.replace(microsecond=0, second=0, minute=0) for t in timepoints[1:-1]])
            elif granularity == "day":
                truncated_timepoints = set([t.replace(microsecond=0, second=0, minute=0, hour=0) for t in timepoints[1:-1]])
            elif granularity == "month":
                truncated_timepoints = set([t.replace(microsecond=0, second=0, minute=0, hour=0, day=1) for t in timepoints[1:-1]])
            elif granularity == "year":
                truncated_timepoints = set([t.replace(microsecond=0, second=0, minute=0, hour=0, day=1, month=1) for t in timepoints[1:-1]])
    
            required_timepoints = [t for t in truncated_timepoints if first_t < t < last_t] + [first_t, last_t] # Order not important
            formatted_timepoints = [t.strftime('%Y-%m-%dT%H:%M:%S.%f%z') for t in required_timepoints]
        return formatted_timepoints


    def write_inode(self, inode, timepoint=None):

        if timepoint is None:
            timepoint = int(time.time()*1000000)

        cursor = self.db.cursor()
        cursor.execute("""INSERT INTO inode VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (inode.inode_number,
                                                                                          inode.st_mode,
                                                                                          inode.st_uid,
                                                                                          inode.st_gid,
                                                                                          inode.st_atime,
                                                                                          inode.st_mtime,
                                                                                          inode.st_ctime,
                                                                                          inode.st_btime,
                                                                                          inode.st_size,
                                                                                          timepoint,
                                                                                          MAX_TIMEPOINT))

        if stat.S_ISREG(inode.st_mode) and len(inode.extent_list) > 0:
            cursor.executemany("""INSERT INTO inode_extents VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", [(inode.inode_number,
                e.volume, e.physical_start_offset, e.block_size, e.block_multiplicity, e.block_count, e.pre_truncate, e.post_truncate,
                e.logical_start_offset, timepoint, MAX_TIMEPOINT) for e in inode.extent_list])
        elif stat.S_ISLNK(inode.st_mode):
            linktarget_binary = inode.linktarget
            cursor.execute("""INSERT INTO inode_symlinks VALUES (?, ?, ?, ?)""", (inode.inode_number, linktarget_binary, timepoint, MAX_TIMEPOINT))

    def write_link(self, inode, parent_dir_inode, name, timepoint=None):
        if timepoint is None:
            timepoint = int(time.time()*1000000)
        cursor = self.db.cursor()
        cursor.execute("""INSERT INTO links VALUES (?, ?, ?, ?, ?)""",(inode, parent_dir_inode, name, timepoint, MAX_TIMEPOINT))
        
    def rename(self, old, new, timepoint):
        old_parentpath, old_fname = os.path.split(old)
        new_parentpath, new_fname = os.path.split(new
)
        old_inode_number = self.get_inode_for_path(old)
        old_parent_inode_number = self.get_inode_for_path(old_parentpath)
        new_parent_inode_number = self.get_inode_for_path(new_parentpath)

        if old_inode_number is None or old_parent_inode_number is None or new_parent_inode_number is None:
            raise FuseOSError(errno.ENOENT)
        cursor = self.db.cursor()
        cursor.execute("""
            SELECT type FROM path WHERE path=:old AND closedate=:max_timepoint
            """, {"old": old, "max_timepoint": MAX_TIMEPOINT})
        filetype = cursor.fetchone()[0]

        cursor.execute("""
            INSERT INTO path SELECT :new, inode, type, :timepoint, :max_timepoint
                             FROM path
                             WHERE path = :old
                               AND closedate = :max_timepoint
            """, {"new": new, "timepoint": timepoint, "old": old, "max_timepoint":  MAX_TIMEPOINT})

        cursor.execute("""
            UPDATE path SET closedate=:timepoint
                        WHERE path = :old
                          AND closedate = :max_timepoint
            """, {"timepoint": timepoint, "old": old, "max_timepoint":  MAX_TIMEPOINT})

        cursor.execute("""
            INSERT INTO links VALUES (?, ?, ?, ?, ?)
            """, (old_inode_number, new_parent_inode_number, new_fname, timepoint, MAX_TIMEPOINT))

        cursor.execute("""
            UPDATE links SET closedate=:timepoint
                         WHERE child=:old_inode_number AND parent=:old_parent_inode_number AND name=:old_fname AND closedate=:max_timepoint
            """, {"timepoint": timepoint, "old_inode_number": old_inode_number,
                  "old_parent_inode_number": old_parent_inode_number, "old_fname": old_fname, "max_timepoint":  MAX_TIMEPOINT})

        if filetype == 'd':
            "Move all entries that were under old under new."
            cursor.execute("""INSERT INTO path SELECT path, inode, type, opendate, ? FROM path WHERE path LIKE ?
                           """, (timepoint, old+'/%'))
            cursor.execute("""UPDATE path SET path = ? || substr(path, ?) WHERE path LIKE ? AND closedate = ?""",
                           (new, len(old)+1, old+'/%', MAX_TIMEPOINT))

        # POSIX: update mtime and ctime for parent dirs.
        old_parent_inode = self.get_inode(old_parent_inode_number, timepoint)
        new_parent_inode = self.get_inode(new_parent_inode_number, timepoint)
        cursor.execute("""
            UPDATE inode SET closedate=? WHERE inode IN (?, ?) AND closedate=?
        """, (timepoint, old_parent_inode_number, new_parent_inode_number, MAX_TIMEPOINT))
        to_update = [old_parent_inode, new_parent_inode] if old_parent_inode_number != new_parent_inode_number else [old_parent_inode]
        for k in to_update:
            cursor.execute("""
                INSERT INTO inode VALUES (?, ?, ?, ? ,? ,? ,? ,?, ?, ?, ?) 
            """, (k.inode_number,
                  k.st_mode,
                  k.st_uid,
                  k.st_gid,
                  k.st_atime,
                  timepoint,
                  timepoint,
                  k.st_btime,
                  k.st_size,
                  timepoint,
                  MAX_TIMEPOINT))

        # POSIX: just update ctime on the inode itself
        k = self.get_inode(old_inode_number, timepoint)
        cursor.execute("""
            UPDATE inode SET closedate=? WHERE inode = ? AND closedate=?
        """, (timepoint, old_inode_number, MAX_TIMEPOINT))
        cursor.execute("""
            INSERT INTO inode VALUES (?, ?, ?, ? ,? ,? ,? ,?, ?, ?, ?) 
        """, (k.inode_number,
              k.st_mode,
              k.st_uid,
              k.st_gid,
              k.st_atime,
              k.st_ctime,
              timepoint,
              k.st_btime,
              k.st_size,
              timepoint,
              MAX_TIMEPOINT))

    def unlink(self, path, timepoint=None):
        if timepoint is None:
            timepoint = int(time.time()*1000000)
        # Some common parameters used in several queries in this function:
        parms = {"timepoint": timepoint, "path": path, "max_timepoint": MAX_TIMEPOINT}
        cursor = self.db.cursor()
        parent_path, fname = os.path.split(path)
        cursor.execute("""UPDATE links SET closedate=:timepoint
                            WHERE child =  (SELECT inode FROM path WHERE path=:path AND opendate <= :timepoint AND :timepoint < closedate)
                              AND parent = (SELECT inode FROM path WHERE path=:parent_path AND opendate <= :timepoint AND :timepoint < closedate)
                              AND name=:fname
                              AND closedate=:max_timepoint""", {**parms, "fname": fname, "parent_path": parent_path})
        cursor.execute("""SELECT count(*)
                          FROM links
                          WHERE child=(SELECT inode FROM path WHERE path=:path AND opendate <= :timepoint AND :timepoint < closedate)
                            AND closedate=:max_timepoint""", parms)
        result = cursor.fetchone()[0]
        if result == 0:
            cursor.execute("""UPDATE inode SET closedate=:timepoint
                              WHERE inode=(SELECT inode FROM path WHERE path=:path AND opendate <= :timepoint AND :timepoint < closedate)
                                AND closedate=:max_timepoint""", parms)
            cursor.execute("""UPDATE xattr SET closedate=:timepoint
                              WHERE inode=(SELECT inode FROM path WHERE path=:path AND opendate <= :timepoint AND :timepoint < closedate)
                                AND closedate=:max_timepoint""", parms)
            cursor.execute("""UPDATE inode_extents SET closedate=:timepoint
                              WHERE inode=(SELECT inode FROM path WHERE path=:path AND opendate <= :timepoint AND :timepoint < closedate)
                                AND closedate=:max_timepoint""", parms)
            cursor.execute("""UPDATE inode_symlinks SET closedate=:timepoint
                              WHERE inode=(SELECT inode FROM path WHERE path=:path AND opendate <= :timepoint AND :timepoint < closedate)
                                AND closedate=:max_timepoint""", parms)

        cursor.execute("""UPDATE path SET closedate=:timepoint WHERE path=:path AND closedate=:max_timepoint""", parms)
            
    def write_path(self, path, inode, filetype, timepoint=None):
        if timepoint is None:
            timepoint = int(time.time()*1000000)
        cursor = self.db.cursor()

        # Assert that no un-closedated path exists already

        cursor.execute("""
            SELECT count(*) FROM path WHERE path=? and closedate=?
        """, (path,  MAX_TIMEPOINT))
        row = cursor.fetchone()
        assert row[0] == 0

        cursor.execute("""
            INSERT INTO path VALUES (?, ?, ?, ?, ?)
        """, (path, inode, filetype, timepoint, MAX_TIMEPOINT))

    def _dump_tables(self):
        cursor = self.db.cursor()
        cursor.execute("""SELECT * from path""")
        print("Path: ", cursor.fetchall())
        cursor.execute("""SELECT * from links""")
        print("Link: ", cursor.fetchall())
        cursor.execute("""SELECT * from inode""")
        print("Inode: ", cursor.fetchall())
        cursor.execute("""SELECT * from inode_extents""")
        print("Extents: ", cursor.fetchall())
        cursor.execute("""SELECT * from inode_symlinks""")
        print("Symlinks: ", cursor.fetchall())
        cursor.execute("""SELECT * from xattr""")
        print("Xattrs: ", cursor.fetchall())
        
    def update_istat(self, volpos, logpos):
        cursor = self.db.cursor()
        cursor.execute("""UPDATE istat SET volpos=:volpos, logpos=:logpos""", (volpos, logpos))

    def set_maxinode(self, maxinode):
        """Used only from the Index rebuilding process"""
        cursor = self.db.cursor()
        cursor.execute("""UPDATE istat SET maxinode=:maxinode""", (maxinode,))

    def readlink(self, path, timepoint=None):
        if timepoint is None:
            timepoint = int(time.time()*1000000)
        cursor = self.db.cursor()
        cursor.execute("""
            SELECT target_path FROM inode_symlinks WHERE inode = (
                SELECT inode FROM path WHERE path = :path
                    AND opendate <= :timepoint AND :timepoint < closedate)
                AND opendate <= :timepoint AND :timepoint < closedate;
        """, {'path': path,
              'timepoint': timepoint})
        target_path = cursor.fetchone()[0]
        return target_path

        inode = self.I.get_inode_for_path(path)

    def setxattr(self, block):
        cursor = self.db.cursor()
        cursor.execute("""UPDATE xattr SET closedate = ? WHERE inode = ? AND name = ? AND closedate=?""",
             (block.log_timestamp, block.inode_number, block.name, MAX_TIMEPOINT))
        cursor.execute("""INSERT INTO xattr VALUES (?, ?, ?, ?, ?)""", 
            (block.inode_number, block.name, block.value, block.log_timestamp, MAX_TIMEPOINT))

    def listxattr(self, path, timepoint=None):
        "Expected return value: list"
        if timepoint is None:
            timepoint = int(time.time()*1000000)
        inode = self.get_inode_for_path(path)
        if inode is None:
            raise FuseOSError(errno.ENOENT)
        cursor = self.db.cursor()
        cursor.execute("""
            SELECT name FROM xattr WHERE inode = :inode
                AND opendate <= :timepoint AND :timepoint < closedate;
        """, {'inode': inode,
              'timepoint': timepoint})
        return [name[0] for name in cursor.fetchall()]

    def getxattr(self, path, name, timepoint=None):
        """Expected return value: byte string? Special-case lists the volumes on which an inode lies.
        Note the hack to work around fusepy issue #34"""
        if timepoint is None:
            timepoint = int(time.time()*1000000)
        inode = self.get_inode_for_path(path)
        if inode is None:
            raise FuseOSError(errno.ENOENT)
        cursor = self.db.cursor()
        if name == "hdrfs.volumes":
            cursor.execute("""
                SELECT DISTINCT volume FROM inode_extents WHERE inode = :inode
                    AND opendate <= :timepoint AND :timepoint < closedate;
            """, {'inode': inode,
                  'timepoint': timepoint})
            row = cursor.fetchall()
        
            result = ','.join([str(r[0]) for r in row]) + "\n"
            if len(result) < 65536:
                return result.encode()
            else:
                hdrfslog.info(("Request for hdrfs.volumes extended attribute on path {} returned result larger than 64KiB. "
                               "Printing in full below:\n\n".format(path)+result))
                return ("Request for extended attribute returned result larger than 64KiB. "
                        "The full result has been printed to the console where HDRFS was started. "
                        "Due to a limitation of fuse.py, this has been printed twice.\n".encode())

        elif name == "hdrfs.history":
            # Show all the times at which key file metadata changed
            cursor.execute("""
                SELECT DISTINCT opendate, st_mode, st_uid, st_gid, st_size
                FROM inode
                WHERE inode = :inode
                ORDER BY opendate;
            """, {'inode': inode})
            rows = cursor.fetchall()

            changelist = [datetime.datetime.utcfromtimestamp(r[0]/1000000).strftime('%Y-%m-%d %H:%M:%S') 
                          + " | " + oct(r[1]) 
                          + " | {0: <5}".format(r[2])
                          + " | {0: <5}".format(r[3])
                          + " | " + str(r[4]) 
                          for r in rows]
            result = ("Change time         | Mode     | UID   | GID   | Size (bytes)\n"
                      "--------------------|----------|-------|-------|-------------\n" + "\n".join(changelist) + "\n")
            if len(result) < 65536:
                return result.encode()
            else:
                hdrfslog.info(("Request for hdrfs.history extended attribute on path {} returned result larger than 64KiB. "
                              "Printing in full below:\n\n".format(path)+result))
                return ("Request for extended attribute returned result larger than 64KiB. "
                        "The full result has been printed to the console where HDRFS was started. "
                        "Due to a limitation of fuse.py, this has been printed twice.\n".encode())
        else:
            cursor.execute("""
                SELECT value FROM xattr WHERE inode = :inode AND name = :name
                    AND opendate <= :timepoint AND :timepoint < closedate
            """, {'inode': inode,
                  'name': name,
                  'timepoint': timepoint})
            row = cursor.fetchone()
            if row is None:
                raise FuseOSError(errno.ENODATA)
            else:
                return row[0]       # already bytes

    def removexattr(self, block):
        cursor = self.db.cursor()
        cursor.execute("""
            UPDATE xattr SET closedate=? WHERE inode = ? AND name = ? AND closedate=?
        """, (block.log_timestamp, block.inode_number, block.name, MAX_TIMEPOINT))


    def commit(self):
        self.db.commit()

class HDRFSVolumeManager(object):
    """Manages a collection of volumes and their file handles

       self._dict is populated with keys at startup. None is the value of each key, until a file handle is opened for that key.
        """

    def __init__(self, logdir):
        self.logdir = logdir
        self._dict = {}

    def __getitem__(self, vol):
        if vol not in self._dict:
            hdrfslog.error("Volume {} not present. Restore this volume and then restart HDRFS.".format(vol))
            raise FuseOSError(errno.EIO)
        if self._dict[vol] is not None:
            return self._dict[vol]
        else:
            try:
                f = self.open_volume(vol)
            except OSError as e:                    
                if e.errno == errno.EMFILE:         # This error means we have opened too many files. Close them all and start opening again.
                    self.closeall()
                    f = self.open_volume(vol)
                else:
                    raise e
            self._dict[vol] = f
            return f
    
    def open_volume(self, vol):
        return open(os.path.join(self.logdir, "L{:016d}.hdrfs".format(vol)), 'ab+')

    def register_volume(self,vol):
        self._dict[vol] = None
        
    def closeall(self):
        for vol in self._dict.keys():
            if self._dict[vol] is not None:
                self._dict[vol].close()
                self._dict[vol] = None
    
    def truncate_log(self, volpos, logpos):
        vol_length = os.path.getsize(self[volpos].name)
        print("Truncating volume", volpos, "to size", logpos, "(previous size was", vol_length, ")") 
        os.truncate(self[volpos].fileno(), logpos)
        for vol in self._dict.keys():
            if (vol > volpos) or (vol == volpos and logpos == 0):   #    If we are truncating back to the start of the volume, delete it
                print("Deleting volume", vol)
                os.remove(self[vol].name)
                self.close_volume(vol)            

    @property
    def head(self):
        x = self[max(self._dict.keys())] if len(self._dict.keys()) > 0 else None
        return x

    @property
    def head_vol_number(self):
        return max(self._dict.keys()) if len(self._dict.keys()) > 0 else None

    @property
    def all_available_volumes(self):
        return self._dict.keys()

    def next_available_volume(self, vol: int):
        "Given a vol number, return the next vol number that is available"
        try:
            return [k for k in sorted(self._dict.keys()) if k > vol][0]
        except IndexError:
            return None

    def prev_available_volume(self, vol: int):
        "Given a vol number, return the next vol number that is available"
        try:
            return [k for k in sorted(self._dict.keys()) if k < vol][-1]
        except IndexError:
            return None

    def close_volume(self, vol):
        # Should there be a periodic check to close volumes that have been unused for some time?
        self._dict[vol].close()
        self._dict[vol] = None

    def is_degraded(self):
        "Returns True if any are missing"
        missing_volumes = set(range(0,max(self._dict.keys())+1)) - set(self._dict.keys())
        if len(missing_volumes) > 0:
            self.degraded = True
            if len(missing_volumes) > 0:
                missing_volumes_list = list(missing_volumes)
                missing_volumes_list.sort()
                if len(missing_volumes) > 1:
                    hdrfslog.warning("Are the following volumes intentionally offline?\n{}".format(
                        ', '.join([str(x) for x in missing_volumes_list])))
                else:
                    hdrfslog.warning("Is the following volume intentionally offline?\n{}".format(
                        ', '.join([str(x) for x in missing_volumes_list])))
            return True
        else:
            return False

    def hash_volume(self, vol):
        "Calculates the SHA256 hash of a volume, returing the value as bytes"
        m = hashlib.sha256()
        f = open(os.path.join(self.logdir, "L{:016d}.hdrfs".format(vol)), 'rb')
        m.update(f.read())
        f.close()
        return m.digest()

    def statvfs(self):
        "Return space used by all online volumes"
        total = 0
        for vol in self._dict.keys():
            total += os.path.getsize(os.path.join(self.logdir, "L{:016d}.hdrfs".format(vol)))
        return total
        
class HDRFSLog(object):

    def __init__(self, logdir, options, create_new=False, I=None):
        """The log needs a reference to the index. Supply it early as a parameter, or late using the register_index method"""
        self.logdir = logdir    
        self.options = options
        self._I = I
        self.volumes = HDRFSVolumeManager(logdir)   # Holds a map from vol number to file handle - lazily populated
        if create_new:
            self.fs_id = uuid.uuid4().bytes_le
            self._newvolume(self.fs_id)
        else:
            self.fs_id = None
            matches = glob.glob(os.path.join(self.logdir, "L????????????????.hdrfs"))
            if matches is not None:
                for m in matches:
                    _, fname = os.path.split(m)
                    try:
                        self.volumes.register_volume(int(fname[1:17])) # Volumes can only be registered at startup or when newly created.
                    except ValueError:
                        hdrfslog.warning("Log.__init__: file {} does not follow naming convention - ignored".format(fname))
        self.degraded = self.volumes.is_degraded()
        self._scan_volumes_for_uuid()

    def statvfs(self):
        return self.volumes.statvfs()

    def truncate_log(self, volpos, logpos):
        "A potentially dangerous option. Called when a bad block is found. Truncates the log to the point immediately prior to the bad block."
        self.volumes.truncate_log(volpos, logpos)

    def register_index(self, I):
        self._I = I

    def _scan_volumes_for_uuid(self):
        for volume in self.volumes.all_available_volumes:
            _, block = self._parse_blockheader(volume, 0)
            if self.fs_id not in (None, block.fs_id):
                hdrfslog.error("All volumes must have the same filesystem unique ID. Have you started HDRFS with a mixture of volumes from different filesystems?")
                sys.exit()
            else:
                self.fs_id = block.fs_id

    def _dump_log_to_text_file(self, filename):
        f = open(filename, 'w')
        for block in self._parse_log(allow_noncontiguity=True):
            f.write(repr(block) + "\n\n")
        f.close()
            
    def _parse_log(self, volcursor=0, logcursor=0, allow_noncontiguity=False):
        volpos, logpos = self.get_head_pointer()  # Define the end of the log
        volume_hashes = {}  # Keep track of the SHA256 hashes so we can verify them
        while (volcursor, logcursor) != (volpos, logpos):
            block_start = logcursor # logcursor is modified during the parsing process, so track the start here for reporting.
            if logcursor == 0:
                logcursor, block = self._parse_blockheader(volcursor, logcursor)
                if len(volume_hashes.keys()) > 0 and block.prev_vol_hash != volume_hashes[self.volumes.prev_available_volume(volcursor)]:
                    raise LogParserException("Error in volume header: Hash of previous file mismatch", volcursor, logcursor)
            else:
                block_type = self.get_bytes(volcursor, logcursor, 1)
                if block_type == BlockNull.block_id:               # The null blocktype is empty space / null bytes
                    zero_count = 0
                    while logcursor < self.get_volume_size(volcursor):
                        logcursor += 1
                        zero_count += 1
                        block_type = self.get_bytes(volcursor, logcursor, 1)
                        if block_type != b'\x00':
                            break # Ignore if we've read into the next block, or read an empty byte (b'') meaning EoF 
                    block = BlockNull(zero_count)
                elif block_type == BlockInode.block_id:
                    logcursor, block = self._parse_inode(volcursor, logcursor)
                elif block_type == BlockLink.block_id:
                    logcursor, block = self._parse_link(volcursor, logcursor)
                elif block_type == BlockUnlink.block_id:
                    logcursor, block = self._parse_unlink(volcursor, logcursor)
                elif block_type == BlockXattr.block_id:
                    logcursor, block = self._parse_xattr(volcursor, logcursor)
                elif block_type == BlockRemovedXattr.block_id:
                    logcursor, block = self._parse_RemovedXattr(volcursor, logcursor)
                elif block_type == BlockData.block_id:
                    logcursor, block = self._parse_data(volcursor, logcursor)
                elif block_type == BlockRename.block_id:
                    logcursor, block = self._parse_rename(volcursor, logcursor)
                elif block_type == BlockLinkTable.block_id:
                    logcursor, block = self._parse_linktable(volcursor, logcursor)
                else:
                    e = LogParserException("Unrecognized block type {}. Trying next byte. ".format(
                        volcursor, logcursor, block_type), volcursor, logcursor)
                    if self.options["ignore_metadata_errors"]:
                        hdrfslog.warning(repr(e))
                        logcursor += 1
                    else:
                        raise e
            yield (volcursor, block_start), block
            # Detect end of volume and move on to next volume
            if logcursor == self.get_volume_size(volcursor):
                volume_hashes[volcursor] = self.volumes.hash_volume(volcursor)
                logcursor = 0
                new_volcursor = self.volumes.next_available_volume(volcursor)
                if new_volcursor == None:
                    break
                elif new_volcursor > volcursor + 1 and not allow_noncontiguity:
                    raise NonContiguityError(volcursor+1)
                else:
                    volcursor = new_volcursor

    def _parse_blockheader(self, volcursor, logcursor):
        """No errors can be ignored here."""
        assert logcursor == 0
        volume_header = self.get_bytes(volcursor, logcursor, 80)

        magic_id = volume_header[:17]
        if magic_id != BlockHeader.magic_id:
            raise LogParserException("Invalid header".format(volcursor), volcursor, logcursor)
        
        version = volume_header[17:18]
        if version != b'\x00':
            raise LogParserException("Unrecognized file format version ({})".format(version), volcursor, logcursor)

        fs_id = volume_header[18:34]           
        # no check on fs_id - any value is theoretically allowable

        crc_algo = volume_header[34:35]
        if crc_algo != b'\x00':
            raise LogParserException("Unrecognized CRC algorithm (this HDRFS implementation only supports CRC32)".format(
                    volcursor), volcursor, logcursor)

        hash_algo = volume_header[35:36]
        if hash_algo != b'\x00':
            raise LogParserException("Unrecognized volume hash algorithm "
                                     "(this HDRFS implementation only supports SHA256)".format(volcursor), volcursor, logcursor)

        # Check the CRC before the vol_seq_no so we can report the reliability of the filename
        CRC = struct.pack("<L", zlib.crc32(volume_header[:76]))
        if CRC != volume_header[76:80]:
            raise LogParserException("Error in volume {} at offset 0: header CRC check failed".format(volcursor), volcursor, logcursor)
                
        try:
            vol_seq_no = struct.unpack("<Q", volume_header[36:44])[0]
        except struct.error:
            raise LogParserException("Could not read header block volume sequence number", volcursor, logcursor)
        if vol_seq_no != volcursor:
            vol_formatted_name = "L{:016}.hdrfs".format(volcursor)
            raise LogParserException("Volume number does not match its filename."
                    "File {} claims to contain volume {} (and the CRC check passes).".format(vol_formatted_name, volcursor), volcursor, logcursor)

        prev_vol_hash = volume_header[44:76]
        if volcursor == 0:
            if prev_vol_hash != b'\x00'*32:
                raise LogParserException("Error in volume 0 at offset 44:"
                                         " This is the first volume but it references a previous volume({}).".format(prev_vol_hash), volcursor, logcursor)

        logcursor = 80
        return logcursor, BlockHeader(fs_id, prev_vol_hash, vol_seq_no)

    def _parse_link(self, volcursor, logcursor):
        initial_offset = logcursor  # Save for the end where we will re-read everything to check the CRC
        try:
            log_timestamp = struct.unpack("<Q", self.get_bytes(volcursor, logcursor+1, 8))[0]       # The +1 is the block_id
        except struct.error:
            e = LogParserException("Could not read link block log timestamp", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e))  # Not fatal
            else:
                raise e
        logcursor += 8 + 1

        try:
            child_inode_number = struct.unpack("<Q", self.get_bytes(volcursor, logcursor, 8))[0]
        except struct.error:
            e = LogParserException("Could not read link block child_inode number", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e)) 
                return logcursor, BlockNull()
            else:
                raise e
        logcursor += 8

        try:
            parent_inode_number = struct.unpack("<Q", self.get_bytes(volcursor, logcursor, 8))[0]
        except struct.error:
            e = LogParserException("Could not read link block parent_inode number", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e)) 
                return logcursor, BlockNull()
            else:
                raise e
        logcursor += 8

        try:
            name_length = struct.unpack("<H", self.get_bytes(volcursor, logcursor, 2))[0]
        except struct.error:
            e = LogParserException("Could not read link block name length", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e)) 
                return logcursor, BlockNull()
            else:
                raise e
        logcursor += 2

        try:
            name = self.get_bytes(volcursor, logcursor, name_length).decode('utf-8')
        except struct.error:
            e = LogParserException("Could not read link block name", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e)) 
                return logcursor, BlockNull()
            else:
                raise e
        except UnicodeDecodeError:
            e = LogParserException("Invalid UTF-8 data in link block name", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e)) 
                return logcursor, BlockNull()
            else:
                raise e
        logcursor += name_length

        # Check the CRC
        whole_block = self.get_bytes(volcursor, initial_offset, logcursor - initial_offset)
        try:
            CRC = struct.unpack("<L", self.get_bytes(volcursor, logcursor, 4))[0]
        except struct.error:
            e = LogParserException("Error reading link block CRC", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e))   # Not fatal - metadata could be fine
            else:
                raise e

        CRC_check = zlib.crc32(whole_block)
        if CRC_check != CRC:
            e = LogParserException("Unlink block CRC check failed".format(
                volcursor, initial_offset), volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e))   # Not fatal - metadata could be fine
            else:
                raise e
        logcursor += 4

        return logcursor, BlockLink(log_timestamp, child_inode_number, parent_inode_number, name)

    def _parse_unlink(self, volcursor, logcursor):
        initial_offset = logcursor  # Save for the end where we will re-read everything to check the CRC
        try:
            log_timestamp = struct.unpack("<Q", self.get_bytes(volcursor, logcursor+1, 8))[0]       # The +1 is the block_id
        except struct.error:
            e = LogParserException("Could not read unlink block log timestamp", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e))  # Not fatal
            else:
                raise e
        logcursor += 8 + 1

        try:
            child_inode_number = struct.unpack("<Q", self.get_bytes(volcursor, logcursor, 8))[0]
        except struct.error:
            e = LogParserException("Could not read unlink block child_inode number", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e)) 
                return logcursor, BlockNull()
            else:
                raise e
        logcursor += 8

        try:
            parent_inode_number = struct.unpack("<Q", self.get_bytes(volcursor, logcursor, 8))[0]
        except struct.error:
            e = LogParserException("Could not read unlink block parent_inode number", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e)) 
                return logcursor, BlockNull()
            else:
                raise e
        logcursor += 8

        try:
            name_length = struct.unpack("<H", self.get_bytes(volcursor, logcursor, 2))[0]
        except struct.error:
            e = LogParserException("Could not read unlink block name length", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e)) 
                return logcursor, BlockNull()
            else:
                raise e
        logcursor += 2

        try:
            name = self.get_bytes(volcursor, logcursor, name_length).decode('utf-8')
        except struct.error:
            e = LogParserException("Could not read unlink block name", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e)) 
                return logcursor, BlockNull()
            else:
                raise e
        except UnicodeDecodeError:
            e = LogParserException("Invalid UTF-8 data in unlink block name", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e)) 
                return logcursor, BlockNull()
            else:
                raise e
        logcursor += name_length

        # Check the CRC
        whole_block = self.get_bytes(volcursor, initial_offset, logcursor - initial_offset)
        try:
            CRC = struct.unpack("<L", self.get_bytes(volcursor, logcursor, 4))[0]
        except struct.error:
            e = LogParserException("Error reading unlink block CRC", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e))   # Not fatal - metadata could be fine
            else:
                raise e

        CRC_check = zlib.crc32(whole_block)
        if CRC_check != CRC:
            e = LogParserException("Unlink block CRC check failed".format(
                volcursor, initial_offset), volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e))   # Not fatal - metadata could be fine
            else:
                raise e
        logcursor += 4

        return logcursor, BlockUnlink(log_timestamp, child_inode_number, parent_inode_number, name)

    def _parse_xattr(self, volcursor, logcursor):
        initial_offset = logcursor  # Save for the end where we will re-read everything to check the CRC
        try:
            log_timestamp = struct.unpack("<Q", self.get_bytes(volcursor, logcursor+1, 8))[0]         # The +1 is the block_id
        except struct.error:
            e = LogParserException("Could not read xattr block log timestamp", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e))   # Not fatal - metadata could be fine
            else:
                raise e
        logcursor += 8 + 1
        try:
            inode_number = struct.unpack("<Q", self.get_bytes(volcursor, logcursor, 8))[0]
        except struct.error:
            e = LogParserException("Could not read xattr block inode number", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e))   
                return logcursor, BlockNull()
            else:
                raise e
        logcursor += 8
        
        try:
            name_length = struct.unpack("<B", self.get_bytes(volcursor, logcursor, 1))[0]
        except struct.error:
            e = LogParserException("Could not read xattr block name length", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e))   
                return logcursor, BlockNull()
            else:
                raise e
        logcursor += 1
        
        try:
            value_length = struct.unpack("<H", self.get_bytes(volcursor, logcursor, 2))[0]
        except struct.error:
            e = LogParserException("Could not read xattr block value length", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e))
                return logcursor, BlockNull()
            else:
                raise e
        logcursor += 2

        try:
            name = self.get_bytes(volcursor, logcursor, name_length).decode('utf-8')
        except struct.error:
            e = LogParserException("Could not read xattr block name", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e))
                return logcursor, BlockNull()
            else:
                raise e
        except UnicodeDecodeError:
            e = LogParserException("Invalid UTF-8 data in xattr block name", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e))
                return logcursor, BlockNull()
            else:
                raise e
        logcursor += name_length

        try:
            value = self.get_bytes(volcursor, logcursor, value_length)
        except struct.error:
            e = LogParserException("Could not read xattr block value", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e)) 
                return logcursor, BlockNull()
            else:
                raise e
        logcursor += value_length

        # Check the CRC
        whole_block = self.get_bytes(volcursor, initial_offset, logcursor - initial_offset)
        try:
            CRC = struct.unpack("<L", self.get_bytes(volcursor, logcursor, 4))[0]
        except struct.error:
            e = LogParserException("Error reading xattr block CRC", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e))   # Not fatal - metadata could be fine
            else:
                raise e

        CRC_check = zlib.crc32(whole_block)
        if CRC_check != CRC:
            e = LogParserException("Xattr block CRC check failed".format(
                volcursor, initial_offset), volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e))   # Not fatal - metadata could be fine
            else:
                raise e
        logcursor += 4

        return logcursor, BlockXattr(log_timestamp, inode_number, name, value)

    def _parse_RemovedXattr(self, volcursor, logcursor):
        initial_offset = logcursor  # Save for the end where we will re-read everything to check the CRC
        try:
            log_timestamp = struct.unpack("<Q", self.get_bytes(volcursor, logcursor+1, 8))[0]       # The +1 is the block_id
        except struct.error:
            e = LogParserException("Could not read RemovedXattr block timestamp", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e))   # Not fatal
            else:
                raise e
        logcursor += 8 + 1

        try:
            inode_number = struct.unpack("<Q", self.get_bytes(volcursor, logcursor, 8))[0]
        except struct.error:
            e = LogParserException("Could not read RemovedXattr block inode number", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e))
                return logcursor, BlockNull()
            else:
                raise e
        logcursor += 8

        try:
            name_length = struct.unpack("<B", self.get_bytes(volcursor, logcursor, 1))[0]
        except struct.error:
            e = LogParserException("Could not read RemovedXattr block name length", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e))
                return logcursor, BlockNull()
            else:
                raise e
        logcursor += 1

        try:
            name = self.get_bytes(volcursor, logcursor, name_length).decode('utf-8')
        except struct.error:
            e = LogParserException("Could not read RemovedXattr block name", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e))
                return logcursor, BlockNull()
            else:
                raise e
        logcursor += name_length

        # Check the CRC
        whole_block = self.get_bytes(volcursor, initial_offset, logcursor - initial_offset)
        try:
            CRC = struct.unpack("<L", self.get_bytes(volcursor, logcursor, 4))[0]
        except struct.error:
            e = LogParserException("Error reading RemovedXattr block CRC", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e))   # Not fatal - metadata could be fine
            else:
                raise e

        CRC_check = zlib.crc32(whole_block)
        if CRC_check != CRC:
            e = LogParserException("RemovedXattr block CRC check failed".format(
                volcursor, initial_offset), volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e))   # Not fatal - metadata could be fine
            else:
                raise e
        logcursor += 4

        return logcursor, BlockRemovedXattr(log_timestamp, inode_number, name)

    def _parse_data(self, volcursor, logcursor):
        initial_offset = logcursor  # Save for the end where we will re-read everything to check the CRC

        try:
            log_timestamp = struct.unpack("<Q", self.get_bytes(volcursor, logcursor+1, 8))[0]      # The +1 is the block_id 
        except struct.error:
            e = LogParserException("Could not read data block timestamp", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e))
            else:
                raise e
        logcursor += 8 + 1

        # The --ignore-data-errors option has no effect here - inability to read the data length is too severe an error to ignore.
        try:
            payload_length = struct.unpack("<Q", self.get_bytes(volcursor, logcursor, 8))[0]
        except struct.error:
            raise LogParserException("Could not read data block payload length", volcursor, initial_offset)
        logcursor += 8

        payload = self.get_bytes(volcursor, logcursor, payload_length)
        logcursor += payload_length

        # Check the CRC
        try:
            CRC = struct.unpack("<L", self.get_bytes(volcursor, logcursor, 4))[0]
        except struct.error:
            e = LogParserException("Could not read data block CRC", volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e))
            else:
                raise e

        whole_block = self.get_bytes(volcursor, initial_offset, logcursor - initial_offset)
        CRC_check = zlib.crc32(whole_block)
        if CRC_check != CRC:
            e = LogParserException("Data block CRC check failed".format(
                volcursor, initial_offset), volcursor, initial_offset)
            if self.options["ignore_data_errors"]:
                hdrfslog.warning(repr(e))
            else:
                raise e
        logcursor += 4

        return logcursor, BlockData(log_timestamp, payload)

    def _parse_rename(self, volcursor, logcursor):
        """Given the starting location of a Rename block, parse it. Raises errors unless the --ignore-metadata-errors option is set,
        though errors will generate a null block - with the exception of timestamp and CRC errors, as these are potentially non-critical information."""
        initial_offset = logcursor  # Save for the end where we will re-read everything to check the CRC

        try:
            log_timestamp = struct.unpack("<Q", self.get_bytes(volcursor, logcursor+1, 8))[0]      # The +1 is the block_id 
        except struct.error:
            e = LogParserException("Error reading rename block log timestamp", volcursor, initial_offset)
            if self.options["ignore_metadata_errors"]:
                hdrfslog.warning(repr(e))
            else:
                raise e 
        logcursor += 8 + 1
        
        try:
            oldpath_length = struct.unpack("<H", self.get_bytes(volcursor, logcursor, 2))[0]
        except struct.error:
            e = LogParserException("Error reading rename block previous path length", volcursor, initial_offset)
            if self.options["ignore_metadata_errors"]:
                hdrfslog.warning(repr(e))
                return logcursor, BlockNull()
            else:
                raise e 
        logcursor += 2

        try:
            newpath_length = struct.unpack("<H", self.get_bytes(volcursor, logcursor, 2))[0]
        except struct.error:
            e = LogParserException("Error reading rename block new path length", volcursor, initial_offset)
            if self.options["ignore_metadata_errors"]:
                hdrfslog.warning(repr(e))
                return logcursor, BlockNull()
            else:
                raise e 
        logcursor += 2
        
        try:
            oldpath = self.get_bytes(volcursor, logcursor, oldpath_length).decode('UTF-8')
        except struct.error:
            e = LogParserException("Error reading rename block previous path", volcursor, initial_offset)
            if self.options["ignore_metadata_errors"]:
                hdrfslog.warning(repr(e))
                return logcursor, BlockNull()
            else:
                raise e 
        logcursor += oldpath_length

        try:
            newpath = self.get_bytes(volcursor, logcursor, newpath_length).decode('UTF-8')
        except struct.error:
            e = LogParserException("Error reading rename block new path", volcursor, initial_offset)
            if self.options["ignore_metadata_errors"]:
                hdrfslog.warning(repr(e))
                return logcursor, BlockNull()
            else:
                raise e 
        logcursor += newpath_length

        # Check the CRC
        whole_block = self.get_bytes(volcursor, initial_offset, logcursor - initial_offset)
        try:
            CRC = struct.unpack("<L", self.get_bytes(volcursor, logcursor, 4))[0]
        except struct.error:
            e = LogParserException("Could not read rename block CRC", volcursor, initial_offset)
            if self.options["ignore_metadata_errors"]:
                hdrfslog.warning(repr(e))
            else:
                raise e 

        CRC_check = zlib.crc32(whole_block)

        if CRC_check != CRC:
            e = LogParserException("Rename block CRC check failed", volcursor, initial_offset)
            if self.options["ignore_metadata_errors"]:
                hdrfslog.warning(repr(e))
            else:
                raise e 
        logcursor += 4

        return logcursor, BlockRename(log_timestamp, oldpath, newpath)

    def _parse_linktable(self, volcursor, logcursor):
        """Given the starting location of a Linktable block, parse it. Raises errors unless the --ignore-metadata-errors option is set,
        though errors will generate a null block - with the exception of timestamp and CRC errors, as these are potentially non-critical information."""
        initial_offset = logcursor  # Save for the end where we will re-read everything to check the CRC

        try:
            entries = struct.unpack("<Q", self.get_bytes(volcursor, logcursor + 1, 8))[0]       # The +1 is the block_id 
        except struct.error:
            e = LogParserException("Could not read linktable count of entries", volcursor, initial_offset)
            if self.options["ignore_metadata_errors"]:
                hdrfslog.warning(repr(e))
            else:
                raise e 
        logcursor += 8 + 1

        i = 0
        links = []
        while i < entries:
            try:
                child = struct.unpack("<Q", self.get_bytes(volcursor, logcursor, 8))[0]
            except struct.error:
                e = LogParserException("Could not read child inode of linktable entry {}".format(i), volcursor, initial_offset)
                if self.options["ignore_metadata_errors"]:
                    hdrfslog.warning(repr(e))
                else:
                    raise e 
            logcursor += 8

            try:
                parent = struct.unpack("<Q", self.get_bytes(volcursor, logcursor, 8))[0]
            except struct.error:
                e = LogParserException("Could not read parent inode of linktable entry {}".format(i), volcursor, initial_offset)
                if self.options["ignore_metadata_errors"]:
                    hdrfslog.warning(repr(e))
                    return logcursor, BlockNull()
                else:
                    raise e 
            logcursor += 8
            
            try:
                name_length = struct.unpack("<H", self.get_bytes(volcursor, logcursor, 2))[0]
            except struct.error:
                e = LogParserException("Could not read name length of linktable entry {}".format(i), volcursor, initial_offset)
                if self.options["ignore_metadata_errors"]:
                    hdrfslog.warning(repr(e))
                    return logcursor, BlockNull()
                else:
                    raise e 
            logcursor += 2

            try:
                name = self.get_bytes(volcursor, logcursor, name_length).decode('UTF-8')
            except struct.error:
                e = LogParserException("Could not read name from linktable entry {}".format(i), volcursor, initial_offset)
                if self.options["ignore_metadata_errors"]:
                    hdrfslog.warning(repr(e))
                    return logcursor, BlockNull()
                else:
                    raise e 
            logcursor += name_length
            links.append((child, parent, name))
            i += 1

        # Check the CRC
        whole_block = self.get_bytes(volcursor, initial_offset, logcursor - initial_offset)
        try:
            CRC = struct.unpack("<L", self.get_bytes(volcursor, logcursor, 4))[0]
        except struct.error:
            e = LogParserException("Could not read linktable block CRC", volcursor, initial_offset)
            if self.options["ignore_metadata_errors"]:
                hdrfslog.warning(repr(e))   # Lack of CRC is not fatal - data could be fine.
            else:
                raise e 
        CRC_check = zlib.crc32(whole_block)
        if CRC_check != CRC:
            e = LogParserException("Linktable block CRC check failed", volcursor, initial_offset)
            if self.options["ignore_metadata_errors"]:
                hdrfslog.warning(repr(e))   # Lack of CRC is not fatal - data could be fine.
            else:
                raise e 
        logcursor += 4

        return logcursor, BlockLinkTable(links)

    def _parse_inode(self, volcursor, logcursor):
        """Given a volume and a logcursor pointed at an inode record, parse it, and return the new logcursor position
        plus the inode object"""
        initial_offset = logcursor  # Save for the end where we will re-read everything to check the CRC
        fixed_length = 70
        v = self.get_bytes(volcursor, logcursor+1, fixed_length)    # The +1 is the block_id
        logcursor += fixed_length + 1

        try:
            inode_number =  struct.unpack("<Q", v[0:8])[0]
        except struct.error:
            e = LogParserException("Could not read inode block inode number", volcursor, initial_offset)
            if self.options["ignore_metadata_errors"]:
                hdrfslog.warning(repr(e))
                return logcursor, BlockNull()
            else:
                raise e 
        try:
            log_timestamp = struct.unpack("<Q", v[8:16])[0]
        except struct.error:
            e = LogParserException("Could not read inode block log timestamp", volcursor, initial_offset)
            if self.options["ignore_metadata_errors"]:
                hdrfslog.warning(repr(e))
            else:
                raise e 

        try:
            st_mode =       struct.unpack("<H", v[16:18])[0]
        except struct.error:
            e = LogParserException("Could not read inode block st_mode", volcursor, initial_offset)
            if self.options["ignore_metadata_errors"]:
                hdrfslog.warning(repr(e))
                return logcursor, BlockNull()
            else:
                raise e 

        try:
            st_uid =        struct.unpack("<H", v[18:20])[0]
        except struct.error:
            e = LogParserException("Could not read inode block st_uid", volcursor, initial_offset)
            if self.options["ignore_metadata_errors"]:
                hdrfslog.warning(repr(e))
                return logcursor, BlockNull()
            else:
                raise e 

        try:
            st_gid =        struct.unpack("<H", v[20:22])[0]
        except struct.error:
            e = LogParserException("Could not read inode block st_gid", volcursor, initial_offset)
            if self.options["ignore_metadata_errors"]:
                hdrfslog.warning(repr(e))
                return logcursor, BlockNull()
            else:
                raise e 

        try:
            st_atime =      struct.unpack("<Q", v[22:30])[0]
        except struct.error:
            e = LogParserException("Could not read inode block st_atime", volcursor, initial_offset)
            if self.options["ignore_metadata_errors"]:
                hdrfslog.warning(repr(e))
                return logcursor, BlockNull()
            else:
                raise e 

        try:
            st_mtime =      struct.unpack("<Q", v[30:38])[0]
        except struct.error:
            e = LogParserException("Could not read inode block st_mtime", volcursor, initial_offset)
            if self.options["ignore_metadata_errors"]:
                hdrfslog.warning(repr(e))
                return logcursor, BlockNull()
            else:
                raise e 

        try:
            st_ctime =      struct.unpack("<Q", v[38:46])[0]
        except struct.error:
            e = LogParserException("Could not read inode block st_ctime", volcursor, initial_offset)
            if self.options["ignore_metadata_errors"]:
                hdrfslog.warning(repr(e))
                return logcursor, BlockNull()
            else:
                raise e 

        try:
            st_btime =      struct.unpack("<Q", v[46:54])[0]
        except struct.error:
            e = LogParserException("Could not read inode block st_btime", volcursor, initial_offset)
            if self.options["ignore_metadata_errors"]:
                hdrfslog.warning(repr(e))
                st_btime = 0        # This is a non-fatal error given that st_btime is currently invisible to POSIX operations.
            else:
                raise e 
        try:
            st_size =       struct.unpack("<Q", v[54:62])[0]
        except struct.error:
            e = LogParserException("Could not read inode block st_size", volcursor, initial_offset)
            if self.options["ignore_metadata_errors"]:
                hdrfslog.warning(repr(e))
                return logcursor, BlockNull()
            else:
                raise e 

        try:
            var_length =    struct.unpack("<Q", v[62:70])[0]
        except struct.error:
            e = LogParserException("Could not read inode block var_length", volcursor, initial_offset)
            if self.options["ignore_metadata_errors"]:
                hdrfslog.warning(repr(e))
                return logcursor, BlockNull()
            else:
                raise e 

        v = self.get_bytes(volcursor, logcursor, var_length)
        logcursor += var_length

        extent_list = []
        linktarget = ""
        # inode records can have some variable length trailing info if they are regular files or symlinks.
        if stat.S_ISREG(st_mode):
            extent_count = int(var_length / 57)   # Always 7 fields of length 8 each, plus 1 for multiplicity. 7*8+1=57
            cur_extent = 0
            while cur_extent != extent_count:
                off = 57*cur_extent

                try:
                    extent_volume =                 struct.unpack("<Q", v[off+ 0:off+ 8])[0]
                except struct.error:
                    e = LogParserException("Could not read inode block extent volume number", volcursor, initial_offset)
                    if self.options["ignore_metadata_errors"]:
                        hdrfslog.warning(repr(e))
                        return logcursor, BlockNull()
                    else:
                        raise e 

                try:
                    extent_physical_start_offset =  struct.unpack("<Q", v[off+ 8:off+16])[0]
                except struct.error:
                    e = LogParserException("Could not read inode block extent physical start offset", volcursor, initial_offset)
                    if self.options["ignore_metadata_errors"]:
                        hdrfslog.warning(repr(e))
                        return logcursor, BlockNull()
                    else:
                        raise e 
                
                try:
                    extent_block_size =             struct.unpack("<Q", v[off+16:off+24])[0]
                except struct.error:
                    e = LogParserException("Could not read inode block extent block size", volcursor, initial_offset)
                    if self.options["ignore_metadata_errors"]:
                        hdrfslog.warning(repr(e))
                        return logcursor, BlockNull()
                    else:
                        raise e 

                try:
                    extent_block_multiplicity =     v[off+24:off+25]
                except struct.error:
                    e = LogParserException("Could not read inode block extent block multiplicity", volcursor, initial_offset)
                    if self.options["ignore_metadata_errors"]:
                        hdrfslog.warning(repr(e))
                        return logcursor, BlockNull()
                    else:
                        raise e 

                try:
                    extent_block_count =            struct.unpack("<Q", v[off+25:off+33])[0]
                except struct.error:
                    e = LogParserException("Could not read inode block extent block count", volcursor, initial_offset)
                    if self.options["ignore_metadata_errors"]:
                        hdrfslog.warning(repr(e))
                        return logcursor, BlockNull()
                    else:
                        raise e 

                try:
                    extent_pre_truncate =           struct.unpack("<Q", v[off+33:off+41])[0]
                except struct.error:
                    e = LogParserException("Could not read inode block extent pre truncate", volcursor, initial_offset)
                    if self.options["ignore_metadata_errors"]:
                        hdrfslog.warning(repr(e))
                        return logcursor, BlockNull()
                    else:
                        raise e 

                try:
                    extent_post_truncate =          struct.unpack("<Q", v[off+41:off+49])[0]
                except struct.error:
                    e = LogParserException("Could not read inode block extent post truncate", volcursor, initial_offset)
                    if self.options["ignore_metadata_errors"]:
                        hdrfslog.warning(repr(e))
                        return logcursor, BlockNull()
                    else:
                        raise e 
                
                try:
                    extent_logical_start_offset =   struct.unpack("<Q", v[off+49:off+57])[0]
                except struct.error:
                    e = LogParserException("Could not read inode block extent logical start offset", volcursor, initial_offset)
                    if self.options["ignore_metadata_errors"]:
                        hdrfslog.warning(repr(e))
                        return logcursor, BlockNull()
                    else:
                        raise e 

                extent_list.append(
                    Extent(extent_volume, extent_physical_start_offset, extent_block_size, extent_block_multiplicity,
                           extent_block_count, extent_pre_truncate, extent_post_truncate, extent_logical_start_offset))
                cur_extent += 1
        elif stat.S_ISLNK(st_mode):
            try:
                linktarget = v.decode('UTF-8')
            except UnicodeDecodeError:
                e = LogParserException("Invalid UTF-8 data in inode block symlink target", volcursor, initial_offset)
                if self.options["ignore_metadata_errors"]:
                    hdrfslog.warning(repr(e))
                    return logcursor, BlockNull()
                else:
                    raise e 
            

        # Check the CRC
        whole_inode_record = self.get_bytes(volcursor, initial_offset, logcursor - initial_offset)
        try:
            CRC = struct.unpack("<L", self.get_bytes(volcursor, logcursor, 4))[0]
        except struct.error:
            e = LogParserException("Could not read inode block CRC", volcursor, initial_offset)
            if self.options["ignore_metadata_errors"]:
                hdrfslog.warning(repr(e))   # Lack of CRC is not fatal - data could be fine.
            else:
                raise e 
        CRC_check = zlib.crc32(whole_inode_record)
        if CRC_check != CRC:
            e = LogParserException("Inode block CRC check failed", volcursor, initial_offset)
            if self.options["ignore_metadata_errors"]:
                hdrfslog.warning(repr(e))   # Lack of CRC is not fatal - data could be fine.
            else:
                raise e 
        logcursor += 4

        # Insert all this inode data into the index

        return logcursor, BlockInode(inode_number, log_timestamp, st_mode, st_uid, st_gid, st_atime, st_mtime, st_ctime,
            st_btime, extent_list, linktarget, st_size)


    def closeall(self):
        self.volumes.closeall()

    def _newvolume(self, fs_id=None):
        """Closes the current volume (if there is one), hashes it, creates a new volume, opens it (writeable), and updates head"""
        if fs_id == None:
            fs_id = self.fs_id
        head_vol_number = self.volumes.head_vol_number
        if head_vol_number is not None:
            hdrfslog.info("Starting new volume")
            self.volumes.close_volume(head_vol_number)
            prev_vol_hash = self.volumes.hash_volume(head_vol_number)              # len 32
        else:
            prev_vol_hash = b'\x00'*32
        vol_seq_no = head_vol_number+1 if head_vol_number is not None else 0    

        b = BlockHeader(fs_id, prev_vol_hash, vol_seq_no)

        self.volumes.register_volume(vol_seq_no)
        self.volumes.head.write(bytes(b))
        
        b = bytes(BlockLinkTable(self._I.get_current_links()))
        if self.options["volume_size"] - self.volumes.head.tell() <= len(b):
            hdrfslog.warning("Not enough space in volume to write metadata. Volume {} will not be independently recoverable".format(vol_seq_no))
        else:
            self.volumes.head.write(bytes(b))
        



    def _finalize_volume(self):
        "Fill the rest of the volume with zeros and start a new one"
        head = self.volumes.head
        cur_pos = head.tell()
        zeros = self.options["volume_size"] - cur_pos
        if zeros > 0:
            head.write(b"\x00"*zeros)
        self._newvolume()


    def initialise_root_dir(self, timepoint):
        inode = BlockInode(0, timepoint, stat.S_IFDIR | 0o777, os.getuid(), os.getgid(), timepoint, timepoint, timepoint, timepoint)
        self.write_inode(inode)
        return self.get_head_pointer()


    def get_head_pointer(self):
        f = self.volumes.head
        logpos = f.seek(0, os.SEEK_END)
        return (self.volumes.head_vol_number, logpos)

    def _write_metadata(self, dat):
        """Metadata cannot span multiple volumes. Luckily all metadata is small."""
        if len(dat) > self.options["volume_size"] - self.volumes.head.tell():
            self._finalize_volume()
        self.volumes.head.seek(0, os.SEEK_END)
        self.volumes.head.write(dat)

    def flush_inodes(self, inodes=[], timepoint=None):
        """timepoint is used to override whatever is in the BlockInodes' log_timestamp. 
        
        This is to keep the Log monotonically increasing.
        
        """
        for k in inodes:
            k.log_timestamp = timepoint
            self._write_metadata(bytes(k))

    def fsync(self):
        self.volumes.head.flush()
        os.fsync(self.volumes.head.fileno())

    def write_data(self, dat, logical_start_offset):
        """Give it some data and the logical offset, and it will return a list of extents where that
        logical data is stored. Note that an extent cannot cross a volume boundary.

        """
        assert type(dat) == bytes

        self.volumes.head.seek(0, os.SEEK_END)

        min_space_requirement = 22   # 22 byte header ('\x06' plus 8 byte log_timestamp plus 8 byte length,
                                     # plus 4 byte CRC footer, and at least 1 byte of data)

        # Start to build a list of written extents. This is what we need to return.
        extents = []

        now = int(time.time()*1000000)

        # Loop until we've written the whole length of dat
        written = 0
        while written < len(dat):
            space_left_in_volume = self.options["volume_size"]-self.volumes.head.tell()
            if space_left_in_volume < min_space_requirement:
                self._finalize_volume()
                continue
            length_to_write = space_left_in_volume - min_space_requirement + 1      # The min constant includes room for at least 1 byte of data,
                                                                                    # as there is no point writing a block with no data in it.
                                                                                    # However, as long as there is room to write some data, it is
                                                                                    # valid to make use of that 1 byte, hence the +1.
            block_payload = dat[written:written+length_to_write]
            block_header = BlockData.block_id   #   TODO: How much of an optimisation is it to hard-code this to b'\x06'? As this is inner loop territory.
            block_header += struct.pack("<Q", now)
            block_header += struct.pack("<Q", len(block_payload))
            block = block_header + block_payload
            block_footer = struct.pack("<L", zlib.crc32(block))
            physical_start_offset = self.volumes.head.tell()
            self.volumes.head.write(block)
            self.volumes.head.write(block_footer)
            extents.append(Extent(self.volumes.head_vol_number, physical_start_offset, len(block_payload), b'C', 1, 0, 0, logical_start_offset+written))
            written += len(block_payload)

        return extents

    def write_inode(self, inode):
        """Serialise all information required to describe a file. Note that this is not
        a full stat structure; many of the stat entries are sourced from the Index or irrelevant
        for hdrfs.
        """
        self._write_metadata(bytes(inode))

    def write_metadata(self, block):
        self._write_metadata(bytes(block))

    def get_bytes(self, vol, offset, size=None, block_size=None):
        """Returns arbitrary bytes from a volume. If size is ommitted, returns
           the whole block starting at offset (checking block size and CRC
           (optionally) and failing if no block begins at offset)."""
        f = self.volumes[vol]
        f.seek(offset)
        if size is not None:
            return f.read(size)
        else:
            block = f.read(17+block_size)       # 17 = 1 (0x06 block type) + 8 (log_timestamp) + 8 (length)
            if self.options['integrity_checks']:
                if block[0:1] != b'\x06':
                    hdrfslog.error("Attempting to read data block but {} block found instead of data block".format(block[0:1]))
                    raise FuseOSError(errno.EIO)
                    # TODO: close the volume so that a re-open is attempted - that way a user can put back a volume that was previously removed.
                log_timestamp_binary = struct.unpack("<Q", block[1:9])[0]     # Can this be validated? Maybe a warning if less than previous? But then this function acquires state...
                block_size_binary = struct.unpack("<Q", block[9:17])[0]     # struct.unpack always returns a tuple
                if block_size_binary != block_size:
                    hdrfslog.error("Unexpected block length: found {}, expecting {}".format(block_size_binary, block_size))
                    raise FuseOSError(errno.EIO)
                block_crc = struct.unpack("<L", f.read(4))[0]
                if block_crc != zlib.crc32(block):
                    if self.options["ignore_data_errors"]:
                        hdrfslog.warning("CRC mismatch in volume {} at offset {}. Returning data anyway.".format(vol, offset))
                    else:
                        hdrfslog.error("CRC mismatch in volume {} at offset {}.".format(vol, offset))
                        raise FuseOSError(errno.EIO)
            return block[17:]
                
    def get_volume_size(self, vol):
        return os.fstat(self.volumes[vol].fileno()).st_size

    def read(self, extent_list, size, offset, st_size):         # st_size needed in order to measure out sparse bytes

        last_logical_end = 0        # Used for detecting sparse gaps between extents
        
        buf = b""
        for e in extent_list:
            # Construct sparse extents since the last extent
            sparse_region_start = last_logical_end
            sparse_region_end = e.logical_start_offset
            if sparse_region_end - sparse_region_start > 0:
                if sparse_region_end >= offset and sparse_region_start < offset + size:
                    sparse_logical_pre_truncate = max(sparse_region_start, offset) - sparse_region_start
                    sparse_logical_post_truncate = sparse_region_end - min(sparse_region_end, offset+size)
                    buf += b"\x00" * (sparse_region_end - sparse_region_start - sparse_logical_pre_truncate - sparse_logical_post_truncate)
                
            # Cut extents down to size
            if e.logical_end_offset >= offset and e.logical_start_offset < offset + size:
                logical_pre_truncate = max(e.logical_start_offset, offset) - e.logical_start_offset
                logical_post_truncate = e.logical_end_offset - min(e.logical_end_offset, offset+size)
                total_pre_truncate = e.pre_truncate + logical_pre_truncate
                total_post_truncate = e.post_truncate + logical_post_truncate
                # frbifrb = first required byte in first required block
                (first_required_block, frbifrb) = divmod(total_pre_truncate, e.block_size) 
                # lrbilrb = first required byte in last required block
                (last_required_block, lrbilrb) = divmod(e.block_size * e.block_count - total_post_truncate, e.block_size)
                if lrbilrb == 0:
                    last_required_block -= 1
                    lrbilrb = e.block_size

                for i in range(first_required_block, last_required_block + 1):
                    # 21 = 1 (0x06 block type) + 8 (log timestamp) + 8 (block length) + 4 (crc)
                    if first_required_block == last_required_block:
                        s = slice(frbifrb, lrbilrb)
                    else:
                        if i == first_required_block:
                            s = slice(frbifrb, None)
                        elif i == last_required_block:
                            s = slice(None, lrbilrb)
                    if e.block_multiplicity == b'R':
                        i = 0                               # Intentionally overrides the loop counter so that get_bytes reads from the same block, repeatedly.
                    buf += self.get_bytes(e.volume, e.physical_start_offset + i*(e.block_size + 21), size=None, block_size=e.block_size)[s]
            last_logical_end = e.logical_end_offset

    
        # Construct trailing sparse bytes
        trailing_sparsity = min(st_size - max(offset, last_logical_end), size - len(buf))
        if trailing_sparsity > 0:
            buf += b"\x00" * trailing_sparsity

        return buf

class Controller(object):
    """
    This class handles UI, and the mapping of FUSE operations to the HDRFS implementation.

    """
    def __init__(self, cmdline_args):
        parser = OptionParser()
        # logdir and indexdir take precedence over datadir
        parser.add_option("--datadir", dest="datadir", default=".",
                            help="Store Log, Index and Dedup files in DATADIR", metavar="DATADIR")
        parser.add_option("--logdir", dest="logdir", default=None,
                            help="Store Log files in LOGDIR (overrides --datadir)", metavar="LOGDIR")
        parser.add_option("--indexdir", dest="indexdir", default=None,
                            help="Store the Index file in INDEXDIR (overrides --datadir)", metavar="INDEXDIR")
        parser.add_option("--dedupdir", dest="dedupdir", default=None,
                            help="Store the deduplication file in DEDUPDIR (overrides --datadir)", metavar="DEDUPDIR")
        parser.add_option("--no-deduplication", dest="deduplication", action="store_false", default=True,
                            help="Disable deduplication")
        parser.add_option("--volume-size", dest="volume_size", default="1G",
                            help="The size, in bytes, of each HDRFS data volume. Suffixes K, M, G, T, P, E are valid.")
        parser.add_option("--atime", dest="atime", action="store_true", default=False,
                            help="Records access times.")
        parser.add_option("--ignore-data-errors", dest="ignore_data_errors", action="store_true", default=False,
                            help="When rebuilding the log and accessing data, report errors to the console rather than halting.")
        parser.add_option("--ignore-metadata-errors", dest="ignore_metadata_errors", action="store_true", default=False,
                            help="When rebuilding the log, report errors to the console rather than halting.")
        parser.add_option("--nuclear-fsck", dest="nuclear_fsck", default=False, action="store_true",
                            help="""If you have received an error message indicating corruption in the Log,
                                    use this option to remove the corrupt block. THIS IS A DESTRUCTIVE OPERATION.
                                    If the offending block is at the head of the Log, fsck will truncate the Log to the last
                                    uncorrupt block.""")
        parser.add_option("--dump-log-to-text-file", dest="dump_log_to_text_file", default=None,
                            help="""Print the Log in a human readable text representation, to the specified file""")
        parser.add_option("--listvol", dest="listvol", default=None, type=int,
                            help="Prints the contents of VOL", metavar="VOL") # TODO: implement! This is a non-mounting mode, just prints contents.
        parser.add_option("--disable-integrity-checks", action="store_false", dest="integrity_checks", default=True,
                            help="""Skip checking CRCs. Assume all data read from the log is valid.""")
        parser.add_option("--history-directory", dest="history_directory", default="history",
                            help="""Name the top-level directory which contains historical snapshots of the filesystem.""")
        parser.add_option("--history-directory-granularity", dest="history_directory_granularity", default="day",
                            choices=["microsecond", "second", "minute", "hour", "day", "month", "year"],
                            help="""The level of summarization at which the historical snapshot directory presents snapshot folders.
                                    N.B. History is *always* recorded at microsecond granularity. This option merely changes
                                    the presentation, and does not prevent you from navigating to other history directories.
                                    """)
        parser.add_option("--startup-notification", dest="startup_notification", default=False, action="store_true",
                            help="""Prints the string 'STARTUP COMPLETE' to standard output. Useful for signalling to
                                    another program that HDRFS has finished initialising.""")
        parser.add_option("--version", dest="version", default=False, action="store_true",
                            help="""Prints the version, then exits.""")
        parser.add_option("--allow-other", dest="allow_other", default=False, action="store_true",
                            help="""Allows other users to access the HDRFS mount point. PERMISSIONS ARE NOT ENFORCED. 
                                    This option only works if 'user_allow_other' is set in /etc/fuse.conf""")
        

        (options, args) = parser.parse_args(cmdline_args)
        options_dict = vars(options)
        
        if options_dict["version"]:
            print("HDRFS Version", HDRFS_VERSION)
            sys.exit()
        if len(args) != 1:
            print("Usage: ./hdrfs.py [options] <mount point>")
            sys.exit()

        vs = options_dict["volume_size"]
        if vs[-1] == 'K':
            volume_size = int(vs[0:-1])*1000**1
        elif vs[-1] == 'M':
            volume_size = int(vs[0:-1])*1000**2
        elif vs[-1] == 'G':
            volume_size = int(vs[0:-1])*1000**3
        elif vs[-1] == 'T':
            volume_size = int(vs[0:-1])*1000**4
        elif vs[-1] == 'P':
            volume_size = int(vs[0:-1])*1000**5
        elif vs[-1] == 'E':                         # No point supporting Z or Y SI prefixes, as a volume cannot store more than 
            volume_size = int(vs[0:-1])*1000**6     # 2^64 bytes due to the use of 64-bit integers as offsets.
        else:
            volume_size = int(vs)
        if volume_size > 2**64:
            print("ERROR: Volume size cannot exceed 2^64")
            sys.exit()
        options_dict["volume_size"] = volume_size

        mountpoint = os.path.normpath(args[0])
        options_dict["mountpoint"] = mountpoint
        # Check for access to the mount point. Both write and execute permission is required.
        if not os.access(mountpoint, os.W_OK | os.X_OK):
            print("User has no write and execute permission to the specified mount point: {}".format(mountpoint))
            sys.exit()
        # Remind user how to exit cleanly. Ctrl+C is not generally safe.
        hdrfslog.info("""HDRFS running. Exit cleanly with the following command:

        fusermount -u {}
            """.format(mountpoint))

        fuse = FUSE(HDRFS(options_dict), mountpoint, nothreads=True, foreground=True, big_writes=True, max_write=131072,
                    use_ino=True, allow_other=options_dict["allow_other"])
        hdrfslog.info("""HDRFS exiting cleanly.""") # TODO When is this case not reached? If never, is the fusermount advice necessary?


if __name__=="__main__":
    # Execute when script is run from the command line
    Controller(sys.argv[1:])
