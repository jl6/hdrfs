# HDRFS file format specification

The HDRFS volume file format consists of a sequence of variable-length blocks.

Every volume begins with a BlockHeader. Every volume except the first then
includes a BlockLinkTable. Every volume then contains zero or more blocks of the
following types: BlockNull, BlockInode, BlockLink, BlockUnlink, BlockXattr,
BlockRemovedXattr, BlockData, BlockRename.

In the definitions below, the tables list the length and order of fields in
the blocks.

## Common elements

### Timestamps

Timestamps are stored as an integer number of microseconds since the epoch.
Blocks in an HDRFS volume should have monotonically increasing timestamp values.
Epoch timestamps are independent of timezone and DST so should never go
backwards. Timestamps are always stored as 64-bit little-endian unsigned
integers.

### CRCs

The CRC is a 32-bit little-endian unsigned integer as returned from the CRC
function. It defines the CRC value of all bytes in the block up to but not
including the CRC value.

### Inode numbers

The inode number is a 64-bit little-endian unsigned integer.

## BlockHeader

The header is a fixed-length prefix for every volume file.

| Length | Field                                  |
| ------ | -------------------------------------- |
|     17 | Magic bytes                            |
|      1 | Version                                |
|     16 | Filesystem ID                          |
|      1 | CRC algorithm                          |
|      1 | Hash algorithm                         |
|      8 | Volume sequence number                 |
|     32 | Previous volume hash                   |
|      4 | CRC                                    |

   
The file format magic bytes are:

    D3 48 44 52 46 53 0D 0A 1A 0A 00 48 44 52 46 53 00

Magic bytes may be used to identify files as being in HDRFS format.

Version is 00 for the current version of the HDRFS file format.

Filesystem ID is a 16-byte random UUID. This must be consistent across all
volume files in a filesystem.

CRC algorithm values are as follows:

| Value | Meaning                        |
| ----- | ------------------------------ |
|  00   | CRC-32 (polynomial 0x04C11DB7) |

(Only one CRC algorithm is defined in the current version of HDRFS)

Hash algorithm values are as follows:

| Value | Meaning                        |
| ----- | ------------------------------ |
|  00   | SHA256                         |

(Only one value is defined in the current version of HDRFS)

The volume sequence number is a 64-bit little-endian unsigned integer. It must
be sequential from one volume to the next, starting from zero.

The previous volume hash is a 32-byte value as returned from the hash algorithm.
In the first volume, this position is occupied by 32 null bytes. In all other
volumes, it is the hash of the previous volume file.

Note that the size of the volume is not defined anywhere within the volume.

## BlockInode

The inode block stores `stat` fields, and a list of extents mapping the logical
structure of the file to the physical structure of HDRFS volumes. If the inode
is a symlink, it stores the link target. For any given inode number, the most
recent BlockInode represents the current version.

| Length | Field                                  |
| ------ | -------------------------------------- |
|      1 | Block ID                               |
|      8 | Inode number                           |
|      8 | Log timestamp                          |
|      2 | st_mode                                |
|      2 | st_uid                                 |
|      2 | st_gid                                 |
|      8 | st_atime                               |
|      8 | st_mtime                               |
|      8 | st_ctime                               |
|      8 | st_btime                               |
|      8 | st_size                                |
|      8 | Variable Length Size                   |
|      n | Variable Length Portion                |
|      4 | CRC                                    |


Block ID is 1

The Log timestamp records the moment the block was written.

st_mode, st_uid, and st_gid are 16-bit little-endian unsigned integers recording the file mode
bits, user ID and group ID respectively.

st_atime, st_mtime, st_ctime, and st_btime are 64-bit little-endian unsigned
integers recording the number of microseconds between the epoch and the file's
latest access time, modification time, metadata change time, and file creation
time respectively.

st_size is a 64-bit little-endian unsigned integer recording the size 
of the file. The size of a regular file is its conventional size in bytes. The
size of a symbolic link is 70 bytes plus the length of the link target. The size
of all other file types is 70 bytes.

Variable Length Size is a 64-bit little-endian unsigned integer recording the
length n of the next part of the block.

If the file is a regular file, then the Variable Length Portion is a List of
Extent Structures. If the file is a symbolic link, then the Variable Length
Portion is the UTF-8 encoded link target. If the file is anything else, the
Variable Length Portion will have zero length.

A List of Extent Structures is:

| Length | Field                                  |
| ------ | -------------------------------------- |
|      8 | Volume                                 |
|      8 | Physical Start Offset                  |
|      8 | Block Size                             |
|      1 | Block Multiplicity                     |
|      8 | Block Count                            |
|      8 | Pre Truncate                           |
|      8 | Post Truncate                          |
|      8 | Logical Start Offset                   |

Each extent structure has a fixed length (57) so the number of extent structures
will be the Variable Length Size divided by 57.

Extents define a logical segment of the file, of length (Block Size) * (Block
Count), starting at Logical Start Offset.

These logical segments are mapped to physical segments on Volume, starting at
Physical Start Offset.

Pre Truncate and Post Truncate are used to modify the logical and physical
segments.

Block Multiplicity is either 82 or 67 (hex for 'R' and 'C' respectively,
meaning Repeat and Count). Repeat extents repeat the block at Physical Start
Offset (Block Count times). Count extents return a number of sequential blocks
(again starting at Physical Start Offset) equal to Block Count.

All values in the extent structure except Block Multiplicity are 64-bit
little-endian unsigned integers.

## BlockLink

A link associates an inode with a parent inode and gives that association a
name.


| Length | Field                                  |
| ------ | -------------------------------------- |
|      1 | Block ID                               |
|      8 | Log timestamp                          |
|      8 | Child Inode                            |
|      8 | Parent Inode                           |
|      2 | Length of link name                    |
|      n | Link name                              |
|      4 | CRC                                    |

Block ID is 2

The Log timestamp records the moment the block was written.

Child Inode is associated with Parent Inode, with the name Name.

Length of link name is a 16-bit little-endian unsigned integer n defining the
length of the name field that follows.

Link name is the UTF-8 encoded name of the link.

## BlockUnlink

An unlink disassociates a named inode from a parent inode. This represents the
deletion of a file.

| Length | Field                                  |
| ------ | -------------------------------------- |
|      1 | Block ID                               |
|      8 | Log Timestamp                          |
|      8 | Child Inode                            |
|      8 | Parent Inode                           |
|      2 | Length of Link Name                    |
|      n | Link Name                              |
|      4 | CRC                                    |

Block ID is 3.

All other fields are the same as in BlockLink.

## BlockXattr

An extended attribute is a name-value pair associated with an inode.

| Length | Field                                  |
| ------ | -------------------------------------- |
|      1 | Block ID                               |
|      8 | Log Timestamp                          |
|      8 | Inode Number                           |
|      1 | Length of Attribute Name               |
|      2 | Length of Attribute Value              |
|      n | Attribute Name                         |
|      m | Attribute Value                        |
|      4 | CRC                                    |


Block ID is 4.

Length of attribute name is an 8-bit unsigned integer n defining the
length of the Attribute Name field.

Length of attribute value is a 16-bit little-endian unsigned integer m defining the
length of the Attribute Value field.

Attribute Name is the UTF-8 encoded name of the attribute.

Attribute Value is a sequence of m raw bytes.

## BlockRemovedXattr

A removed extended attribute represents the deletion of an extended attribute
from an inode.

| Length | Field                                  |
| ------ | -------------------------------------- |
|      1 | Block ID                               |
|      8 | Log Timestamp                          |
|      8 | Inode Number                           |
|      1 | Length of Attribute Name               |
|      n | Attribute Name                         |
|      4 | CRC                                    |


Block ID is 5.

All other fields are the same as in BlockXattr.

## BlockData

This block stores file data.

| Length | Field                                  |
| ------ | -------------------------------------- |
|      1 | Block ID                               |
|      8 | Log Timestamp                          |
|      8 | Payload length                         |
|      n | Payload                                |
|      4 | CRC                                    |

Block ID is 6

Payload length is a 64-bit little-endian unsigned integer n defining the length
of the block of data that follows.

Payload is a sequence of n raw bytes.

## BlockRename

Renaming is an atomic operation, hence is not simply a pair of link/unlink
events.

| Length | Field                                  |
| ------ | -------------------------------------- |
|      1 | Block ID                               |
|      8 | Log Timestamp                          |
|      2 | Length of Previous Path                |
|      2 | Length of New Path                     |
|      n | Previous Path                          |
|      m | New Path                               |
|      4 | CRC                                    |

Block ID is 7

Length of Previous Path is a 16-bit little-endian unsigned integer n defining the
length of the Previous Path field.

Length of New Path is a 16-bit little-endian unsigned integer m defining the
length of the New Path field.

Previous Path and New Path are UTF-8 encoded full path names relative to the
root of the HDRFS filesystem.

## BlockLinkTable

The link table is a representation of the filesystem tree. It maps inodes
to parent inodes, with a name for each relationship. An inode may be mapped to
the same parent inode multiple times under different names. The link table is
included in every volume. Strictly speaking this table is redundant, as all this
information is already recorded in BlockLink blocks. But its purpose is to aid
disaster recovery: with this table, it becomes possible to extract files to
their correct location in the filesystem tree - without needing access to
previous volumes.

| Length | Field                                  |
| ------ | -------------------------------------- |
|      1 | Block ID                               |
|      8 | Number of links                        |
|      n | List of link structures                |
|      4 | CRC                                    |

Block ID is 8

Number of links is a 64-bit little-endian unsigned integer n. It defines the
number of link structures to follow.

The CRC is a 32-bit little-endian unsigned integer as returned from the CRC
function.

List of link structures is:

| Length | Field                                  |
| ------ | -------------------------------------- |
|      8 | Child inode number                     |
|      8 | Parent inode number                    |
|      2 | Length of link name                    |
|      m | Link name                              |


Child and parent inode numbers are 64-bit little-endian unsigned integers
defining an is-in relationship.

Length of link name is a 16-bit little-endian unsigned integer m defining
the length of the name to follow.

Link name is the UTF-8 encoded name of the link.
