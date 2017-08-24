*Please note HDRFS is in its initial testing phase. Do not rely on it for storing critical data.*

# What is HDRFS?

HDRFS is a lossless filesystem application which stores a complete history of
every byte ever written to it. It is backed by a strictly append-only log, but
works as a fully read/write POSIX-compatible filesystem. Think of it as a cross
between a filesystem and `tar`, with infinite versioning and tuned to maximise
ease of backups.

It is intended to be used by individuals to archive personal files.

This repository contains a specification for the log file format, and a
reference implementation in Python.

HDRFS has the following properties:

* The filesystem is backed by one or more volume files which exist on an
 underlying filesystem. You may tune the size of the volume files to whatever
 size you find easiest to manage. For example, you might choose a volume size
 of 4.7GB in order to write each to a DVD.

* The volume files are *strictly* append-only. Once written, data is never
 overwritten or deleted. Users may still modify or delete data in the HDRFS
 filesystem, but modifications and deletions are recorded as additional events
 and it is always possible to wind back to a previous state of the filesystem.

* HDRFS may be mounted with some or all of its volume files missing (e.g. if
 they have been moved to offline media). Data cannot be read from missing
 volumes, but the filesystem can still be browsed. Providing the most recent
 volume is not missing, the filesystem also supports writes. You can even write
 to a file which is held on offline media, as long as you don't try to read
 from it.

* Backing up an HDRFS volume is very easy: only the most recent volume ever
 changes. Once a volume reaches its maximum size it becomes immutable, and the
 next volume is started.

* HDRFS presents a virtual directory through which to browse historical
 snapshots. Full history of every write is generated at fsync-level
 granularity. There is no need to create snapshots - simply browse to whatever
 timestamp you wish.

* HDRFS deduplicates data by default, though it can be turned off for a small
 performance improvement.

* HDRFS checksums all data and metadata to help identify integrity issues.

# Installation and quick start

HDRFS is a single Python program, with one dependency (fusepy) which is
available in Ubuntu 17.04 and above:

    # apt install python3-fusepy

Alternatively, you can get fusepy from here:
https://github.com/terencehonles/fusepy. If you don't want to install fusepy,
you can just download `fuse.py` save it as `fusepy.py` in the same directory as
`hdrfs.py`.

Once fusepy is installed, download `hdrfs.py` and launch it like this:

    $ ./hdrfs.py /path/to/mount/point

By default, it will create Log and Index files in the current directory. This
can be changed using options. View all options as follows:

    $ ./hdrfs.py --help

# Non-features

* HDRFS does not compress data. If you wish to compress your data, use an
 underlying filesystem which supports compression (such as btrfs), or compress
 the volume files manually (this will take them offline - you must uncompress
 them before HDRFS will be able to read from them).

* HDRFS does not encrypt data. If you wish to encrypt your data, use an
  underlying filesystem which
  supports encryption, or take the volume files offline and encrypt them using
  another tool. If you have a strong need for data security, you should consult a
  security professional.

* HDRFS does not store parity data. Thanks to the use of checksums,
  data corruption in an HDRFS volume can be detected - but it cannot be fixed.
  Instead, HDRFS relies on its underlying filesystem to provide RAID-like
  features.

* HDRFS does not enforce permissions checks. Permission bits, owners, and groups
 are recorded faithfully, but not used to grant or deny access.This means you
 can chown or chmod a file to whatever you like. This non-feature is consistent
 with HDRFS being a single-user personal filesystem.  HDRFS makes no attempt to
 support locking.

* HDRFS is not a high-performance multi-user filesystem. Although you *could*
 run a database server on it, performance may be problematic. Any application
 which writes and overwrites lots of transient data is likely to generate
 unmanageably large and useless Log files.

# How does a filesystem help make backups easier?

HDRFS's primary benefit is its volume management. It is very simple to manage a
small number of large files which never change. You can take backups and layer
on compression, encryption and erasure coding, and because the volumes never
change, you only have to do this once. If you go to the effort of dividing your
data into n erasure-coded blocks hosted on n different cloud storage providers,
you don't want to have to repeat that when you do your next backup.

# How does HDRFS operate with offline volumes?

HDRFS maintains an index holding filesystem metadata. This allows you to mount
the filesystem without *any* data volumes being available. You can also mount
the filesystem with only *some* volumes available, for example with some
transferred to tape and taken offline. As long as the most recent volume is
available, you can even write to the filesystem. One possible use case for this
might be an archive which needs to hold large files indefinitely, reorganise
them and add metadata over time, but rarely access the actual data in the files.

The index is not part of the HDRFS specification, and you don't strictly need to
back it up: if it's missing at startup, HDRFS will regenerate it. You might
still want to back it up because the regeneration process can be slow, and
requires all volumes to be online.

# How do I use the history feature?

By default, HDRFS will create a directory at the root its filesystem named
`history`. You can change the name of this top-level folder if you wish, using
the `--history-directory` option. Within the history directory there are folders
corresponding to change events. By default, there will be a folder for every day
on which data was written, plus folders representing the first and last writes.
The first folder will always be empty. You can use the
`--history-directory-granularity` option to change the granularity of these
directories - anything from year to microsecond.

The format of the directory name is ISO8601 *including* timezone.

Regardless of the granularity option, you can manually navigate to any timestamp
of your choosing.

History view will always give you file data as-at an fsync.

You can get some useful metadata about the change history of a file by examining
the `hdrfs.history` extended attribute. For example:

    $ getfattr --only-values -n hdrfs.history bigfile.zip
    Change time         | Mode     | UID   | GID   | Size (bytes)
    --------------------|----------|-------|-------|-------------
    2017-07-30 20:22:42 | 0o100644 | 1000  | 1000  | 0
    2017-07-30 20:22:43 | 0o100644 | 1000  | 1000  | 107644039

Another useful extended attribute is `hdrfs.volumes`, which shows you a
comma-separated list of the volumes that a file resides on:

    $ getfattr --only-values -n hdrfs.volumes bigfile.zip
    0,1,2,3

You could use this to ensure that all volumes are online before attempting to
access a file.

# How fast is HDRFS?

HDRFS is extremely sensitive to file block size. It performs best when writes
are in large blocks. `cp` will use a default block size of 128KiB, which is good
for HDRFS performance. Other utilities may use a smaller block size such as 4KiB,
which is not good for HDRFS performance.

Worst case performance is reached when writing many small files using a small
block size.

Best case performance is reached when writing large files using a large block
size.

An HDRFS filesystem loaded with media files is capable of achieving decent
performance - certainly enough to stream high bitrate video and perform interactive file
management without the end-user experience being all that different from a
native filesystem.

As well as block size, there is overhead to creating volumes. For good
performance, choose a large volume size that will fill up only occasionally.

Example benchmark showing best case performance:

    $ dd if=/dev/zero count=8000 bs=131072 of=mnt/1
    8000+0 records in
    8000+0 records out
    1048576000 bytes (1.0 GB, 1000 MiB) copied, 5.6616 s, 185 MB/s

Example benchmark showing far worse performance:

    $ dd if=/dev/zero count=256000 bs=4096 of=mnt/2
    256000+0 records in
    256000+0 records out
    1048576000 bytes (1.0 GB, 1000 MiB) copied, 25.028 s, 41.9 MB/s

HDRFS attempts to counteract the effect of small writes by buffering into 128KiB
chunks. This means that slow 4K writes won't compromise future reads:

    $ dd if=mnt/2 of=/dev/null bs=131072
    8000+0 records in
    8000+0 records out
    1048576000 bytes (1.0 GB, 1000 MiB) copied, 2.262 s, 464 MB/s

As a further benchmark, here is `postmark` running with default settings on an HDRFS
filesystem:

    Creating files...Done
    Performing transactions..........Done
    Deleting files...Done
    Time:
        14 seconds total
        6 seconds of transactions (83 per second)

    Files:
        764 created (54 per second)
            Creation alone: 500 files (100 per second)
            Mixed with transactions: 264 files (44 per second)
        243 read (40 per second)
        257 appended (42 per second)
        764 deleted (54 per second)
            Deletion alone: 528 files (176 per second)
            Mixed with transactions: 236 files (39 per second)

    Data:
        1.36 megabytes read (99.80 kilobytes per second)
        4.45 megabytes written (325.20 kilobytes per second)

And for comparison here it is running on the same hardware on an ext4
filesystem:

    Creating files...Done
    Performing transactions..........Done
    Deleting files...Done
    Time:
        1 seconds total
        1 seconds of transactions (500 per second)

    Files:
        764 created (764 per second)
            Creation alone: 500 files (500 per second)
            Mixed with transactions: 264 files (264 per second)
        243 read (243 per second)
        257 appended (257 per second)
        764 deleted (764 per second)
            Deletion alone: 528 files (528 per second)
            Mixed with transactions: 236 files (236 per second)

    Data:
        1.36 megabytes read (1.36 megabytes per second)
        4.45 megabytes written (4.45 megabytes per second)

You can see HDRFS is an order of magnitude slower than ext4. However, this is
with file size set to range between 500 and 10000 bytes, with read and write
block sizes of 512 bytes. HDRFS put in a stronger performance with file size set
to range between 1,000,000 and 10,000,000 bytes, with read and write block sizes
of 131,072 bytes. With these settings we get the following results:

    Creating files...Done
    Performing transactions..........Done
    Deleting files...Done
    Time:
        51 seconds total
        26 seconds of transactions (19 per second)

    Files:
        757 created (14 per second)
            Creation alone: 500 files (22 per second)
            Mixed with transactions: 257 files (9 per second)
        285 read (10 per second)
        215 appended (8 per second)
        757 deleted (14 per second)
            Deletion alone: 514 files (171 per second)
            Mixed with transactions: 243 files (9 per second)

    Data:
        1619.88 megabytes read (31.76 megabytes per second)
        4365.11 megabytes written (85.59 megabytes per second)

And for comparison on ext4:

    Creating files...Done
    Performing transactions..........Done
    Deleting files...Done
    Time:
        24 seconds total
        16 seconds of transactions (31 per second)

    Files:
        757 created (31 per second)
            Creation alone: 500 files (62 per second)
            Mixed with transactions: 257 files (16 per second)
        285 read (17 per second)
        215 appended (13 per second)
        757 deleted (31 per second)
            Deletion alone: 514 files (514 per second)
            Mixed with transactions: 243 files (15 per second)

    Data:
        1619.88 megabytes read (67.49 megabytes per second)
        4365.11 megabytes written (181.88 megabytes per second)

Now HDRFS is only about half as slow as ext4. You still wouldn't want to run a
mail server on an HDRFS filesystem.

By the way, you *can* run an HDRFS filesystem within another HDRFS filesystem if
you wish to plumb the depths of awful performance.


# How slow is HDRFS?

HDRFS is written in Python, and uses FUSE. This adds several layers of
abstraction over a native filesystem such as btrfs.

Normal filesystem: Disk <-> Filesystem Driver <-> Kernel <-> User

HDRFS filesystem: Disk <-> Filesystem Driver <-> Kernel <-> FUSE Kernel Module
<-> fusepy.py <-> hdrfs.py <-> fusepy.py <-> FUSE Kernel Module <-> Kernel <->
User

Greater performance could undoubtedly be achieved by rewriting HDRFS in a faster
language such as C or Go - and in fact, the log file format is designed with C
data types in mind - but even an assembly implementation would have to contend
with all this layering and will never be as fast as a native filesystem.

Fortunately, modern PCs have powerful CPUs, and for single-user purposes, the
performance of even the Python implementation can be solidly adequate.

# How big should my volumes be?

Choose whatever size is convenient for you to manage. 

* If you backup to DVDs, you could choose 4.7G
* If you backup to triple-layer BDXL discs, you could choose 100G.
* If you backup to cloud storage, you could tune the volume size to your upload
  speed, so that a volume uploads in a reasonable time.

Every time a volume is filled, it is hashed and the hash stored in the header of
the next volume. This forms a chain of integrity checks, but does mean that each
volume must be hashed in full - potentially in the middle of a write operation.
This manifests as a pause, and the large the volume the longer the pause.

Each volume can be a different size if you wish. The `--volume-size` option only
takes effect at the moment a volume reaches that size, so if you started HDRFS
with a volume size of 100P and meant to type 100G, don't worry - just restart
HDRFS with the desired volume size.

However, you cannot change the size of a volume which has already been
finalized.

The default volume size is 1G.

# Why does HDRFS not support compression?

It's a personal judgement call based on four reasons - one aesthetic, three
pragmatic.

1. I believe compression is more effective elsewhere in the storage stack. The
purpose of HDRFS is to create a canonical logical representation of your
versioned data. Compression is a physical layer under that.  It is better to
have checksums of your data than of a compressed representation of your data.

2. Filesystem compression is a solved problem: if you use btrfs or ZFS as the
underlying filesystem for your HDRFS volumes, you can enable transparent
compression very easily. No need for HDRFS to reimplement that.

3. Compression ratios are better over large ranges of data, not small filesystem
blocks. To get the most out of an algorithm like lrzip, you should compress the
whole HDRFS volume. To take advantage of long range inter-block redundancies, it
is essential that the data is not already compressed.

2. HDRFS is a singler-user personal filesystem, and the vast majority of
personal files are already compressed. JPG, PNG, MOV, MP4, AVI, MP3, FLAC, OGG,
GIF, ZIP, DOCX, XLSX, PPTX, and PDF are amongst the most common file types and
all of them use their own compression. When your storage system is scaled to
store 30GB Blu-Ray rips, it makes little difference whether your textfiles are
compressed or not.

# Is HDRFS multi-threaded?

No. Writes are single-threaded by design and by necessity, as there is only a
single Log. The Python implementation is single-threaded.

If you are doing a large transfer of data into HDRFS, you should wait for that
to complete before doing reads (whether reading specific files or just browsing
directories). There's nothing to stop you reading and writing at the same time.
It will just be slow.

An alternative implementation could choose to do multi-threaded reads, but the
Python implementation will never do this.

# Can HDRFS store any type of file?

Yes, but there are some patterns of data which will enjoy worst-case
performance. Testing has shown that large files with alternating patterns of
repeating blocks and non-repeating blocks will generate a lot of metadata, and
therefore be slow to access. These types of files are atypical in normal usage.

# At what level of granularity does deduplication operate?

Under normal usage, and with reasonably large files, data will be deduplicated
in 128KiB blocks. However, HDRFS uses a variable block size so there are
scenarios where data may be deduplicated in smaller blocks.

HDRFS has an internal write buffer which batches POSIX write() calls into blocks
of up to 128KiB. A number of events can cause the write buffer to be flushed
before it reaches this size. Examples include: any metadata operation (e.g.
`chmod`), any attempt to read data from the buffered inode, or upon a `flush` or
`fsync` call. This means a data block may be less than 128KiB. In degenerate
cases, it could be as small as one byte!

# How much memory does deduplication require?

A deduplication index is stored in memory during normal operation, and saved when
HDRFS exits. Deduplicating 1GB of random data requires approximately 1.4MB of
memory. When saved following HDRFS exit, this same deduplication index requires
approximately 1MB of disk space.

You can turn off deduplication using the `--no-deduplication` option. This does
not delete the deduplication index already stored on disk, and only takes effect from the
point of starting HDRFS with this option. You can restart HDRFS with
deduplication turned back on (this is the default) to resume deduplicating data,
but any data stored while deduplication was off will not have generated entries
in the deduplication index.

# What operating systems will HDRFS run on?

HDRFS uses FUSE, which is a feature of the Linux kernel. Other operating systems
have similar features, but HDRFS has been developed and tested on Linux and I
have no immediate plans to port it.

# How do I delete data from an HDRFS filesystem?

You may delete or overwrite files within the filesystem, but the history of
those files remains. There is no option to permanently delete data which has
been written to the Log.

HDRFS is not designed for people who need to delete data. Take care not to write
confidential data to HDRFS if you think you might ever need to delete it.

However, if you have written data which must truly be destroyed, your options
are:

* Copy all data out of one HDRFS filesystem and into another - minus the unwanted file.
    * You would lose all history.
* Delete the volume on which the unwanted data is stored.
    * HDRFS will function normally with a volume missing, but you will not be
      able to read any files on that volume, nor rebuild the index without
      that volume.
* Encrypt the data on which the unwanted data is stored.
    * This takes the volume offline as far as HDRFS is concerned, but allows
      you the possibility of restoring the volume at a later date if you
      needed to do an Index rebuild.
    * You could use this option to mitigate the downsides of deleting the volume:
        * Start HDRFS with deduplication disabled.
        * Locate all files stored on the same volume as the unwanted file.
        * Copy each of these files to a temporary directory within the HDRFS
          filesystem. With deduplication disabled, this will created a new
          copy of each file on the current volume.
        * Restart HDRFS with deduplication enabled. Copy the files back to
          their desired location within the HDRFS filesystem.  Encrypt the 
          volume with the unwanted file.
        * The result is that all required files are available in an online
          volume, except the unwanted file which exists only on the encrypted
          volume. The downside is that history of those files is unavailable.
          
        * Parse the Log file(s) to locate the unwanted data and overwrite it
          with zeros.
            * N.B. This would cause checksums to fail.
            * N.B. This would still leave a zero-filled hole the same size as your data, so if even the length of your data is private, this is not an option.
* Parse and rewrite all Log files to omit the unwanted files.

A 3rd-party tool or alternative HDRFS implementation could be created to
automate some or all of the above, as the Log files are written in a fully
documented format and can be modified independently of any particular
implementation of HDRFS.

# What happens if I remove volumes while the filesystem is mounted?

It's not recommended.

If you remove the volume currently being written to, you risk corrupting the
filesystem. If you remove an older volume, you won't corrupt anything, but you
may get I/O errors if you try to read from a file stored on that volume.

If you remove volumes before starting HDRFS, then reading a file stored on the
removed volume will at least generate a meaningful error message printed on the
terminal where you started HDRFS.

Removing old volumes does *not* remove the ability to write to files - even to
files stored on the removed volume.

# How POSIX compliant is HDRFS?

Enough for normal everyday use.

Files may be opened, closed, truncated, appended, and written to at arbitrary
offsets with arbitrary lengths of data. Reads follow writes with strong
consistency.

Sparse files, symlinks, hard links, FIFOs and extended attributes are supported.

`st_btime` is supported in HDRFS's own internal data structures, but FUSE
currently lacks the ability to implement the statx system call, so there is no
way of viewing it. (Although you can query the Index file, which is in sqlite3
format).

HDRFS falls short of full POSIX compliance in the following areas.

* Permissions are recorded but never enforced
* No locking
* atime is supported but off by default. Turn it on using the `--atime` option.


# Can I use the Index file for any other purposes?

Yes, though I wouldn't recommend doing so while the filesystem is mounted. The
Index file is in sqlite3 format. You can browse it with `sqlitebrowser` and run
SQL queries against it. If you like SQL better than `find`, you might find this
useful for doing analytics on your filesystem.

The schema is documented in the `hdrfs.py` source code.

# Comparisons

In this section I will explain the limitations of similar filesystems as
compared to HDRFS. This is not to say that HDRFS is superior to any of these
filesystems, simply that it offers features which are a subset of no single
other filesystem.

## How is this different from LTFS?

I've never used LTFS and only found out about it while researching similar
filesystems for this README. It does appear to have a lot of similarities with LTFS. Both append changed data to a linear
bytestream rather than overwrite it. But HDRFS makes a guarantee that the Log is
immutable, whereas LTFS's tapes are mostly-immutable, and only due to the
constraints of the underlying technology.

My research indicates the following LTFS limitations, though I would happily be
corrected by an LTFS expert:

* If you change one byte of a large file, LTFS will rewrite the whole file, as
  opposed to HDRFS which will just write the one changed byte.

* LTFS does not support spanning a filesystem across multiple volumes.

* LTFS can only be used in conjunction with tape hardware.

## How is this different from NILFS?

My research indicates the following NILFS limitations, though I would happily be
corrected by a NILFS expert:

* NILFS has no volume management features.
* NILFS takes a *generally* log-structured approach, but will infact delete or
  overwrite data over time, as checkpoints expire without being converted to
  snapshots. 

## How is this different from btrfs and ZFS?

Those two are superficially similar to each other, and radically different from
HDRFS. Both have far higher performance than HDRFS. Both offer volume
management, but neither use immutable data structures, meaning they do not offer
HDRFS's primary benefit of easy backups, nor its secondary benefit of preserving
fine-grained change history of data and metadata.

Either btrfs or ZFS would be a fine complement to HDRFS, as the underlying
filesystem.

# What do HDRFS version numbers mean?

HDRFS is a file format specification, and a reference implementation of a FUSE
filesystem that uses that file format.

The file format is currently version 1.

The Python implementation has a version number of the form `X.Y.Z TAG`, where X
is the file format version, Y is the implementation major version, Z is the
implementation minor version, and TAG is an optional string describing the
release.

For example, HDRFS 1.0.0 RC1 processes Log files in version 1 of the file
format, is the first version of the software that does that, and is the first
release candidate of that version.

Major implementation versions add features. Minor implementation versions fix
bugs. The file format version will only change if the file format changes.

The reference implementation of HDRFS is guaranteed to read and write versions
of the file format less than or equal to its file format version number,
although it may only create new volumes in the latest file format.

# What are HDRFS's limitations

* All pathnames, filenames, and extended attribute names must be in UTF-8 encoding.
* 2^64 maximum number of files
* 2^64 maximum file size (approximately 18 Exabytes)
* 2^64 maximum number of volumes
* 2^64 maximum volume size
