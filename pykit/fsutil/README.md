<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
#   Table of Content

- [Name](#name)
- [Status](#status)
- [Description](#description)
- [Synopsis](#synopsis)
- [Exceptions](#exceptions)
  - [NoData](#nodata)
  - [LockTimeout](#locktimeout)
  - [NotMountPoint](#notmountpoint)
  - [NoSuchFile](#nosuchfile)
- [Classes](#classes)
  - [fsutil.Cat](#fsutilcat)
    - [Cat.iterate](#catiterate)
    - [Cat.cat](#catcat)
    - [Cat.stat_path](#catstat_path)
- [Methods](#methods)
  - [fsutil.assert_mountpoint](#fsutilassert_mountpoint)
  - [fsutil.get_all_mountpoint](#fsutilget_all_mountpoint)
  - [fsutil.get_device](#fsutilget_device)
  - [fsutil.get_device_fs](#fsutilget_device_fs)
  - [fsutil.get_disk_partitions](#fsutilget_disk_partitions)
  - [fsutil.get_mountpoint](#fsutilget_mountpoint)
  - [fsutil.get_path_fs](#fsutilget_path_fs)
  - [fsutil.get_path_inode_usage](#fsutilget_path_inode_usage)
  - [fsutil.get_path_usage](#fsutilget_path_usage)
  - [fsutil.makedirs](#fsutilmakedirs)
  - [fsutil.read_file](#fsutilread_file)
  - [fsutil.remove](#fsutilremove)
  - [fsutil.write_file](#fsutilwrite_file)
  - [fsutil.calc_checksums](#fsutilcalc_checksums)
- [Author](#author)
- [Copyright and License](#copyright-and-license)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

#   Name

fsutil

#   Status

This library is considered production ready.

#   Description

File-system Utilities.


# Synopsis

```python
fsutil.get_mountpoint('/bin')
# '/'

fsutil.get_device('/bin')
# '/dev/sdb1'

fsutil.get_disk_partitions()
# {
#  '/': {'device': '/dev/disk1',
#        'fstype': 'hfs',
#        'mountpoint': '/',
#        'opts': 'rw,local,rootfs,dovolfs,journaled,multilabel'},
#  '/dev': {'device': 'devfs',
#           'fstype': 'devfs',
#           'mountpoint': '/dev',
#           'opts': 'rw,local,dontbrowse,multilabel'},
#  ...
# }
```

# Exceptions

## NoData

Raises when there is no data to scan before timeout, when `Cat.cat()` or
`Cat.iterate()`.

## LockTimeout

Raises when there are more than one exclusive `Cat` instances(with the same id)
trying to scan a same file.

## NotMountPoint

Raises when trying to use an invalid mount point path.

## NoSuchFile

Raises when there is no file present before timeout, when `Cat.cat()` or
`Cat.iterate()`.


# Classes

##  fsutil.Cat

**Synopsis**: continuously tail nginx log and print it. If there is no more data
for 1 hour, it quits.


```python
from pykit import fsutil

fn = '/var/log/nginx/access.log'
for l in fsutil.Cat(fn).iterate(timeout=3600):
    print l
```

Just like nix command cat or tail, it continuously scan a file line by line.

It provides with two way for user to handle lines: as a generator or specifying
a handler function.

It also remembers the offset of the last scanning in a file in `/tmp/`.
If a file does not change(inode number does not change), it scans from the last
offset, or it scan from the first byte.


**syntax**:
`Cat(fn, handler=None, file_end_handler=None, exclusive=True, id=None, strip=False)`

**arguments**:

-   `fn`:
    specifies the file to scan.

-   `handler`:
    specifies a callable to handle each line, if `Cat()` is not used in
    generator mode.
    It can be a callable or a list of callable.
    See method `Cat.cat()`.

-   `file_end_handler`:
    specifies a callable when file end reached.
    Every time it scans to end of file, `file_end_handler` is called, but it is still
    able to not quit and to wait for new data for a while.
    Thus `file_end_handler` will be called more than one time.

-   `exclusive`:
    is `True` means at the same time there can be only one same progress
    scanning a same file, which means, `Cat` with the same `id` and the same
    `fn`.
    Two `Cat` instances with different id are able to scan a same file at the
    same time and they record their own offset in separate file.

    By default it is `True`.

-   `id`:
    specifies the instance id.
    `id` is used to identify a `Cat` instance and is used as part of offset
    record file(in `/tmp/`) and is used to exclude other instance.
    See `exclusive`.

    By default `id` is the file name of the currently running python script.
    Thus normally a user does not need to specify `id` explicitly.

-   `strip`:
    is `True` or `False` to specifies if to strip blank chars(space, tab, `\r`
    and `\n`) before returning each line.

    By default it is `False`.

**config**:

-   `cat_stat_dir`:
    specifies base dir to store offset recording file.

    By default it is `/tmp`.

    ```sh
    # cat pykitconfig
    cat_stat_dir = '/'

    # cat usage.py
    from pykit import fsutil
    fn = '/var/log/nginx/access.log'
    for l in fsutil.Cat(fn).iterate(timeout=3600):
        print l
    ```


###  Cat.iterate

Make a generator to yield every line.

**syntax**:
`Cat.iterate(timeout=None)`

**arguments**:

-   `timeout`:
    specifies the time in second to wait for new data.

    If timeout is `0` or smaller than `0`, it means to scan a file no more than one time:

    -   If it sees any data, it returning them until it reaches file end.
    -   If there is not any data, it raises `NoData` error.

    By default it is 3600.

**return**:
a generator.

**raise**:

-   `NoSuchFile`: if file does not present before `timeout`.
-   `NoData`: if file does not have un-scanned data before `timeout`.


###  Cat.cat

Similar to `Cat.iterate` except it blocks until timeout or reaches file end and
let `Cat.handler` to deal with each line.

**syntax**:
`Cat.cat(timeout=None)`

**return**:
Nothing.


###  Cat.stat_path

Returns the full path of the file to store scanning offset.

**syntax**:
`Cat.stat_path()`

**return**:
string


# Methods

##  fsutil.assert_mountpoint

**syntax**:
`fsutil.assert_mountpoint(path)`

Ensure that `path` must be a **mount point**.
Or an error `NotMountPoint` is emitted.

**arguments**:

-   `path`:
    is a path that does have to be an existent file path.

**return**:
Nothing


##  fsutil.get_all_mountpoint

**syntax**:
`fsutil.get_all_mountpoint(all=False)`

Returns a list of all mount points on this host.

**arguments**:

-   `all`:
    specifies if to return non-physical device mount points.

    By default it is `False` thus only disk drive mount points are returned.
    `tmpfs` or `/proc` are not returned by default.

**return**:
a list of mount point path in string.


##  fsutil.get_device

**syntax**:
`fsutil.get_device(path)`

Get the device path(`/dev/sdb` etc) where `path` resides on.

**arguments**:

-   `path`:
    is a path that does have to be an existent file path.

**return**:
device path like `"/dev/sdb"` in string.


##  fsutil.get_device_fs

**syntax**:
`fsutil.get_device_fs(device)`

Return the file-system name of a device, if the device is a disk device.

**arguments**:

-   `device`:
    is a path of a device, such as `/dev/sdb1`.

**return**:
the file-system name, such as `ext4` or `hfs`.


##  fsutil.get_disk_partitions

**syntax**:
`fsutil.get_disk_partitions()`

Find and return all mounted path and its mount point information in a
dictionary.

All mount points including non-disk path are also returned.

**return**:
an dictionary indexed by mount point path:

```python
{
 '/': {'device': '/dev/disk1',
       'fstype': 'hfs',
       'mountpoint': '/',
       'opts': 'rw,local,rootfs,dovolfs,journaled,multilabel'},
 '/dev': {'device': 'devfs',
          'fstype': 'devfs',
          'mountpoint': '/dev',
          'opts': 'rw,local,dontbrowse,multilabel'},
 '/home': {'device': 'map auto_home',
           'fstype': 'autofs',
           'mountpoint': '/home',
           'opts': 'rw,dontbrowse,automounted,multilabel'},
 '/net': {'device': 'map -hosts',
          'fstype': 'autofs',
          'mountpoint': '/net',
          'opts': 'rw,nosuid,dontbrowse,automounted,multilabel'}
}
```


##  fsutil.get_mountpoint

**syntax**:
`fsutil.get_mountpoint(path)`

Return the mount point where this `path` resides on.
All symbolic links are resolved when looking up for mount point.

**arguments**:

-   `path`:
    is a path that does have to be an existent file path.

**return**:
the mount point path(one of output of command `mount` on linux)


##  fsutil.get_path_fs

**syntax**:
`fsutil.get_path_fs(path)`

Return the name of device where the `path` is mounted.

**arguments**:

-   `path`:
    is a file path on a file system.

**return**:
the file-system name, such as `ext4` or `hfs`.


##  fsutil.get_path_inode_usage

**syntax**:
`fsutil.get_path_inode_usage(path)`

Collect inode usage information of the file system `path` is mounted on.

**arguments**:

- `path`:
specifies the fs - path to collect usage info.
Such as `/tmp` or `/home/alice`.

**return**:
a dictionary in the following format:

```json
{
    'total':     total number of inode,
    'used':      used inode(includes inode reserved for super user),
    'available': total - used,
    'percent':   float(used) / 'total'
}
```


##  fsutil.get_path_usage

**syntax**:
`fsutil.get_path_usage(path)`

Collect space usage information of the file system `path` is mounted on.

**arguments**:

-   `path`:
    specifies the fs-path to collect usage info.
    Such as `/tmp` or `/home/alice`.

**return**:
a dictionary in the following format:

```
{
    'total':     total space in byte,
    'used':      used space in byte(includes space reserved for super user),
    'available': total - used,
    'percent':   float(used) / 'total',
}
```

There two concept for unused space: `free` and `available`
because some file systems have a reserved(maybe 5%) for super user like `root`:

- free:      with    blocks reserved for super users.

- available: without blocks reserved for super users.

Since most of the time an application can not run as `root`
then it can not use the reserved space.
Thus this function provides with the `available` bytes by default.


##  fsutil.makedirs

**syntax**:
`fsutil.makedirs(*path, mode=0755, uid=None, gid=None)`

Make directory.
If intermediate directory does not exist, create them too.

**arguments**:

-   `*path`:
    is a single part path such as `/tmp/foo` or a separated path such as
    `('/tmp', 'foo')`.

-   `mode`:
    specifies permission mode for the dir created or existed.

    By defaul it is `0755`.

-   `uid`:
    and `gid` to specify another user/group for the dir to create.

    By default they are `None` and the created dir inherits ownership from the
    running python program.

**return**:
Nothing

**raise**:
`OSError` if trying to create dir with the same path of a non-dir file, or
having other issue like permission denied.


##  fsutil.read_file

**syntax**:
`fsutil.read_file(path)`

Read and return the entire file specified by `path`

**arguments**:

-   `path`:
    is the file path to read.

**return**:
file content in string.


##  fsutil.remove

**syntax**:
`fsutil.remove(path, ignore_errors=False, onerror=None)`

Recursively delete `path`, the `path` is one of *file*, *directory* or *symbolic link*.

**arguments**:

-   `path`:
    is the path to remove.

-   `ignore_errors`:
    whether ignore *os.error* while deleting the `path`.

-   `onerror`:
    If `ignore_errors` is set to `True`, errors(os.error) are ignored;
    otherwise, if `onerror` is set, it is called to handle the error with
    arguments `(func, path, exc_info)` where func is *os.listdir*,
    *os.remove*, *os.rmdir* or *os.path.isdir*.

**return**:
Nothing


##  fsutil.write_file

**syntax**:
`fsutil.write_file(path, content, uid=None, gid=None, atomic=False)`

Write `content` to file `path`.

**arguments**:

-   `path`:
    is the file path to write to.

-   `content`:
    specifies the content to write.

-   `uid` and `gid`:
     specifies the user_id/group_id the file belongs to.

     Bedefault they are `None`, which means the file that has been written
     inheirts ownership of the running python script.

-   `atomic`:
    atomically write content to the path.

    Write content to a temporary file, then rename to the path.
    The temporary file names of same path in one process distinguish with
    `timeutil.ns()`, it is not atomic if the temporary files of same path
    created at the same nanosecond.
    The renaming will be an atomic operation (this is a POSIX requirement).

**return**:
Nothing


##  fsutil.calc_checksums

**syntax**:
`fsutil.calc_checksums(path, sha1=False, md5=False, crc32=False, sha256=False,
                   block_size=READ_BLOCK, io_limit=READ_BLOCK):`

Calculate checksums of `path`, like: `sha1` `md5` `crc32`.

```python
from pykit import fsutil

file_name = 'test.file'

fsutil.write_file(file_name, '')
print fsutil.calc_checksums(file_name, sha1=True, md5=True, crc32=False, sha256=True)
#{
# 'sha1': 'da39a3ee5e6b4b0d3255bfef95601890afd80709',
# 'md5': 'd41d8cd98f00b204e9800998ecf8427e',
# 'crc32': None,
# 'sha256':'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
#}
```

**arguments**:

-   `path`:
    is the file path to calculate.

-   `sha1` and `md5` and `crc32` and `sha256`:
    are checksum types to calculate. Default is `False`.

    The result of this type is `None` if the checksum type is `False`.

-   `block_size`:
     is the buffer size while reading content of `path`.

-   `io_limit`:
    is the IO limitation per second while reading content of `path`.

    There is no limitation if `io_limit` is negative number.

**return**:
a dict with keys `sha1` and `md5` and `crc32` and `sha256`.


#   Author

Zhang Yanpo (张炎泼) <drdr.xp@gmail.com>

#   Copyright and License

The MIT License (MIT)

Copyright (c) 2015 Zhang Yanpo (张炎泼) <drdr.xp@gmail.com>
