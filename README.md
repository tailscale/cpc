# cpc

A copy tool for incremental copies of large files, such as databases.

It's like `rsync --inplace` but a bit faster.

It uses a thread per core and doesn't do writes of 4KB pages that are unchanged.

## Problem statement

We had two filesystems on a machine and some large SQLite databases and other
files on one filesystem that we wanted to move to the other filesystem. More
specifically: two ext4 filesystems on separate AWS EBS block devices, both
attached to the same Linux 64-core VM with lots of memory (larger than the data
to be copied).

To minimize service disruption, we wanted to do a live inconsistent copy of the
data to get most of it over, then stop the service, then do another quick
increment copy, then start it up again.

This tool let us do the migration with minimal downtime.

