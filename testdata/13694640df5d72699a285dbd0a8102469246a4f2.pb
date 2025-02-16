
packagename�
�

size	      9@
I

maintainer;9Ubuntu Developers <ubuntu-devel-discuss@lists.ubuntu.com>
�	
description�	�	Zerofree finds the unallocated blocks with non-zero value content in
an ext2, ext3 or ext4 file-system and fills them with zeroes
(zerofree can also work with another value than zero). This is mostly
useful if the device on which this file-system resides is a disk
image. In this case, depending on the type of disk image, a secondary
utility may be able to reduce the size of the disk image after
zerofree has been run. Zerofree requires the file-system to be
unmounted or mounted read-only.
.
The usual way to achieve the same result (zeroing the unused
blocks) is to run "dd" to create a file full of zeroes that takes up
the entire free space on the drive, and then delete this file. This
has many disadvantages, which zerofree alleviates:
* it is slow;
* it makes the disk image (temporarily) grow to its maximal extent;
* it (temporarily) uses all free space on the disk, so other
concurrent write actions may fail.
.
Zerofree has been written to be run from GNU/Linux systems installed
as guest OSes inside a virtual machine. If this is not your case, you
almost certainly don't need this package. (One other use case would
be to erase sensitive data a little bit more securely than with a
simple "rm").


name
zerofree

sectionadmin

status	installed

version	1.1.1-1
C
summary86zero free blocks from ext2, ext3 and ext4 file-systems

priority
optional

architectureamd64
E
url>*<

Schemehttps

Hostfrippery.org

Path/uml/"�dpkg.package
package �**�2return.item._INBOX.YUAfGqdDedSMbQa81pc5fB.arSO7yDu�6return.response._INBOX.YUAfGqdDedSMbQa81pc5fB.6vfJQYy1"��������*�ެ62��
:dpkg*ubuntu2004.localdomain