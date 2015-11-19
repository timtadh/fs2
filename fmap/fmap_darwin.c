//+build darwin
/* Copyright (c) 2015 Tim Henderson
 * Release under the GNU General Public License version 3.
 *
 * fs2 is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 3 as
 * published by the Free Software Foundation.
 *
 * fs2 is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
 * License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with goiso.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "fmap.h"

void
memclr(void *addr, size_t size) {
	memset(addr, 0, size);
	return;
}

int
create_anon_mmap(void **addr, size_t length) {
	void * mapped = NULL;
	mapped = mmap(
		NULL, // address hint
		length,
		PROT_READ | PROT_WRITE, // protection flags (rw)
		MAP_ANON | MAP_PRIVATE, // anon map
		-1, // the fd
		0 // the offset into the file
	);
	if (mapped == MAP_FAILED) {
		int err = errno;
		errno = 0;
		char *msg = strerror(err);
		fprintf(stderr, "MMAP ERROR: %s\n", msg);
		fprintf(stderr, "length = %d\n", (int)length);
		return err;
	}
	*addr = mapped;
	return 0;
}

int
create_mmap(void **addr, int fd) {
	size_t length;
	int err = fd_size(fd, &length);
	if (err != 0) {
		return err;
	}
	void * mapped = NULL;
	mapped = mmap(
		NULL, // address hint
		length,
		PROT_READ | PROT_WRITE, // protection flags (rw)
		MAP_SHARED, // writes reflect in the file,
		fd,
		0 // the offset into the file
	);
	if (mapped == MAP_FAILED) {
		int err = errno;
		errno = 0;
		char *msg = strerror(err);
		fprintf(stderr, "MMAP ERROR: %s\n", msg);
		fprintf(stderr, "fd = %d\n", (int)fd);
		fprintf(stderr, "length = %d\n", (int)length);
		return err;
	}
	*addr = mapped;
	return 0;
}

int
destroy_anon_mmap(void *addr, size_t length) {
	int ret = munmap(addr, length);
	if (ret != 0) {
		int err = errno;
		errno = 0;
		char *msg = strerror(err);
		fprintf(stderr, "MUNMAP ERROR: %s\n", msg);
		return err;
	}
	return 0;
}

int
destroy_mmap(void *addr, int fd) {
	size_t length;
	int err = fd_size(fd, &length);
	if (err != 0) {
		return err;
	}
	int ret = munmap(addr, length);
	if (ret != 0) {
		int err = errno;
		errno = 0;
		char *msg = strerror(err);
		fprintf(stderr, "MUNMAP ERROR: %s\n", msg);
		return err;
	}
	return 0;
}

int
sync_mmap(void *addr, int fd) {
	size_t length;
	int err = fd_size(fd, &length);
	if (err != 0) {
		return err;
	}
	int ret = msync(addr, length, MS_SYNC);
	if (ret != 0) {
		int err = errno;
		errno = 0;
		char *msg = strerror(err);
		fprintf(stderr, "MSYNC ERROR: %s\n", msg);
		return err;
	}
	return 0;
}

// this seems impossible with darwin :-( ....
// not sure how to do it as their is no mremap ...
// possible idea:
// 1. try to extend the mapping by creating a new mapping at the end of this
//     mapping. It would be convient if only one munmap call to munmap was
//     required to unmap the whole region. If not a linked list of mmaps is
//     going to need to be created such that munmap can unlink them. If this
//     succeeds the map has been "resized" and it is ok to return 0 with the
//     new_addr set to the old_addr.
// 2. If 1 fails, then mmap a whole new region of the full size. If this fails
//    the whole call fails.
// 3. Copy the contents of the old map to the new with memcpy.
// 4. Unmap the old region
// 5. Return the new map in new_addr.
int
anon_resize(void *old_addr, void **new_addr, size_t old_length, size_t
		new_length) {
	if (new_length <= old_length) {
		*new_addr = old_addr;
		return 0;
	}
	void *extension = mmap(
		old_addr + old_length, // start at end of previous mapping
		(new_length - old_length), // just add the new length
		PROT_READ | PROT_WRITE, // read write
		MAP_ANON | MAP_PRIVATE | MAP_FIXED, // anon map, at this locatation
		-1, // no fd
		0 // no offset
	);
	if (extension != MAP_FAILED) {
		*new_addr = old_addr;
		return 0;
	}
	// else we are going to go with a fall back of a new address space plus a
	// memcopy
	void *mapped = mmap(
		NULL,
		new_length,
		PROT_READ | PROT_WRITE, // read write
		MAP_ANON | MAP_PRIVATE, // anon map
		-1, // no fd
		0 // no offset
	);
	if (mapped == MAP_FAILED) {
		int err = errno;
		errno = 0;
		char *msg = strerror(err);
		fprintf(stderr, "MREMAP ERROR: %s\n", msg);
		return err;
	}
	memcpy(mapped, old_addr, old_length);
	int err = munmap(old_addr, old_length);
	if (err != 0) {
		int err = errno;
		errno = 0;
		char *msg = strerror(err);
		fprintf(stderr, "MUNMAP ERROR: %s\n", msg);
		return err;
	}
	*new_addr = mapped;
	return 0;
}

int
fd_size(int fd, size_t *size) {
	struct stat fdstat;
	int ret = fstat(fd, &fdstat);
	if (ret != 0) {
		int err = errno;
		errno = 0;
		char *msg = strerror(err);
		fprintf(stderr, "FSTAT ERROR: %s\n", msg);
		return err;
	}
	*size = (size_t)(fdstat.st_size);
	return 0;
}

int
fd_resize(int fd, size_t new_length) {
	int ret = ftruncate(fd, (off_t)new_length);
	if (ret != 0) {
		int err = errno;
		errno = 0;
		char *msg = strerror(err);
		fprintf(stderr, "FTRUNCATE ERROR: %s\n", msg);
		return err;
	}
	return 0;
}

// This needs to be changed to a munmap and mmap call. Pain in the neck.
int
resize(void *old_addr, void **new_addr, int fd, size_t new_length) {
	size_t old_length;
	int err = fd_size(fd, &old_length);
	if (err != 0) {
		return err;
	}
	err = fd_resize(fd, new_length);
	if (err != 0) {
		return err;
	}
	int ret = munmap(old_addr, old_length);
	if (ret != 0) {
		int err = errno;
		errno = 0;
		char *msg = strerror(err);
		fprintf(stderr, "MUNMAP ERROR: %s\n", msg);
		return err;
	}
	return create_mmap(new_addr, fd);
}

