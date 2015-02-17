/* Copyright (c) 2015 Tim Henderson
 * Release under the GNU General Public License version 3.
 *
 * fs2 is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 3 as
 * published by the Free Software Foundation.
 *
 * fs2 is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * for more details.
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
create_mmap(void **addr, int fd) {
	size_t length;
	int err = fd_size(fd, &length);
	if (err != 0) {
		return err;
	}
	void * mapped = mmap(
		NULL, // address hint
		length,
		PROT_READ | PROT_WRITE, // protection flags (rw)
		MAP_SHARED | MAP_POPULATE, // writes reflect in the file,
		                           // prepopulate the tlb
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
	int ret = msync(addr, length, MS_ASYNC | MS_INVALIDATE);
	if (ret != 0) {
		int err = errno;
		errno = 0;
		char *msg = strerror(err);
		fprintf(stderr, "MSYNC ERROR: %s\n", msg);
		return err;
	}
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
	void *mapped = mremap(
		old_addr,
		old_length,
		new_length,
		MREMAP_MAYMOVE
	);
	if (mapped == MAP_FAILED) {
		int err = errno;
		errno = 0;
		char *msg = strerror(err);
		fprintf(stderr, "MREMAP ERROR: %s\n", msg);
		return err;
	}
	*new_addr = mapped;
	return 0;
}

int
is_sequential(void *addr, size_t offset, size_t length) {
	return do_madvise(MADV_SEQUENTIAL, addr, offset, length);
}

int
is_normal(void *addr, size_t offset, size_t length) {
	return do_madvise(MADV_NORMAL, addr, offset, length);
}

int
do_madvise(int flag, void *addr, size_t offset, size_t length) {
	void *start = (void *)((size_t)(addr) + offset);
	int ret = madvise(start, length, flag);
	if (ret != 0) {
		int err = errno;
		errno = 0;
		char *msg = strerror(err);
		fprintf(stderr, "MADVISE ERROR: %s\n", msg);
		return err;
	}
	return 0;
}

