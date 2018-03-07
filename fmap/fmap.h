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

#define _GNU_SOURCE
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/* memclr(*addr, size)
 *
 * clears the memory starting at addr, for size.
 *
 */
void memclr(void *addr, size_t size);

/* create_anon_mmap(*addr, length)
 *
 * creates a mmap using the length. this map will be anonymous
 *
 * (*addr) is a pointer to the address pointer. This is an out
 * paramter.  The output will be placed in this pointer.
 *
 * (length) is the file descriptor to map. It must be valid as this
 * creates
 *
 * (returns) 0 on success and an errno value of failure.
 */
int create_anon_mmap(void **addr, size_t fd);

/* create_mmap(*addr, fd)
 *
 * creates a mmap using the file descriptor
 *
 * (*addr) is a pointer to the address pointer. This is an out
 * paramter.  The output will be placed in this pointer.
 *
 * (fd) is the file descriptor to map. It must be valid as this
 * creates a shared mapping to an actual file.
 *
 * (returns) 0 on success and an errno value of failure.
 */
int create_mmap(void **addr, int fd);

/* destroy_anon_map(addr, length)
 *
 * destroys the mapping. Caution: subsequent access will cause a
 * SIGSEGV
 *
 * (addr) the address of the mapping.
 *
 *
 * (returns) 0 on success and an errno value on failure.
 */
int destroy_anon_mmap(void *addr, size_t length);

/* destroy_map(addr, length)
 *
 * destroys the mapping. Caution: subsequent access will cause a
 * SIGSEGV
 *
 * (addr) the address of the mapping.
 *
 * (length) is the file descriptor to map. It must be valid as this creates
 *
 * (returns) 0 on success and an errno value on failure.
 */
int destroy_mmap(void *addr, int fd);

/* sync_map(addr, length)
 *
 * syncs any changes down to disk. This function is NON-BLOCKING. It
 * uses MS_ASYNC flags to msync such that this program will not block
 * on the sync. It also uses MS_INVALIDATE to invalidate any other
 * mappings.
 *
 * (addr) the address of the mapping.
 *
 * (fd) is the file descriptor for this map.
 *
 * (returns) 0 on success and an errno value on failure.
 */
int sync_mmap(void *addr, int fd);

/* resize(addr, new_addr, fd, new_length)
 *
 * this resizes both file and the mapping to the new length.
 *
 * (old_addr) the address of the mapping.
 *
 * (*new_addr) is a pointer to the address pointer. This is an out
 * paramter.  This function may move the mapping. When that happens
 * the new_address will be at this pointer. You should only call this
 * if you can know there are no outstanding pointers into the memory
 * of the mapping.
 *
 * (old_length) the old size of the mapping
 *
 * (new_length) the new size of the mapping
 *
 * (returns) 0 on success and an errno value on failure.
 */
int anon_resize(void *old_addr, void **new_addr, size_t old_length,
                size_t new_length);

/* resize(addr, new_addr, fd, new_length)
 *
 * this resizes both file and the mapping to the new length.
 *
 * (old_addr) the address of the mapping.
 *
 * (*new_addr) is a pointer to the address pointer. This is an out
 * paramter.  This function may move the mapping. When that happens
 * the new_address will be at this pointer. You should only call this
 * if you can know there are no outstanding pointers into the memory
 * of the mapping.
 *
 * (fd) is the file descriptor for this map.
 *
 * (new_length) the new size of the file/mapping
 *
 * (returns) 0 on success and an errno value on failure.
 */
int resize(void *old_addr, void **new_addr, int fd, size_t new_length);

/* is_sequential(addr, offset, length)
 *
 * mark the address + offset, for length as a sequential segment.
 *
 * (addr) the address of the mapping
 *
 * (offset) the start of the sequential region
 *
 * (length) the length of the sequential region
 *
 * (returns) 0 on success and an errno value on failure
 */
int is_sequential(void *addr, size_t offset, size_t length);

/* is_normal(addr, offset, length)
 *
 * mark the address + offset, for length as a normal segment.
 *
 * (addr) the address of the mapping
 *
 * (offset) the start of the sequential region
 *
 * (length) the length of the sequential region
 *
 * (returns) 0 on success and an errno value on failure
 */
int is_normal(void *addr, size_t offset, size_t length);

/* fd_size(fd, size)
 *
 * finds the size of the file backing the file descriptor.
 *
 * (fd) the file descriptor
 *
 * (size) the output variable. an int pointer where the size will be written
 *
 * (returns) 0 on success and an errno value on failure
 */
int fd_size(int fd, size_t *size);

/* do_madvise(flag, *addr, offset, length)
 *
 * mark the address + offset, with the given madvise flag
 *
 * (flag) the flag to apply to the range
 *
 * (addr) the address of the mapping
 *
 * (offset) the start of the sequential region
 *
 * (length) the length of the sequential region
 *
 * (returns) 0 on success and an errno value on failure
 */
int do_madvise(int flag, void *addr, size_t offset, size_t length);
