#include "userfs.h"
#include <stddef.h>
#include <string.h>
#include <stdlib.h>

enum {
	BLOCK_SIZE = 8192, // 512 -> 8192 fix for heap_help
	MAX_FILE_SIZE = 1024 * 1024 * 100,
	BASE_FILE_DESCRIPTOR_CAPACITY = 4,
	BASE_FILE_DESCRIPTOR_MULTIPLICATOR = 2,
};

/** Global error code. Set from any function on any error. */
static enum ufs_error_code ufs_error_code = UFS_ERR_NO_ERR;

struct block {
	/** Block memory. */
	char* memory;
	/** How many bytes are occupied. */
	size_t occupied;
	/** Next block in the file. */
	struct block* next;
	/** Previous block in the file. */
	struct block* prev;
};

struct file {
	/** Double-linked list of file blocks. */
	struct block* block_list;
	/**
	 * Last block in the list above for fast access to the end
	 * of file.
	 */
	struct block* last_block;
	/** How many file descriptors are opened on the file. */
	int refs;
	/** File name. */
	char* name;
	/** Files are stored in a double-linked list. */
	struct file* next;
	struct file* prev;

	size_t offset;
	size_t size;
};

/** List of all files. */
static struct file* file_list = NULL;

struct filedesc {
	struct file* file;

	size_t offset;
	int flags;
};

/**
 * An array of file descriptors. When a file descriptor is
 * created, its pointer drops here. When a file descriptor is
 * closed, its place in this array is set to NULL and can be
 * taken by next ufs_open() call.
 */
static struct filedesc** file_descriptors = NULL;
static int file_descriptor_count = 0;
static int file_descriptor_capacity = 0;

enum ufs_error_code
	ufs_errno()
{
	return ufs_error_code;
}

int ufs_open(const char* filename, int flags) {
	struct file* f = file_list;
	while (f) {
		if (strcmp(f->name, filename) == 0) {
			break;
		}
		f = f->next;
	}

	if (!f && (flags & UFS_CREATE)) {
		f = malloc(sizeof(*f));
		if (!f) {
			ufs_error_code = UFS_ERR_NO_MEM;
			return -1;
		}
		memset(f, 0, sizeof(*f));
		f->name = strdup(filename);
		f->next = file_list;
		if (file_list) file_list->prev = f;
		file_list = f;
	}

	if (!f) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}

	if (file_descriptor_count == file_descriptor_capacity) {
		int new_capacity = file_descriptor_capacity == 0 ?
			BASE_FILE_DESCRIPTOR_CAPACITY : file_descriptor_capacity * BASE_FILE_DESCRIPTOR_MULTIPLICATOR;

		struct filedesc** new_fd_table = realloc(file_descriptors, new_capacity * sizeof(*new_fd_table));
		if (!new_fd_table) {
			ufs_error_code = UFS_ERR_NO_MEM;
			return -1;
		}

		file_descriptors = new_fd_table;
		file_descriptor_capacity = new_capacity;
	}

	struct filedesc* fd = malloc(sizeof(*fd));
	if (!fd) {
		ufs_error_code = UFS_ERR_NO_MEM;
		return -1;
	}

	fd->flags = flags;
	fd->file = f;
	fd->offset = 0;
	f->refs++;

	file_descriptors[file_descriptor_count++] = fd;
	return file_descriptor_count - 1;
}

ssize_t ufs_write(int fd, const char* buf, size_t size) {
	if (fd < 0 || fd >= file_descriptor_count || !file_descriptors[fd]) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}

	struct filedesc* fd_struct = file_descriptors[fd];
	if (fd_struct->flags == UFS_READ_ONLY) {
		ufs_error_code = UFS_ERR_NO_PERMISSION;
		return -1;
	}

	struct file* f = fd_struct->file;
	size_t offset = fd_struct->offset;
	if (offset + size > f->size) {
		if (offset + size > MAX_FILE_SIZE) {
			ufs_error_code = UFS_ERR_NO_MEM;
			return -1;
		}
	}

	size_t bytes_written = 0;
	size_t remaining = size;
	struct block* b = f->block_list;
	size_t local_offset = offset;

	while (b && local_offset >= (size_t)b->occupied) {
		local_offset -= b->occupied;
		b = b->next;
	}

	if (!b && f->last_block) {
		b = f->last_block;
		local_offset = b->occupied;
	}

	while (remaining > 0) {
		if (!b) {
			b = malloc(sizeof(struct block));
			if (!b) {
				ufs_error_code = UFS_ERR_NO_MEM;
				return -1;
			}

			b->memory = malloc(BLOCK_SIZE);
			if (!b->memory) {
				free(b);
				ufs_error_code = UFS_ERR_NO_MEM;
				return -1;
			}

			b->occupied = 0;
			b->next = NULL;
			b->prev = f->last_block;

			if (f->last_block) {
				f->last_block->next = b;
			} else {
				f->block_list = b;
			}

			f->last_block = b;
		}

		size_t to_write = (BLOCK_SIZE - local_offset < remaining) ? BLOCK_SIZE - local_offset : remaining;
		memcpy(b->memory + local_offset, buf + bytes_written, to_write);

		if (local_offset + to_write > (size_t)b->occupied) {
			b->occupied = local_offset + to_write;
		}

		bytes_written += to_write;
		remaining -= to_write;
		local_offset = 0;

		if (!b->next && remaining > 0) {
			struct block* new_block = malloc(sizeof(struct block));
			if (!new_block) {
				ufs_error_code = UFS_ERR_NO_MEM;
				return -1;
			}

			new_block->memory = malloc(BLOCK_SIZE);
			if (!new_block->memory) {
				free(new_block);
				ufs_error_code = UFS_ERR_NO_MEM;
				return -1;
			}

			new_block->occupied = 0;
			new_block->next = NULL;
			new_block->prev = b;
			b->next = new_block;
			f->last_block = new_block;
		}

		b = b->next;
	}

	fd_struct->offset += bytes_written;
	if (fd_struct->offset > f->size) {
		f->size = fd_struct->offset;
	}

	return bytes_written;
}


ssize_t ufs_read(int fd, char* buf, size_t size) {
	if (fd < 0 || fd >= file_descriptor_count || !file_descriptors[fd]) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}

	struct filedesc* fd_struct = file_descriptors[fd];
	if (fd_struct->flags == UFS_WRITE_ONLY) {
		ufs_error_code = UFS_ERR_NO_PERMISSION;
		return -1;
	}

	struct file* f = fd_struct->file;
	size_t offset = fd_struct->offset;
	if (offset >= f->size) {
		return 0;
	}

	size_t bytes_read = 0;
	struct block* b = f->block_list;
	size_t local_offset = offset;

	while (b && local_offset >= (size_t)b->occupied) {
		local_offset -= b->occupied;
		b = b->next;
	}

	while (b && bytes_read < size) {
		size_t to_read = (b->occupied - local_offset < size - bytes_read) ? b->occupied - local_offset : size - bytes_read;
		memcpy(buf + bytes_read, b->memory + local_offset, to_read);
		bytes_read += to_read;
		local_offset = 0;
		b = b->next;
	}

	fd_struct->offset += bytes_read;
	return bytes_read;
}


int ufs_close(int fd) {
	if (fd < 0 || fd >= file_descriptor_count || !file_descriptors[fd]) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}

	free(file_descriptors[fd]);
	file_descriptors[fd] = NULL;
	return 0;
}

int ufs_delete(const char* filename) {
	struct file* f = file_list;
	while (f) {
		if (strcmp(f->name, filename) == 0) {
			if (f->prev) {
				f->prev->next = f->next;
			} else {
				file_list = f->next;
			}

			if (f->next) {
				f->next->prev = f->prev;
			}

			struct block* b = f->block_list;
			while (b) {
				struct block* next = b->next;
				free(b->memory);
				free(b);
				b = next;
			}
			free(f->name);
			free(f);
			return 0;
		}
		f = f->next;
	}

	ufs_error_code = UFS_ERR_NO_FILE;
	return -1;
}

#if NEED_RESIZE

int ufs_resize(int fd, size_t new_size) {
	if (fd < 0 || fd >= file_descriptor_count || !file_descriptors[fd]) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}

	struct filedesc* fd_struct = file_descriptors[fd];
	struct file* f = fd_struct->file;

	if (new_size > MAX_FILE_SIZE) {
		ufs_error_code = UFS_ERR_NO_MEM;
		return -1;
	}

	if (new_size < f->size) {
		size_t remaining = new_size;
		struct block* b = f->block_list;

		while (b && remaining > 0) {
			if ((size_t)b->occupied > remaining) {
				b->occupied = remaining;
				break;
			}
			remaining -= b->occupied;
			b = b->next;
		}

		struct block* to_delete = b ? b->next : f->block_list;
		while (to_delete) {
			struct block* next = to_delete->next;
			free(to_delete->memory);
			free(to_delete);
			to_delete = next;
		}

		if (b) {
			b->next = NULL;
		} else {
			f->block_list = NULL;
		}

		f->last_block = b;
	} else if (new_size > f->size) {
		size_t to_allocate = new_size - f->size;
		while (to_allocate > 0) {
			struct block* new_block = malloc(sizeof(struct block));
			if (!new_block) {
				ufs_error_code = UFS_ERR_NO_MEM;
				return -1;
			}
			new_block->memory = malloc(BLOCK_SIZE);
			if (!new_block->memory) {
				free(new_block);
				ufs_error_code = UFS_ERR_NO_MEM;
				return -1;
			}
			new_block->occupied = 0;
			new_block->next = NULL;
			new_block->prev = f->last_block;

			if (f->last_block) {
				f->last_block->next = new_block;
			} else {
				f->block_list = new_block;
			}

			f->last_block = new_block;
			to_allocate -= BLOCK_SIZE;
		}
	}

	f->size = new_size;
	return 0;
}


#endif

void ufs_destroy(void) {
	while (file_list) {
		struct file* next_file = file_list->next;
		ufs_delete(file_list->name);
		file_list = next_file;
	}

	for (int i = 0; i < file_descriptor_count; i++) {
		if (file_descriptors[i]) {
			free(file_descriptors[i]);
		}
	}

	free(file_descriptors);
	file_descriptors = NULL;
	file_descriptor_count = 0;
	file_descriptor_capacity = 0;
}
