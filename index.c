// index.c — Staging area implementation
//
// Text format of .pes/index (one entry per line, sorted by path):
//
//   <mode-octal> <64-char-hex-hash> <mtime-seconds> <size> <path>
//
// Example:
//   100644 a1b2c3d4e5f6...  1699900000 42 README.md
//   100644 f7e8d9c0b1a2...  1699900100 128 src/main.c
//
// This is intentionally a simple text format. No magic numbers, no
// binary parsing. The focus is on the staging area CONCEPT (tracking
// what will go into the next commit) and ATOMIC WRITES (temp+rename).
//
// PROVIDED functions: index_find, index_remove, index_status
// TODO functions:     index_load, index_save, index_add

#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

/* Forward decl: no object.h in template. */
extern int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

// ─── PROVIDED ────────────────────────────────────────────────────────────────

// Find an index entry by path (linear scan).
IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

// Remove a file from the index.
// Returns 0 on success, -1 if path not in index.
int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            int remaining = index->count - i - 1;
            if (remaining > 0)
                memmove(&index->entries[i], &index->entries[i + 1],
                        remaining * sizeof(IndexEntry));
            index->count--;
            return index_save(index);
        }
    }
    fprintf(stderr, "error: '%s' is not in the index\n", path);
    return -1;
}

// Print the status of the working directory.
//
// Identifies files that are staged, unstaged (modified/deleted in working dir),
// and untracked (present in working dir but not in index).
// Returns 0.
int index_status(const Index *index) {
    printf("Staged changes:\n");
    int staged_count = 0;
    // Note: A true Git implementation deeply diffs against the HEAD tree here. 
    // For this lab, displaying indexed files represents the staging intent.
    for (int i = 0; i < index->count; i++) {
        printf("  staged:     %s\n", index->entries[i].path);
        staged_count++;
    }
    if (staged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Unstaged changes:\n");
    int unstaged_count = 0;
    for (int i = 0; i < index->count; i++) {
        struct stat st;
        if (stat(index->entries[i].path, &st) != 0) {
            printf("  deleted:    %s\n", index->entries[i].path);
            unstaged_count++;
        } else {
            // Fast diff: check metadata instead of re-hashing file content
            if (st.st_mtime != (time_t)index->entries[i].mtime_sec || st.st_size != (off_t)index->entries[i].size) {
                printf("  modified:   %s\n", index->entries[i].path);
                unstaged_count++;
            }
        }
    }
    if (unstaged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Untracked files:\n");
    int untracked_count = 0;
    DIR *dir = opendir(".");
    if (dir) {
        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {
            // Skip hidden directories, parent directories, and build artifacts
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
            if (strcmp(ent->d_name, ".pes") == 0) continue;
            if (strcmp(ent->d_name, "pes") == 0) continue; // compiled executable
            if (strstr(ent->d_name, ".o") != NULL) continue; // object files

            // Check if file is tracked in the index
            int is_tracked = 0;
            for (int i = 0; i < index->count; i++) {
                if (strcmp(index->entries[i].path, ent->d_name) == 0) {
                    is_tracked = 1; 
                    break;
                }
            }
            
            if (!is_tracked) {
                struct stat st;
                stat(ent->d_name, &st);
                if (S_ISREG(st.st_mode)) { // Only list regular files for simplicity
                    printf("  untracked:  %s\n", ent->d_name);
                    untracked_count++;
                }
            }
        }
        closedir(dir);
    }
    if (untracked_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    return 0;
}

// ─── TODO: Implement these ───────────────────────────────────────────────────

// Load the index from .pes/index.
//
// HINTS - Useful functions:
//   - fopen (with "r"), fscanf, fclose : reading the text file line by line
//   - hex_to_hash                      : converting the parsed string to ObjectID
//
// Returns 0 on success, -1 on error.
int index_load(Index *index) {
    index->count = 0;

    FILE *f = fopen(INDEX_FILE, "r");
    if (!f) return 0;  // No file yet → empty index, not an error.

    char line[1024];
    while (fgets(line, sizeof(line), f)) {
        if (index->count >= MAX_INDEX_ENTRIES) { fclose(f); return -1; }

        IndexEntry *e = &index->entries[index->count];
        char hex[HASH_HEX_SIZE + 1];
        unsigned int mode;
        unsigned long long mtime;
        unsigned int size;
        int consumed = 0;

        // Parse the four fixed-width fields; %n captures the offset so we can
        // copy the remainder (the path, which may contain spaces) verbatim.
        if (sscanf(line, "%o %64s %llu %u %n",
                   &mode, hex, &mtime, &size, &consumed) < 4 || consumed == 0) {
            fclose(f);
            return -1;
        }

        const char *path = line + consumed;
        size_t path_len = strlen(path);
        while (path_len > 0 && (path[path_len - 1] == '\n' || path[path_len - 1] == '\r')) {
            path_len--;
        }
        if (path_len == 0 || path_len >= sizeof(e->path)) { fclose(f); return -1; }

        if (hex_to_hash(hex, &e->hash) != 0) { fclose(f); return -1; }

        e->mode = mode;
        e->mtime_sec = mtime;
        e->size = size;
        memcpy(e->path, path, path_len);
        e->path[path_len] = '\0';

        index->count++;
    }

    fclose(f);
    return 0;
}

// Save the index to .pes/index atomically.
//
// HINTS - Useful functions and syscalls:
//   - qsort                            : sorting the entries array by path
//   - fopen (with "w"), fprintf        : writing to the temporary file
//   - hash_to_hex                      : converting ObjectID for text output
//   - fflush, fileno, fsync, fclose    : flushing userspace buffers and syncing to disk
//   - rename                           : atomically moving the temp file over the old index
//
// Returns 0 on success, -1 on error.
// Comparator: sort IndexEntry array alphabetically by path so the on-disk
// index file is deterministic regardless of add order.
static int compare_index_entries_by_path(const void *a, const void *b) {
    return strcmp(((const IndexEntry *)a)->path, ((const IndexEntry *)b)->path);
}

int index_save(const Index *index) {
    // Heap-allocate a sort buffer sized to the live entries only. Copying
    // the whole Index (which contains a MAX_INDEX_ENTRIES-sized inline
    // array, ~5.4 MB) to the stack would overflow on top of the caller's
    // own Index local.
    IndexEntry *sorted = NULL;
    if (index->count > 0) {
        sorted = malloc((size_t)index->count * sizeof(IndexEntry));
        if (!sorted) return -1;
        memcpy(sorted, index->entries, (size_t)index->count * sizeof(IndexEntry));
        qsort(sorted, index->count, sizeof(IndexEntry),
              compare_index_entries_by_path);
    }

    const char *tmp = INDEX_FILE ".tmp";
    FILE *f = fopen(tmp, "w");
    if (!f) { free(sorted); return -1; }

    for (int i = 0; i < index->count; i++) {
        const IndexEntry *e = &sorted[i];
        char hex[HASH_HEX_SIZE + 1];
        hash_to_hex(&e->hash, hex);
        if (fprintf(f, "%o %s %llu %u %s\n",
                    (unsigned)e->mode, hex,
                    (unsigned long long)e->mtime_sec,
                    (unsigned)e->size, e->path) < 0) {
            fclose(f); unlink(tmp); free(sorted); return -1;
        }
    }

    // Flush userspace buffers, then fsync to force the bytes to disk before
    // renaming — without this, a crash between rename and writeback could
    // leave the index empty or truncated.
    if (fflush(f) != 0)           { fclose(f); unlink(tmp); free(sorted); return -1; }
    if (fsync(fileno(f)) != 0)    { fclose(f); unlink(tmp); free(sorted); return -1; }
    fclose(f);
    free(sorted);

    if (rename(tmp, INDEX_FILE) != 0) { unlink(tmp); return -1; }
    return 0;
}

// Stage a file for the next commit.
//
// HINTS - Useful functions and syscalls:
//   - fopen, fread, fclose             : reading the target file's contents
//   - object_write                     : saving the contents as OBJ_BLOB
//   - stat / lstat                     : getting file metadata (size, mtime, mode)
//   - index_find                       : checking if the file is already staged
//
// Returns 0 on success, -1 on error.
int index_add(Index *index, const char *path) {
    // Read the working-tree file into memory.
    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return -1; }
    long raw_len = ftell(f);
    if (raw_len < 0)                { fclose(f); return -1; }
    rewind(f);

    size_t len = (size_t)raw_len;
    void *data = malloc(len ? len : 1);
    if (!data)                      { fclose(f); return -1; }
    if (len > 0 && fread(data, 1, len, f) != len) {
        fclose(f); free(data); return -1;
    }
    fclose(f);

    // Persist as a blob in the object store; the returned ObjectID is what
    // the index entry will reference.
    ObjectID blob_id;
    int rc = object_write(OBJ_BLOB, data, len, &blob_id);
    free(data);
    if (rc != 0) return -1;

    // Metadata: exec-bit sets mode to 100755 (vs 100644), mtime and size are
    // snapshotted so index_status can fast-path unchanged-file detection.
    struct stat st;
    if (stat(path, &st) != 0) return -1;
    uint32_t mode       = (st.st_mode & S_IXUSR) ? 0100755 : 0100644;
    uint64_t mtime_sec  = (uint64_t)st.st_mtime;
    uint32_t size       = (uint32_t)st.st_size;

    // Upsert: overwrite every field on an existing entry so re-staging a
    // file refreshes its hash+mtime+size, otherwise append.
    IndexEntry *e = index_find(index, path);
    if (!e) {
        if (index->count >= MAX_INDEX_ENTRIES) return -1;
        e = &index->entries[index->count++];
    }
    e->mode      = mode;
    e->hash      = blob_id;
    e->mtime_sec = mtime_sec;
    e->size      = size;
    snprintf(e->path, sizeof(e->path), "%s", path);

    return index_save(index);
}
