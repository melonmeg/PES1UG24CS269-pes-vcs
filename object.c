// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).
//
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions:     object_write, object_read

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/sha.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    SHA256((const unsigned char *)data, len, id_out->hash);
}

void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── TODO ────────────────────────────────────────────────────────────────────

int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    // Resolve the type string that prefixes the header.
    const char *type_str;
    if (type == OBJ_BLOB)        type_str = "blob";
    else if (type == OBJ_TREE)   type_str = "tree";
    else if (type == OBJ_COMMIT) type_str = "commit";
    else return -1;

    // Build "<type> <size>\0" — header_len includes the terminating NUL.
    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len) + 1;

    // Concatenate header and data into a single buffer so the hash covers both.
    size_t full_len = (size_t)header_len + len;
    uint8_t *full = malloc(full_len);
    if (!full) return -1;
    memcpy(full, header, header_len);
    memcpy(full + header_len, data, len);

    // Content-addressed name: SHA-256 over the full object bytes.
    compute_hash(full, full_len, id_out);

    // Deduplication: same content → same hash → object already on disk.
    if (object_exists(id_out)) {
        free(full);
        return 0;
    }

    // Derive the shard directory (first two hex chars) and the final path.
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id_out, hex);

    char dir[64];
    snprintf(dir, sizeof(dir), "%s/%.2s", OBJECTS_DIR, hex);
    mkdir(dir, 0755);

    char path[128];
    snprintf(path, sizeof(path), "%s/%s", dir, hex + 2);

    // Atomic write: tmp file → fsync(data) → rename → fsync(dir).
    char tmp_path[160];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", path);

    int fd = open(tmp_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) { free(full); return -1; }
    if (write(fd, full, full_len) != (ssize_t)full_len) {
        close(fd); unlink(tmp_path); free(full); return -1;
    }
    if (fsync(fd) != 0) { close(fd); unlink(tmp_path); free(full); return -1; }
    close(fd);
    free(full);

    if (rename(tmp_path, path) != 0) { unlink(tmp_path); return -1; }

    int dir_fd = open(dir, O_RDONLY);
    if (dir_fd >= 0) { fsync(dir_fd); close(dir_fd); }

    return 0;
}

int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    // Resolve the on-disk path from the hash.
    char path[128];
    object_path(id, path, sizeof(path));

    // Slurp the whole file into memory.
    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return -1; }
    long size = ftell(f);
    if (size < 0) { fclose(f); return -1; }
    rewind(f);

    size_t full_len = (size_t)size;
    uint8_t *full = malloc(full_len);
    if (!full) { fclose(f); return -1; }
    if (fread(full, 1, full_len, f) != full_len) {
        fclose(f); free(full); return -1;
    }
    fclose(f);

    // Integrity check: recompute SHA-256 over the raw bytes and confirm it
    // matches the hash the caller asked for. A mismatch means corruption
    // on disk (or an attacker who swapped the file) — fail closed.
    ObjectID computed;
    compute_hash(full, full_len, &computed);
    if (memcmp(computed.hash, id->hash, HASH_SIZE) != 0) {
        free(full);
        return -1;
    }

    // Header terminator: the first NUL separates "<type> <size>" from the payload.
    uint8_t *nul = memchr(full, '\0', full_len);
    if (!nul) { free(full); return -1; }

    if (strncmp((char *)full, "blob ", 5) == 0)        *type_out = OBJ_BLOB;
    else if (strncmp((char *)full, "tree ", 5) == 0)   *type_out = OBJ_TREE;
    else if (strncmp((char *)full, "commit ", 7) == 0) *type_out = OBJ_COMMIT;
    else { free(full); return -1; }

    size_t header_len = (size_t)(nul - full) + 1;
    *len_out = full_len - header_len;

    // Over-allocate by 1 and null-terminate: PROVIDED commit_parse/tree_parse
    // use sscanf %s which requires NUL termination beyond *len_out.
    *data_out = malloc(*len_out + 1);
    if (!*data_out) { free(full); return -1; }
    memcpy(*data_out, full + header_len, *len_out);
    ((uint8_t *)*data_out)[*len_out] = '\0';

    free(full);
    return 0;
}

