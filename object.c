// object.c — Content-addressable object store
#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>

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
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
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

// ─── IMPLEMENT ───────────────────────────────────────────────────────────────

int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    const char *type_str = (type == OBJ_BLOB) ? "blob" :
                           (type == OBJ_TREE) ? "tree" : "commit";

    // Build header: "blob 16\0"
    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len) + 1;

    // Combine header + data
    size_t total = header_len + len;
    uint8_t *full = malloc(total);
    if (!full) return -1;
    memcpy(full, header, header_len);
    memcpy(full + header_len, data, len);

    // Compute SHA-256 of full object
    compute_hash(full, total, id_out);

    // Deduplication: already stored?
    if (object_exists(id_out)) { free(full); return 0; }

    // Get hex and build shard dir path
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id_out, hex);

    char dir[512];
    snprintf(dir, sizeof(dir), "%s/%.2s", OBJECTS_DIR, hex);
    mkdir(dir, 0755);

    // Final object path
    char path[512];
    object_path(id_out, path, sizeof(path));

    // Write to temp file in same directory
    char tmp[600];
    snprintf(tmp, sizeof(tmp), "%s/tmp_XXXXXX", dir);
    int fd = mkstemp(tmp);
    if (fd < 0) { free(full); return -1; }

    if (write(fd, full, total) != (ssize_t)total) {
        close(fd); free(full); return -1;
    }

    fsync(fd);
    close(fd);
    free(full);

    // Atomic rename temp -> final
    if (rename(tmp, path) != 0) return -1;

    // fsync the shard directory
    int dfd = open(dir, O_RDONLY);
    if (dfd >= 0) { fsync(dfd); close(dfd); }

    return 0;
}

int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    char path[512];
    object_path(id, path, sizeof(path));

    // Read entire file into memory
    FILE *f = fopen(path, "rb");
    if (!f) return -1;
    fseek(f, 0, SEEK_END);
    size_t total = ftell(f);
    rewind(f);
    uint8_t *buf = malloc(total);
    if (!buf) { fclose(f); return -1; }
    if (fread(buf, 1, total, f) != total) {
        free(buf); fclose(f); return -1;
    }
    fclose(f);

    // Verify integrity: recompute hash and compare
    ObjectID check;
    compute_hash(buf, total, &check);
    if (memcmp(check.hash, id->hash, HASH_SIZE) != 0) { free(buf); return -1; }

    // Find '\0' separating header from data
    uint8_t *null_pos = memchr(buf, '\0', total);
    if (!null_pos) { free(buf); return -1; }

    // Parse type from header
    if      (strncmp((char*)buf, "blob",   4) == 0) *type_out = OBJ_BLOB;
    else if (strncmp((char*)buf, "tree",   4) == 0) *type_out = OBJ_TREE;
    else if (strncmp((char*)buf, "commit", 6) == 0) *type_out = OBJ_COMMIT;
    else { free(buf); return -1; }

    // Copy data portion (after the '\0')
    size_t data_offset = null_pos - buf + 1;
    *len_out = total - data_offset;
    *data_out = malloc(*len_out + 1);
    if (!*data_out) { free(buf); return -1; }
    memcpy(*data_out, buf + data_offset, *len_out);
    ((char *)*data_out)[*len_out] = '\0';
    free(buf);
    return 0;
}
