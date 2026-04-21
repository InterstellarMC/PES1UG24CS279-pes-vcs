// tree.c — Tree object serialization and construction
#include "tree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>

// Forward declarations
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

// ─── Mode Constants ─────────────────────────────────────────────────────────

#define MODE_FILE      0100644
#define MODE_EXEC      0100755
#define MODE_DIR       0040000

// ─── PROVIDED ───────────────────────────────────────────────────────────────

uint32_t get_file_mode(const char *path) {
    struct stat st;
    if (lstat(path, &st) != 0) return 0;
    if (S_ISDIR(st.st_mode))  return MODE_DIR;
    if (st.st_mode & S_IXUSR) return MODE_EXEC;
    return MODE_FILE;
}

int tree_parse(const void *data, size_t len, Tree *tree_out) {
    tree_out->count = 0;
    const uint8_t *ptr = (const uint8_t *)data;
    const uint8_t *end = ptr + len;

    while (ptr < end && tree_out->count < MAX_TREE_ENTRIES) {
        TreeEntry *entry = &tree_out->entries[tree_out->count];

        const uint8_t *space = memchr(ptr, ' ', end - ptr);
        if (!space) return -1;

        char mode_str[16] = {0};
        size_t mode_len = space - ptr;
        if (mode_len >= sizeof(mode_str)) return -1;
        memcpy(mode_str, ptr, mode_len);
        entry->mode = strtol(mode_str, NULL, 8);

        ptr = space + 1;

        const uint8_t *null_byte = memchr(ptr, '\0', end - ptr);
        if (!null_byte) return -1;

        size_t name_len = null_byte - ptr;
        if (name_len >= sizeof(entry->name)) return -1;
        memcpy(entry->name, ptr, name_len);
        entry->name[name_len] = '\0';

        ptr = null_byte + 1;

        if (ptr + HASH_SIZE > end) return -1;
        memcpy(entry->hash.hash, ptr, HASH_SIZE);
        ptr += HASH_SIZE;

        tree_out->count++;
    }
    return 0;
}

static int compare_tree_entries(const void *a, const void *b) {
    return strcmp(((const TreeEntry *)a)->name, ((const TreeEntry *)b)->name);
}

int tree_serialize(const Tree *tree, void **data_out, size_t *len_out) {
    size_t max_size = tree->count * 296;
    uint8_t *buffer = malloc(max_size);
    if (!buffer) return -1;

    Tree sorted_tree = *tree;
    qsort(sorted_tree.entries, sorted_tree.count, sizeof(TreeEntry), compare_tree_entries);

    size_t offset = 0;
    for (int i = 0; i < sorted_tree.count; i++) {
        const TreeEntry *entry = &sorted_tree.entries[i];
        int written = sprintf((char *)buffer + offset, "%o %s", entry->mode, entry->name);
        offset += written + 1;
        memcpy(buffer + offset, entry->hash.hash, HASH_SIZE);
        offset += HASH_SIZE;
    }

    *data_out = buffer;
    *len_out = offset;
    return 0;
}

// ─── TODO: Implement ────────────────────────────────────────────────────────

int tree_from_index(ObjectID *id_out) {
    FILE *f = fopen(INDEX_FILE, "r");
    if (!f) return -1;

    char lines[10000][600];
    int count = 0;
    while (fgets(lines[count], sizeof(lines[count]), f)) {
        lines[count][strcspn(lines[count], "\n")] = '\0';
        if (strlen(lines[count]) > 0) count++;
    }
    fclose(f);

    if (count == 0) return -1;

    Tree root;
    root.count = 0;

    for (int i = 0; i < count; i++) {
        uint32_t mode;
        char hex[65];
        unsigned long mtime;
        uint32_t size;
        char path[512];

        if (sscanf(lines[i], "%o %64s %lu %u %511s",
                   &mode, hex, &mtime, &size, path) != 5)
            continue;

        ObjectID hash;
        hex_to_hash(hex, &hash);

        char *slash = strchr(path, '/');
        if (!slash) {
            // Top-level file
            TreeEntry *e = &root.entries[root.count++];
            e->mode = mode;
            e->hash = hash;
            strncpy(e->name, path, sizeof(e->name) - 1);
            e->name[sizeof(e->name) - 1] = '\0';
        } else {
            // Get directory name e.g. "src" from "src/main.c"
            char dirname[256];
            size_t dlen = slash - path;
            strncpy(dirname, path, dlen);
            dirname[dlen] = '\0';

            // Skip if we already processed this directory
            int already = 0;
            for (int r = 0; r < root.count; r++) {
                if (strcmp(root.entries[r].name, dirname) == 0) {
                    already = 1;
                    break;
                }
            }
            if (already) continue;

            // Collect all files under this directory
            Tree subtree;
            subtree.count = 0;
            for (int j = 0; j < count; j++) {
                uint32_t m2;
                char h2[65], p2[512];
                unsigned long mt2;
                uint32_t s2;
                if (sscanf(lines[j], "%o %64s %lu %u %511s",
                           &m2, h2, &mt2, &s2, p2) != 5)
                    continue;
                if (strncmp(p2, dirname, dlen) == 0 && p2[dlen] == '/') {
                    ObjectID h2id;
                    hex_to_hash(h2, &h2id);
                    TreeEntry *e = &subtree.entries[subtree.count++];
                    e->mode = m2;
                    e->hash = h2id;
                    strncpy(e->name, p2 + dlen + 1, sizeof(e->name) - 1);
                    e->name[sizeof(e->name) - 1] = '\0';
                }
            }

            // Write subtree object
            void *tdata;
            size_t tlen;
            if (tree_serialize(&subtree, &tdata, &tlen) != 0) return -1;
            ObjectID subtree_id;
            if (object_write(OBJ_TREE, tdata, tlen, &subtree_id) != 0) {
                free(tdata);
                return -1;
            }
            free(tdata);

            // Add directory entry to root
            TreeEntry *e = &root.entries[root.count++];
            e->mode = 0040000;
            e->hash = subtree_id;
            strncpy(e->name, dirname, sizeof(e->name) - 1);
            e->name[sizeof(e->name) - 1] = '\0';
        }
    }

    // Write root tree object
    void *data;
    size_t dlen;
    if (tree_serialize(&root, &data, &dlen) != 0) return -1;
    if (object_write(OBJ_TREE, data, dlen, id_out) != 0) {
        free(data);
        return -1;
    }
    free(data);
    return 0;
}

