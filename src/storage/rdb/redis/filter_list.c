#ifndef __REDIS_FILTER_LIST_H
#define __REDIS_FILTER_LIST_H

#include <time.h>

#include "redis.h"

static int _filter_node_cmp(struct filterNode* node, int8_t type, int len,
        char* buff) {
    if (node == NULL && (len == 0 || buff == NULL)) {
        return 0;
    }
    if (node == NULL || len == 0 || buff == NULL) {
        return 1;
    }
    if (node->type != type || node->len != len) {
        return 1;
    }

    return memcmp(node->buff, buff, len);
}

static struct filterNode* _create_filter_node(int8_t type, int len,
        char* buff) {
    if (buff == NULL || len == 0) {
        return NULL;
    }
    struct filterNode* fn = zmalloc(sizeof(struct filterNode) + len);
    if (fn == NULL) {
        return NULL;
    }

    fn->type      = type;
    fn->timestamp = time(NULL);
    fn->len       = len;
    fn->next      = NULL;
    memcpy(fn->buff, buff, len);

    return fn;
}

static void _free_filter_node(struct filterNode* node) {
    zfree(node);
}

/* ================================= public =================================== */
struct filterList* create_filter_list() {
    struct filterList* list = zmalloc(sizeof(struct filterList));
    if (list == NULL) {
        return NULL;
    }
    list->size = 0;
    list->head = NULL;
    return list;
}

int add_filter_node(struct filterList* list, int8_t type, int len, char* buff) {
    if (list == NULL) {
        return 0;
    }

    struct filterNode* iter = list->head;

    while(iter != NULL) {
        if (_filter_node_cmp(iter, type, len, buff) == 0) {
            break;
        }
        iter = iter->next;
    }

    if (iter == NULL) {
        struct filterNode* fn = _create_filter_node(type, len, buff);
        if (fn == NULL) {
            return 0;
        }
        fn->next = list->head;
        list->head = fn;
        list->size++;
    } else {
        iter->timestamp = time(NULL);
    }

    return 1;
}

int add_filter_node1(struct filterList* list, struct filterNode* node) {
    if (list == NULL || node == NULL) {
        return 0;
    }

    struct filterNode* iter = list->head;

    while(iter != NULL) {
        if (_filter_node_cmp(iter, node->type, node->len, node->buff) == 0) {
            break;
        }
        iter = iter->next;
    }

    if (iter == NULL) {
        node->next = list->head;
        list->head = node;
        list->size++;
    } else {
        iter->timestamp = node->timestamp;
        _free_filter_node(node);
    }

    return 1;
}

int remove_filter_node(struct filterList* list, int8_t type, int len, char* buff) {
    if (list == NULL) {
        return 0;
    }

    struct filterNode* iter = list->head;
    struct filterNode* last = iter;

    while(iter != NULL) {
        if (_filter_node_cmp(iter, type, len, buff) == 0) {
            break;
        }
        last = iter;
        iter = iter->next;
    }

    if (iter != NULL) {
        if (last == list->head) {
            list->head = iter->next;
        } else {
            last->next = iter->next;
        }
        _free_filter_node(iter);
        list->size--;
    }

    return 1;
}

int free_filter_list(struct filterList* list) {
    if (list == NULL) {
        return 0;
    }
    struct filterNode* ptr = list->head;
    while(ptr) {
        struct filterNode* node = ptr;
        ptr = ptr->next;
        _free_filter_node(node);
    }

    zfree(list);
    return 1;
}

unsigned int search_filter_node(struct filterList* list, int8_t type, int len, char* buff) {
    if (list == NULL) {
        return 0;
    }

    struct filterNode* iter = list->head;

    while(iter != NULL) {
        if (_filter_node_cmp(iter, type, len, buff) == 0) {
            return iter->timestamp;
        }
        iter = iter->next;
    }

    return 0;
}

void reset_filter_list_iterator(struct filterListIterator* iter) {
    if (iter) {
        iter->next = iter->head;
    }
}

void free_filter_list_iterator(struct filterListIterator *iter) {
    if (iter) {
        zfree(iter);
    }
}

struct filterListIterator* create_filter_list_iterator(struct filterList *list, int8_t type) {
    if (list == NULL) {
        return NULL;
    }

    struct filterListIterator* fli = zmalloc(sizeof(struct filterListIterator));
    if (fli == NULL) {
        return NULL;
    }

    fli->type = type;
    fli->head = list->head;
    fli->next = list->head;

    return fli;
}

struct filterNode* next_filter_node(struct filterListIterator* fli) {
    if (fli == NULL) {
        return NULL;
    }

    while(fli->next) {
        struct filterNode* node = fli->next;
        if ((fli->type) & (node->type)) {
            fli->next = fli->next->next;
            return node;
        }
    }
    return NULL;
}

#endif
