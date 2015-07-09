#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <errno.h>

#include "mem_queue.hpp"

static int max_num = 10000000;

void* push_thread_func(void* argv) {
    int now = 0;
    MemQueue<int>* memQueue = (MemQueue<int>*)argv;
    while(1) {
        int* ins = (int*)malloc(sizeof(int));
        *ins = now;
        int isOk = memQueue->push(ins); 
        if (isOk) {
            now++;
            if (now >= max_num) {
                break;
            }
        } else {
            printf("wait...%d\n", now);
        }
    }
    return NULL;
}

void* pop_thread_func(void* argv) {
    int now = 0;
    MemQueue<int>* memQueue = (MemQueue<int>*)argv;
    while(1) {
        int* ins = NULL;
        int isOk = memQueue->pop(&ins);
        if (isOk && ins != NULL) {
            if (*ins != now) {
                printf("%d != %d", *ins, now);
                break;
            }
            now++;
            free(ins);
            if (now >= max_num) {
                break;
            }
        }
    }
    return NULL;
}

int main(int argc, char** argv) {
    MemQueue<int>* memQueue = new MemQueue<int>(0, 1024*10);

    pthread_t push_thread[10];
    pthread_t pop_thread[10];

    for(int i = 0; i < 10; i++) {
        if (pthread_create(&push_thread[i], NULL, push_thread_func, (void*)memQueue) != 0) {
            printf("error\n");
        }
    }
    for(int i = 0; i < 10; i++) {
        if (pthread_create(&pop_thread[i], NULL, pop_thread_func, (void*)memQueue) != 0) {
            printf("error\n");
        }
    }

    for(int i = 0; i < 10; i++) {
        pthread_join(push_thread[i], NULL);
    }
    for(int i = 0; i < 10; i++) {
        pthread_join(pop_thread[i], NULL);
    }

    delete memQueue;
    printf("over\n");

    return 0;
}
