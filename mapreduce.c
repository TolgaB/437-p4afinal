#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>
#include <semaphore.h>
#include "mapreduce.h"

int num_reducersGlobal;
struct reduceTable **tableArray;
Partitioner partitionFunc;
int *reduceNeeded;
int hashSize;

//Thread local variable
__thread struct Node **hashTable;
pthread_mutex_t lock;

struct wrapperStruct {
        Mapper map;
        Combiner combine;
	int numFiles;
        char **files;
};

struct Node {
	char *key;
	char *value;
	struct Node *prev;
	struct Node *next;
};

struct reduceStruct {
        Reducer reduce;
        int partition_number;
};

struct reduceTable {
	struct Node **hashTable;
};

//Hash Table Stuff

//djb2 hashing algorithm by Dan Bernstein
unsigned long
hash(unsigned char *str)
{
    unsigned long hash = 5381;
    int c;

    while (((c) = *str++))
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

    return hash;
}



char *get_next(char *key) {
	int hashVal = hash((unsigned char *)key) % hashSize;
	struct Node *traversal = (struct Node *)malloc(sizeof(struct Node));
	traversal = hashTable[hashVal];
	int found = 0;
	while (traversal != NULL) {
		if (strcmp(traversal->key,key) == 0) {
			//found one
			if (traversal->prev == NULL) {
				//the head
				if (hashTable[hashVal]->next != NULL) {
                                        (hashTable[hashVal]->next)->prev = NULL;
                                        hashTable[hashVal] = hashTable[hashVal]->next;
                                }else {
                                        hashTable[hashVal] = NULL;
                                }
			}
			else {
				if (traversal->next != NULL) {
					(traversal->prev)->next = traversal->next;
					(traversal->next)->prev = traversal->prev;
				} else {
					(traversal->prev)->next = NULL;
				}
			}
			found = 1;
			break;
		}
		else {
			traversal = traversal->next;
		}
	}
	if (found == 1) {
		free(traversal->key);
		free(traversal->value);
		free(traversal);
		return key;
	}
	free(traversal);
	//couldnt find
	return NULL;
}

char *get_nextReduce(char *key, int partition_number) {
	pthread_mutex_lock(&lock);
        //gotta use mutex when accessing the reduceArray
	int secHashVal = hash((unsigned char *)key) %hashSize;
	struct Node *traversal = (struct Node *)malloc(sizeof(struct Node));
        traversal = ((tableArray[partition_number])->hashTable)[secHashVal];
        char *value = "";
	while (traversal != NULL) {
                if (strcmp(traversal->key,key) == 0) {
                        value = malloc(sizeof(char)*strlen(traversal->value));
			strcpy(value,traversal->value);
			//found one
                        if (traversal->prev == NULL) {
                                //the head
                                if (((tableArray[partition_number])->hashTable)[secHashVal]->next != NULL) {
					(((tableArray[partition_number])->hashTable)[secHashVal]->next)->prev = NULL;
                                       ((tableArray[partition_number])->hashTable)[secHashVal] = ((tableArray[partition_number])->hashTable)[secHashVal]->next;
                                }else {
                                        ((tableArray[partition_number])->hashTable)[secHashVal]  = NULL;
                                }
				break;
                        }
                        else {
				if (traversal->next != NULL) {
                                        (traversal->prev)->next = traversal->next;
                                        (traversal->next)->prev = traversal->prev;
                                } else {
                                        (traversal->prev)->next = NULL;
                                }
				break;
                        }
                }
                else {
                        traversal = traversal->next;
                }
        }
	pthread_mutex_unlock(&lock);
	if (strcmp(value,"") != 0) {
		free(traversal->key);
		free(traversal->value);
		free(traversal);
		return value;
	}
	free(traversal);
	return NULL;
}

void MR_EmitToCombiner(char *key, char *value) {
	int hashVal = hash((unsigned char *) key) % hashSize;
	if (hashTable == NULL) {
		hashTable = malloc(sizeof(struct Node) * hashSize);	
	}
	if (hashTable[hashVal] == NULL) {
		hashTable[hashVal] = (struct Node *)malloc(sizeof(struct Node));
		hashTable[hashVal]->key = (char *)malloc(sizeof(char) * (strlen(key)));
		hashTable[hashVal]->value = (char *)malloc(sizeof(char) * (strlen(value)));
		strcpy(hashTable[hashVal]->key,key);
		strcpy(hashTable[hashVal]->value,value);
		hashTable[hashVal]->next = NULL;
	} else {
		struct Node *newHead = (struct Node *)malloc(sizeof(struct Node));
		newHead->key = (char *)malloc(sizeof(char) * (strlen(key)));
                newHead->value = (char *)malloc(sizeof(char) * (strlen(value)));
                strcpy(newHead->key,key);
                strcpy(newHead->value,value);
		newHead->next = hashTable[hashVal];
		hashTable[hashVal]->prev = newHead;
		hashTable[hashVal] = newHead;
	}
}

void MR_EmitToReducer(char *key, char *value) {
	//need to use mutex
        //gotta use mutex when accessing the reduceArray
	pthread_mutex_lock(&lock);

	unsigned long hashVal;
	int secHashVal = hash((unsigned char *)key) %hashSize;
        //check which partition func to use
        if (partitionFunc == NULL) {
                hashVal = MR_DefaultHashPartition(key, num_reducersGlobal);
        } else {
                hashVal = partitionFunc(key, num_reducersGlobal);
        }
	if (((tableArray[hashVal])->hashTable)[secHashVal] == NULL) {
		reduceNeeded[hashVal] = 1;
                ((tableArray[hashVal])->hashTable)[secHashVal] = (struct Node *)malloc(sizeof(struct Node));
		((tableArray[hashVal])->hashTable)[secHashVal]->key = malloc(sizeof(char) * strlen(key));
                ((tableArray[hashVal])->hashTable)[secHashVal]->value = malloc(sizeof(char) * strlen(value));
                strcpy(((tableArray[hashVal])->hashTable)[secHashVal]->key,key);
                strcpy(((tableArray[hashVal])->hashTable)[secHashVal]->value,value);
		((tableArray[hashVal])->hashTable)[secHashVal]->next = NULL;
	} else {
		//add to existing list
		struct Node *newHead = (struct Node *)malloc(sizeof(struct Node));
                newHead->key = malloc(sizeof(char) * strlen(key));
                newHead->value = malloc(sizeof(char) * strlen(value));
                strcpy(newHead->key,key);
                strcpy(newHead->value,value);
                newHead->next = ((tableArray[hashVal])->hashTable)[secHashVal];
                ((tableArray[hashVal])->hashTable)[secHashVal]->prev = newHead;
                ((tableArray[hashVal])->hashTable)[secHashVal] = newHead;
	}
	pthread_mutex_unlock(&lock);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}


void *mapper_wrapper(void *vwrapperInfo) {
	struct wrapperStruct *wrapperInfo = (struct wrapperStruct *) vwrapperInfo;
	Mapper map = wrapperInfo->map;
	Combiner combine = wrapperInfo->combine;
	//loop through all the files
	for (int i = 0; i < wrapperInfo->numFiles; i++) {
		map(wrapperInfo->files[i]);
		//now need to call cominbe
	//	if (combine == NULL) {continue;}
	}
	if (combine != NULL) {
	for (int i = 0; i < hashSize; i++) {
                        while (hashTable[i] != NULL) {
                                char *tempKeyCpy = (char *)malloc(sizeof(char) * strlen(hashTable[i]->key));
                                strcpy(tempKeyCpy,hashTable[i]->key);
                                combine(tempKeyCpy, get_next);
                                free(tempKeyCpy);
                        }
         }
	}
	free(hashTable);
	hashTable = NULL;
	return 0;
}

void *reduce_helper(void *wreducerInfo) {
	struct reduceStruct *reduceInfo = (struct reduceStruct *)wreducerInfo;
	Reducer reduce = reduceInfo->reduce;
	for (int i = 0; i < hashSize; i++) {
		while(((tableArray[reduceInfo->partition_number])->hashTable)[i] != NULL) {
			char *tempKeyCpy = (char *)malloc(sizeof(char) * strlen(((tableArray[reduceInfo->partition_number])->hashTable)[i]->key));
                        strcpy(tempKeyCpy,((tableArray[reduceInfo->partition_number])->hashTable)[i]->key);
                        reduce(tempKeyCpy,NULL,get_nextReduce,reduceInfo->partition_number);
                        free(tempKeyCpy);	
		}
	}
	free(reduceInfo);
	reduceInfo = NULL;
	return 0;
};

void MR_Run(int argc, char *argv[],
        Mapper map, int num_mappers,
        Reducer reduce, int num_reducers,
        Combiner combine,
        Partitioner partition) {


	pthread_mutex_init(&lock,NULL);
	num_reducersGlobal = num_reducers;
	hashSize = 256000;
	reduceNeeded = (int *)malloc(sizeof(int) * num_reducers);	
	tableArray = (struct reduceTable **)malloc(sizeof(struct reduceTable *)*num_reducers);
	for (int i = 0; i < num_reducers; i++) {
		tableArray[i] = (struct reduceTable *)malloc(sizeof(struct reduceTable));
		if (tableArray[i] == NULL) {
                        printf("space was inufficient\n");
                }
		tableArray[i]->hashTable = (struct Node **)malloc(sizeof(struct Node *)* (hashSize));
		if (tableArray[i]->hashTable == NULL) {
			printf("space was inufficient\n");
		}
	}


	if (partition == NULL) {
		partitionFunc = NULL;
	} else {
		partitionFunc = partition;
	}

	pthread_t *mapperThreads = (pthread_t *)malloc(sizeof(pthread_t) * num_mappers);
	struct wrapperStruct **wrapperArr = (struct wrapperStruct**)malloc(sizeof(struct wrapperStruct ) * num_mappers);
	
	int properDivide = (argc % num_mappers);
        if (properDivide != 0) {
                properDivide = 1;
        }

        int maxFileInMapper = (((argc-1)/num_mappers) + properDivide);
	
	for (int i = 1; i < argc; i++) {
		if (wrapperArr[(i-1)%num_mappers] == NULL) {
			wrapperArr[(i-1)%num_mappers] = (struct wrapperStruct *) malloc(sizeof(struct wrapperStruct));
			wrapperArr[(i-1)%num_mappers]->combine = combine;
			wrapperArr[(i-1)%num_mappers]->map = map;
		}
		if (wrapperArr[(i-1)%num_mappers]->files == NULL) {
			wrapperArr[(i-1)%num_mappers]->files = malloc(sizeof(char *) * maxFileInMapper);
			wrapperArr[(i-1)%num_mappers]->numFiles = 0;
		}
		int tempNum = wrapperArr[(i-1)%num_mappers]->numFiles;
                wrapperArr[(i-1)%num_mappers]->files[tempNum] = malloc(sizeof(char) * strlen(argv[i]));
                strcpy(wrapperArr[(i-1)%num_mappers]->files[tempNum],argv[i]);
                wrapperArr[(i-1)%num_mappers]->numFiles = wrapperArr[(i-1)%num_mappers]->numFiles + 1;
	}

	//create the mapper threads
	for (int j = 0; j < num_mappers; j++) {
		if (wrapperArr[j] == NULL) {
			continue;
		}
		//create the thread
		int chk = pthread_create(&(mapperThreads[j]),NULL,(&mapper_wrapper), wrapperArr[j]);
		if (chk != 0) {
                        printf("error occured when attempting to create thread\n");
                        exit(1);
                }
	}
	//join all the mapper threads
	for (int j = 0; j < num_mappers; j++) {
		if (wrapperArr[j] == NULL) {
			continue;
		}
		int chk = pthread_join(mapperThreads[j],NULL);
		if (chk != 0) {
                        printf("error occured when attempting to joing thread\n");
                        exit(1);
                }
	}

	//free up mem
        for (int i = 0; i < num_mappers; i++) {
                if (wrapperArr[i] == NULL) {
                        continue;
                }
                for (int j = 0; j < wrapperArr[i]->numFiles; j++) {
                        if (wrapperArr[i]->files[j] != NULL) {
                                //free the file strings
                                free(wrapperArr[i]->files[j]);
                        }
                }
                free(wrapperArr[i]->files);
                //removing line below break the program?
                free(wrapperArr[i]);
		wrapperArr[i]->files = NULL;
		wrapperArr[i]->numFiles = 0;
		wrapperArr[i] = NULL;
        }
        free(wrapperArr);
	wrapperArr = NULL;

	
	free(mapperThreads);
	mapperThreads = NULL;

	pthread_t *reducerThreads = (pthread_t *)malloc(sizeof(pthread_t) * num_reducers);

	//create the reduce threads
	for (int i = 0; i < num_reducers; i++) {
		if (reduceNeeded[i] == 1) {
			struct reduceStruct *helperInfo = (struct reduceStruct*)malloc(sizeof(struct reduceStruct));
			helperInfo->reduce = reduce;
			helperInfo->partition_number = i;
			int chk = pthread_create((&(reducerThreads[i])),NULL, &reduce_helper, helperInfo);
                	if (chk != 0) {
                        	printf("error occured when attempting to create thread\n");
                        	exit(1);
                	}
		}
	}

	//join all the reduce threads
	for (int i = 0; i < num_reducers; i++) {
		if (reduceNeeded[i] == 1) {
			int chk = pthread_join(reducerThreads[i],NULL);
			if (chk != 0) {
                        	printf("error occured when attempting to joing thread\n");
                        	exit(1);
                	}
		}
	}


	for (int i = 0; i < num_reducers; i++) {
                if (tableArray[i]->hashTable != NULL) {
                        free(tableArray[i]->hashTable);
			tableArray[i]->hashTable = NULL;
                }
                free(tableArray[i]);
		tableArray[i] = NULL;
        }
        free(tableArray);
	tableArray = NULL;


	free(reducerThreads);
	reducerThreads = NULL;
	free(reduceNeeded);
	reduceNeeded = NULL;

	pthread_mutex_destroy(&lock);
}
