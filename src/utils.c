#include "utils.h" 
#include "csapp/csapp.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <stdarg.h>
#include <assert.h>

/** @brief round up 'n' to the nearest multiple of 'mult'. This function assumes
 *         that mult is a power of two.
*/
size_t round_up(size_t n, size_t mult) { return (n + (mult-1)) & ~(mult-1); }


/** @brief Creates a hash table used to lookup database entries. The hash table
 *         is created by reading the entries in db->m_ptr. After returning from 
 *         this function, db->h_table will point to the newly created hash table
 * 
 *  @param db the database struct to construct the hash table for
 *  
 *  @note  A node should call this function after it has received it's partition
 *         of the database in the digest phase.
*/
void build_hash_table(database *db) {
  hash_table *ht; 
  char *curr_offset = db->m_ptr;
  int index;

  ht = calloc(1, sizeof(hash_table)); 
  if(!ht) {
      // Handle memory allocation failure (shouldnt happen)
      return;
  }
  ht->num_buckets = NUM_BUCKETS;
  ht->buckets = calloc(NUM_BUCKETS, sizeof(bucket)); 
  if(!ht->buckets) {
      // Handle memory allocation failure (shouldnt happen)
      free(ht);
      return;
  }
  while (curr_offset < db->m_ptr + db->db_size) {
    index = lookup_insert(ht, curr_offset); 
    ht->buckets[index].used = 1;
    ht->buckets[index].word = curr_offset;
    curr_offset = get_next_key_offset(curr_offset); 
  }
  db->h_table = ht;
}

/** @brief  Find a free slot in a hash table for the given word. The index of
 *          the bucket that the word can be inserted into is returned. 
 * 
 *  @param  ht hash table to insert the word into 
 *  @param  word word to insert into the hash table
 *  @return index at which the word can be inserted
*/
int lookup_insert(hash_table *ht, char *word) {
  int i;
  unsigned int h, o, k = 0;
  for (i = 0; word[i]; i++)
    k = (k * 33) + word[i];
  h = k % (ht->num_buckets);
  o = 1 + (k % (ht->num_buckets - 1));
  for (i = 0; i < ht->num_buckets; i++) {
    if (ht->buckets[h].used == 0) return h;
    h += o;
    if (h >= (unsigned int)ht->num_buckets)
      h = h - ht->num_buckets;
  }
  fprintf(stderr, "pedsort: hash table full\n");
  exit(1);
}

/** @brief  Searches a hash table for a given word, and returns the index where
 *          the word is found. 
 * 
 *  @param  ht hash table to search
 *  @param  word word to search for
 *  @return Index of the bucket that contains the word, or -1 if the word is not
 *          found.
 */
int lookup_find(hash_table *ht, char *word) {
  int i;
  unsigned int h, o, k = 0;
  for (i = 0; word[i]; i++) k = (k * 33) + word[i];
  h = k % (ht->num_buckets);
  o = 1 + (k % (ht->num_buckets - 1));
  for (i = 0; i < ht->num_buckets; i++) {
    if (ht->buckets[h].used && (strcmp(ht->buckets[h].word, word) == 0))
      return h;
    h += o;
    if (h >= (unsigned int)ht->num_buckets) h = h - ht->num_buckets;
  }
  return -1;
}

/** @brief  Gets the file size of a file known by the given file descriptor.
 *  
 *  @param  fd file descriptor to find the size of. 
 *  @return File size. Will print an error message and exit if fstat fails for
 *          any reason. 
 *  
 *  @note   This function only needs to be called by the parent process when 
 *          loading the entire database into memory in the digest phase. You 
 *          will not need to call it yourself.
*/
off_t get_filesize(int fd) {
  struct stat info;
  if (fstat(fd, &info) < 0) {
    fprintf(stderr, "fstat error: %s\n", strerror(errno));
    exit(1);
  }
  return info.st_size;
}

/** @brief  Loads a database using mmap from the filepath given by db_filename.
 *          Returns a pointer to a newly allocated database struct that contains
 *          the size of the file and a pointer to the start of the memory mapped
 *          region.
 *         
 *  @param  db_filename filename of the database to load into memory. 
 * 
 *  @return a pointer to a newly allocated database struct
 * 
 *  @note   This function should ONLY be called by the parent process in the 
 *          digest phase. Individual nodes receive their partition of the 
 *          database by sending requests to the parent process, not by loading
 *          the entire file in themselves.
*/
database *load_database(char *db_filename) {
  int db_fd; 
  off_t size;
  char *ptr;
  database *db = malloc(sizeof(database)); 
  db_fd = open(db_filename, O_RDWR);
  if (db_fd < 0) {
    fprintf(stderr, "open error: %s\n", strerror(errno));
    exit(1);
  }
  size = get_filesize(db_fd);
  ptr = Mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE, db_fd, 0);
  if (ptr == MAP_FAILED) {
    fprintf(stderr, "mmap error: %s\n", strerror(errno));
    exit(1);
  }
  Close(db_fd);
  db->db_size = size;
  db->h_table = NULL;
  db->m_ptr   = ptr;
  return db; 
}



/** @brief Given a pointer into the memory-mapped database that is the start of
 *         an entry, returns a pointer to the entry's associated value_array. 
 *  
 *  @param entry_offset pointer to the start of an entry. It should be pointing
 *         to the first character of the entry's key. 
 * 
 *  @note  This function makes no checks around the validity of the entry that 
 *         is pointed to by entry_offset. 
*/
value_array *get_value_array(char *entry_offset) {
  return (value_array *) (entry_offset + round_up(strlen(entry_offset)+1, sizeof(int)));
}


/** @brief  Allocate memory for a value array and populate it with the values in 
 *          the entry that starts at the given offset.
 * 
 *  @param  entry_str An entry in "string form". This is the key followed by a 
 *          commma separated list of values. 
 *  @return Pointer to the newly created value array, or NULL if there was a 
 *          problem when converting the string to a value array.
 *  
 *  @note   You may find this function useful when implementing intersection 
 *          requests between keys that are in different nodes, and you need to 
 *          convert the response you receive from the other node from string 
 *          form to a value_array. 
 *  @note   Remember to free the memory that this function allocates when you're
 *          done with it!
*/
value_array *create_value_array(char *entry_str) {
  value_array *va;
  int ccount = 0;
  char *str  = strchr(entry_str, ','); // skip to first comma
  if (!str) 
    return NULL;

  for (char *c = str; *c != '\0'; c++) // number of commas = len of array
    if (*c == ',') 
      ccount++;

  va = malloc(sizeof(value_array) + (ccount * sizeof(int))); 
  if (va == NULL)
    return NULL;

  va->len = ccount;
  for (int i = 0; i < ccount; i++) {
    if (sscanf(str, ",%u", va->arr+i) <= 0) {
      free(va);
      return NULL;
    }
    str = strchr(++str, ',');
  }
  return va;
}

/** @brief  Calculates the intersection of two value arrays, removing all 
 *          duplicate values. Returns a pointer to a newly allocated value_array
 *          that contains the intersection.
 * 
 *  @param  va_1 first value array 
 *  @param  va_2 second value array
 *  @return A pointer to a value_array that contains the intersection of va_1 
 *          and va_2, or NULL if either va_1/va_2 is NULL, or malloc fails. 
 * 
 *  @note   This function allocates memory using malloc. If you call this 
 *          function you will need to later free the return value yourself.
 *  @note   You don't /need/ to modify this method, but if you want to implement
 *          a more efficient way of calculating the intersection you are free to
 *          do so. 
*/
value_array *get_intersection(value_array *va_1, value_array *va_2) {
  int i, j, k = 0;
  value_array *dst;
  
  if ((va_1 == NULL) || (va_2 == NULL)) {
    return NULL;
  }

  dst = malloc(sizeof(value_array) + (va_1->len * sizeof(unsigned int))); 
  for (i = 0; i < va_1->len; i++) {
    if (i > 0 && va_1->arr[i] == va_1->arr[i-1]) // skip duplicates in va_1
      continue;
    for (j = 0; j < va_2->len; j++) {
      if (va_2->arr[j] > va_1->arr[i]) 
        break;
      else if (va_1->arr[i] == va_2->arr[j]) {
        dst->arr[k++] = va_1->arr[i]; 
        break; // skip duplicates in va_2
      }
    }
    dst->len = k; 
  }

  return dst;
}

/** @brief  Converts the value array to a string. The string is stored in the
 *          given buffer. Returns number of characters written.
 *  @note   This function will write a ',' before the first value in the array.
 * 
 *  @param  va Pointer to a value_array to convert to a string
 *  @param  buffer Buffer to write to
 *  @param  len Length of buffer. 
 *  @return Number of characters written to the buffer
*/
int value_array_to_str(value_array *va, char *buffer, int len) {
  int wl = 0; 
  for (int i = 0; i < va->len; i++) {
    wl += snprintf(buffer+wl, len-wl, ",%u", va->arr[i]);
  }
  wl += snprintf(buffer+wl, len-wl, "\n"); 
  return wl;
}

/** @brief  Writes an entry in string form (the key followed by the comma 
 *          separated list of values) to a given buffer. The last value is 
 *          followed by a newline character.
 * 
 *  @param  entry_offset Pointer to the start of an entry. This function makes 
 *          no checks regarding the validity of the entry. 
 *  @param  buffer Location to which the resulting string is written to. 
 *  @param  len Size of the buffer.
 *  @return The total number of characters written to the buffer.
*/
int entry_to_str(char *entry_offset, char *buffer, int len) {
  int n = 0; 
  n = snprintf(buffer, len, "%s", entry_offset); 
  return n + value_array_to_str(get_value_array(entry_offset), buffer+n, len-n);
}

/** @brief  Given a pointer to the start of an entry stored in the memory-mapped 
 *          database, returns a pointer to the start of the next entry stored. 
 * 
 *  @param  entry_offset a pointer to the start of an entry. This should point
 *          to the first character of an entry's key. This function makes no 
 *          checks on the validity of the entry_offset.
 *  @return A pointer to the start of the next entry.
 *  
 *  @note   This function does not check whether the address returned is a valid
 *          entry in the memory-mapped database. You may have to check that the
 *          the return value is still in the "range" of the memory-mapped region
 *          you're using.
*/
char *get_next_key_offset(char *entry_offset) {
  value_array *va = get_value_array(entry_offset); 
  return (char *) &(va->arr)[va->len];
}


/** @brief  Determines which node a key belongs to. 
 *  
 *  @param  key The key to find the 'owner' node of. This function assumes that
 *          the first character of the key is a valid starting character.
 *  @param  total_nodes The total number of nodes
 *  @return id of the node that should contain the given key.
 * 
 *  @note   You should use this function when you start implementing forwarding
 *          requests between multiple nodes.
*/
int find_node(char *key, int total_nodes) {
  return MIN(((*key - '0') / (KEY_SPACE/total_nodes)), total_nodes-1); 
}

/** @brief  Determine which section of the database should be sent to a given 
 *          node. Called by the parent process in the digest phase to figure out
 *          the section of the database to send to a node.
 * 
 *  @param  db The database that is to be partitioned. 
 *  @param  total_nodes Total number of nodes to partition the database between
 *  @param  node_id The ID of the node that the partition will be sent to. 
 *  @param  length A pointer to a variable that will contain the size of the 
 *          partition after this function returns. 
 *  @return A pointer to the start of the region of memory to send to the given
 *          node. The value of the length parameter is also updated to be the 
 *          total length (in bytes) of the section to send. 
 *
 *  @note   This function is used by the parent process in the digest phase, the
 *          nodes themselves do not need to call this function.
*/
char *get_partition(database *db, int total_nodes, int node_id, size_t *length) {
  char start = (node_id * (KEY_SPACE / total_nodes)) + '0'; 
  char end = ((node_id+1) * (KEY_SPACE / total_nodes)) + '0';
  char *curr = db->m_ptr;
  while ((curr < DB_END(db)) && (*curr < start)) {
    curr = get_next_key_offset(curr); 
  }
  char *start_ptr = curr;
  while ((curr < DB_END(db)) && ((*curr < end) || (node_id == total_nodes-1))) {
    curr = get_next_key_offset(curr); 
  }
  *length = (size_t) (curr - start_ptr); 
  return start_ptr;
}

/** @brief  Given a key and a database, searches for the key in the database's
 *          hash table. If it is found, returns a pointer to the start of the 
 *          entry in the database. If it is not found, returns NULL.
 * 
 *  @param  db  The database to search.
 *  @param  key The key to look for in the database's hash table.
 *  @return A pointer to the start of the entry in the database, or NULL if it
 *          is not found. 
*/
char *find_entry(database *db, char *key) {
  int idx;
  if ((db->m_ptr == NULL) || (db->h_table == NULL))
    return NULL;
  if ((idx = lookup_find(db->h_table, key)) == -1) 
    return NULL;
  else 
    return GET_BUCKET(db, idx).word;
}

/** @brief Replaces the first occurrence of a newline or carriage return in a 
 *         string with a null terminator. This function modifies the string in
 *         place.
 *  
 *  @param request_line The string to modify. 
 * 
 *  @note  This function may be useful when you have received a request for a 
 *         key from a client, and need to look for it in a node's database. Just
 *         calling "find_entry" on the entire request line will not work if the
 *         string still has a newline/carriage return in it.
*/
void request_line_to_key(char *request_line) {
  char *c; 
  for (c = request_line; *c != '\0'; c++) {
    if ((*c == '\n') || (*c == '\r')) {
      *c = '\0';
      break;
    }
  }
}

/** @brief  Converts a port number represented as an integer into a string that 
 *          is stored in the given str buffer.
 * 
 *  @param  port Port number. Must be an integer between 0 and MAX_PORTNUM.
 *  @param  buff Buffer to write the string representation of the port number 
 *               to. This function assumes that the size of the buffer is at 
 *               least six characters.
 *  @return number of characters written, or -1 if the port number is invalid.
*/
int port_number_to_str(int port, char *buff) {
  if (port < 0 || port > MAX_PORTNUM) {
    return -1; 
  }
  return snprintf(buff, 6, "%d", (unsigned short) port);
}

// NODE UTILS
/** @brief  Trims any characters following the last newline character in a string.
 * 
 *  @param  str The string to be trimmed. The function modifies this string in place.
 */
void trim_extraneous_chars(char *str) {
    char *last_newline = strrchr(str, '\n'); // Find last occurrence of newline
    if (last_newline) {
        *(last_newline + 1) = '\0'; // Null-terminate the string right after the last newline
    }
}


// THREADS

/** @brief  The main function to be run by each worker thread in the thread pool.
 * 
 *  @param  threadpool A pointer to the threadpool_t struct representing the thread pool.
 *  @return NULL.
 */
void* threadpool_thread(void* threadpool) {
    threadpool_t* pool = (threadpool_t*)threadpool;
    threadpool_task_t task;

    for (;;) {
        pthread_mutex_lock(&(pool->lock));

        // Wait on condition variable, check for spurious wakeups.
        while ((pool->count == 0) && (!pool->shutdown)) {
            pthread_cond_wait(&(pool->notify), &(pool->lock));
        }

        // Shutdown condition
        if ((pool->shutdown) && (pool->count == 0)) {
            break;
        }

        // Get our task
        task.function = pool->queue[pool->head].function;
        task.data = pool->queue[pool->head].data;
        pool->head = (pool->head + 1) % pool->queue_size;
        pool->count -= 1;

        pthread_mutex_unlock(&(pool->lock));

        // Execute the task
        (*(task.function))(task.data);
    }

    pthread_mutex_unlock(&(pool->lock));
    pthread_exit(NULL);
    return (NULL);
}
/** @brief  Creates a new thread pool.
 * 
 *  @param  thread_count The number of worker threads to create.
 *  @param  queue_size   The size of the task queue.
 *  @return A pointer to the newly created thread pool, or NULL if an error occurs.
 */
threadpool_t* threadpool_create(int thread_count, int queue_size) {
    threadpool_t* pool;
    int i;

    if ((pool = (threadpool_t*)malloc(sizeof(threadpool_t))) == NULL) {
        goto err;
    }

    pool->thread_count = 0;
    pool->queue_size = queue_size;
    pool->head = pool->tail = pool->count = 0;
    pool->shutdown = 0;
    pool->threads = (pthread_t*)malloc(sizeof(pthread_t) * thread_count);
    pool->queue = (threadpool_task_t*)malloc(sizeof(threadpool_task_t) * queue_size);

    if ((pthread_mutex_init(&(pool->lock), NULL) != 0) ||
        (pthread_cond_init(&(pool->notify), NULL) != 0) ||
        (pool->threads == NULL) ||
        (pool->queue == NULL)) {
        goto err;
    }

    for (i = 0; i < thread_count; i++) {
        if (pthread_create(&(pool->threads[i]), NULL, threadpool_thread, (void*)pool) != 0) {
            threadpool_destroy(pool, 0);
            return NULL;
        }
        pool->thread_count++;
    }

    return pool;

 err:
    if (pool) {
        threadpool_destroy(pool, 0);
    }
    return NULL;
}
/** @brief  Adds a new task to the thread pool's task queue.
 * 
 *  @param  pool     A pointer to the threadpool_t struct representing the thread pool.
 *  @param  function The function to be executed by a worker thread.
 *  @param  data     The data to be passed to the function.
 *  @return 0 if the task is added successfully, or -1 otherwise.
 */
int threadpool_add(threadpool_t* pool, void (*function)(void*), void* data) {
    int err = 0;
    if (pool == NULL || function == NULL) {
        return -1;
    }

    if (pthread_mutex_lock(&(pool->lock)) != 0) {
        return -1;
    }

    if (pool->count == pool->queue_size) {
        err = -1;
    } else {
        pool->queue[pool->tail].function = function;
        pool->queue[pool->tail].data = data;
        pool->tail = (pool->tail + 1) % pool->queue_size;
        pool->count += 1;

        if (pthread_cond_signal(&(pool->notify)) != 0) {
            err = -1;
        }
    }

    if (pthread_mutex_unlock(&pool->lock) != 0) {
        err = -1;
    }

    return err;
}
/** @brief  Destroys the thread pool.
 * 
 *  @param  pool  A pointer to the threadpool_t struct representing the thread pool.
 *  @param  flags Flags to indicate the shutdown mode (graceful or immediate).
 *  @return 0 if the pool is destroyed successfully, or -1 otherwise.
 */
int threadpool_destroy(threadpool_t* pool, int flags) {
    int i, err = 0;

    if (pool == NULL) {
        return -1;
    }

    if (pthread_mutex_lock(&(pool->lock)) != 0) {
        return -1;
    }

    if (flags && pool->shutdown) {
        err = -1;
    } else {
        pool->shutdown = (flags & 1) ? 1 : 0;
        if ((pthread_cond_broadcast(&(pool->notify)) != 0) ||
            (pthread_mutex_unlock(&(pool->lock)) != 0)) {
            err = -1;
        }

        for (i = 0; i < pool->thread_count; i++) {
            if (pthread_join(pool->threads[i], NULL) != 0) {
                err = -1;
            }
        }
    }

    if (!err) {
        threadpool_free(pool);
    }
    return err;
}

/** @brief  Frees all dynamically allocated resources used by the thread pool.
 * 
 *  @param  pool A pointer to the threadpool_t struct representing the thread pool.
 */
void threadpool_free(threadpool_t* pool) {
    if (pool == NULL) {
        return;
    }

    if (pool->threads) {
        free(pool->threads);
    }

    if (pool->queue) {
        free(pool->queue);
    }

    pthread_mutex_lock(&(pool->lock));
    pthread_mutex_destroy(&(pool->lock));
    pthread_cond_destroy(&(pool->notify));
    free(pool);
}

