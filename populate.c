#define _GNU_SOURCE

#define NTHREADS 32
#define MAXDIRS 1024
#define BULK 0
#define RECURSIVE 1
#define PREPARE 1
#define OUTPUT 0

#include <dirent.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
//#include <sys/attr.h>
#include <sys/errno.h>
#include <sys/stat.h>
//#include <sys/vnode.h>
#include <unistd.h>

#if OUTPUT
#include <sqlite3.h>
#endif

const char preamble[] =
  "BEGIN TRANSACTION; "
  "CREATE TABLE IF NOT EXISTS metadata (name TEXT PRIMARY KEY, mtime INTEGER, size INTEGER); "
  "CREATE INDEX IF NOT EXISTS mtime_index ON metadata (mtime); "
  "DELETE FROM metadata;";

const char postamble[] = "COMMIT TRANSACTION;";

const char insert_template[] = "INSERT INTO metadata VALUES(?, ?, ?);";

const char insert_fmt[] = "INSERT INTO metadata VALUES('%s', %ld, %lld);";

static pthread_mutex_t global_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t global_cond = PTHREAD_COND_INITIALIZER;
static const char *global_queue[MAXDIRS];
static int global_queue_len;
static int global_active;
static off_t global_size;

#if OUTPUT
static sqlite3 *conn;
#endif

void die(const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  va_end(ap);

  if (errno) {
    fprintf(stderr, ": %s", strerror(errno));  
  }
  
  fputc('\n', stderr);

  exit(EXIT_FAILURE);
}

#if OUTPUT
void insert(void *stmt_ptr, const char *path, long mtime, off_t size) {
#if PREPARE
  sqlite3_stmt *stmt = (sqlite3_stmt *) stmt_ptr;
  if (sqlite3_reset(stmt) != SQLITE_OK) {
    die("sqlite3_reset");
  }
  if (sqlite3_bind_text(stmt, 1, path, -1, SQLITE_STATIC) != SQLITE_OK) {
    die("sqlite3_bind_text");
  }
  if (sqlite3_bind_int64(stmt, 2, mtime) != SQLITE_OK) {
    die("sqlite3_bind_int64");
  }
  if (sqlite3_bind_int64(stmt, 3, size) != SQLITE_OK) {
    die("sqlite3_bind_int64");
  }
  if (sqlite3_step(stmt) != SQLITE_DONE) {
    die("sqlite3_step");
  }
#else
  char *errmsg;
  char query[1024];
  snprintf(query, sizeof(query), insert_fmt, path, mtime, size);
  if (sqlite3_exec(conn, query, NULL, NULL, &errmsg) != SQLITE_OK) {
    die("sqlite3_exec: %s", errmsg);
  }
#endif
}
#endif

#if BULK
off_t process_dir_fast(const char *path, void *stmt) {
  struct attrlist alist;
  memset(&alist, 0, sizeof(alist));
  alist.bitmapcount = ATTR_BIT_MAP_COUNT;
  alist.commonattr = ATTR_CMN_RETURNED_ATTRS | ATTR_CMN_NAME | ATTR_CMN_ERROR | ATTR_CMN_OBJTYPE | ATTR_CMN_MODTIME;
  alist.fileattr = ATTR_FILE_DATALENGTH;

  char attrbuf[65536];

  off_t total_size = 0;

  int fd = open(path, O_RDONLY | O_DIRECTORY);
  if (fd < 0) {
    die("open(%s)", path);
  }

  for (;;) {
    int count = getattrlistbulk(fd, &alist, attrbuf, sizeof(attrbuf), 0);
    if (count < 0) {
      die("getattrlistbulk(%s)", path);
    }
    if (count == 0) {
      break;
    }

    char *ptr = &attrbuf[0];

    for (int j = 0; j < count; j++) {
      char *field = ptr;

      const char *name = "";
      int err = 0;
      fsobj_type_t type = 0;
      long mtime = 0;
      off_t size = 0;

      uint32_t attrlen = *((uint32_t *) field);
      field += sizeof(uint32_t);

      attribute_set_t *attrset = (attribute_set_t *) field;
      field += sizeof(attribute_set_t);

      if (attrset->commonattr & ATTR_CMN_ERROR) {
        err = *((uint32_t *) field);
        field += sizeof(uint32_t);
      }

      if (attrset->commonattr & ATTR_CMN_NAME) {
        attrreference_t *ref = (attrreference_t *) field;
        name = field + ref->attr_dataoffset;
        field += sizeof(attrreference_t);
      }

      if (attrset->commonattr & ATTR_CMN_OBJTYPE) {
        type = *((fsobj_type_t *) field);
        field += sizeof(fsobj_type_t);
      }

      if (attrset->commonattr & ATTR_CMN_MODTIME) {
        mtime = ((struct timespec *) field)->tv_sec;
        field += sizeof(struct timespec);
      }

      if (attrset->fileattr & ATTR_FILE_DATALENGTH) {
        size = *((off_t *) field);
        field += sizeof(off_t);
      }

      if (err) {
        errno = err;
        die("getattrlistbulk(%s)", path);
      }

#if RECURSIVE
      if (type == VDIR && strcmp(name, ".") != 0 && strcmp(name, "..") != 0) {
        char *child_path;
        asprintf(&child_path, "%s/%s", path, name);

        if (pthread_mutex_lock(&global_mutex)) {
          die("pthread_mutex_lock");
        }

        global_queue[global_queue_len++] = child_path;

        if (pthread_cond_signal(&global_cond)) {
          die("pthread_cond_signal");
        }

        if (pthread_mutex_unlock(&global_mutex)) {
          die("pthread_mutex_unlock");
        }
      }
#endif

      if (type == VREG) {
        char child_path[1024];
        snprintf(child_path, sizeof(child_path), "%s/%s", path, name);

#if OUTPUT
        insert(stmt, child_path, mtime, size);
#endif
        total_size += size;
      }

      ptr += attrlen;
    }
  }

  if (close(fd)) {
    die("close(%s)", path);
  }

  return total_size;
}
#endif

#if !BULK
off_t process_dir_slow(const char *path, void *stmt) {
  off_t total_size = 0;

  DIR *dir = opendir(path);
  if (dir == NULL) {
    die("opendir(%s)", path);
  }

  int fd = dirfd(dir);

  struct dirent *e;
  struct stat st;

  while (errno = 0, (e = readdir(dir)) != NULL) {
    if (fstatat(fd, e->d_name, &st, AT_SYMLINK_NOFOLLOW )) {
      die("stat(%s)", e->d_name);
    }

#if RECURSIVE
    if (S_ISDIR(st.st_mode) && strcmp(e->d_name, ".") != 0 && strcmp(e->d_name, "..") != 0) {
      char *child_path;
      asprintf(&child_path, "%s/%s", path, e->d_name);

      if (pthread_mutex_lock(&global_mutex)) {
        die("pthread_mutex_lock");
      }

      global_queue[global_queue_len++] = child_path;

      if (pthread_cond_signal(&global_cond)) {
        die("pthread_cond_signal");
      }

      if (pthread_mutex_unlock(&global_mutex)) {
        die("pthread_mutex_unlock");
      }
    }
#endif

    if (S_ISREG(st.st_mode)) {
      char *child_path;
      asprintf(&child_path, "%s/%s", path, e->d_name);

#if OUTPUT
      insert(stmt, child_path, st.st_mtimespec.tv_sec, st.st_size);
#endif
      total_size += st.st_size;

      free(child_path);
    }
  }
  if (errno) {
    die("readdir(%s)", path);
  }

  if (closedir(dir)) {
    die("closedir(%s)", path);
  }

  return total_size;
}
#endif

void *threadmain(void *unused) {
  void *stmt = NULL;

#if OUTPUT && PREPARE
  if (sqlite3_prepare_v2(conn, insert_template, sizeof(insert_template), &stmt, NULL) != SQLITE_OK) {
    die("sqlite3_prepare_v2");
  }
#endif

  for (;;) {
    const char *path;
    bool exit;

    if (pthread_mutex_lock(&global_mutex)) {
      die("pthread_mutex_lock");
    }

again:
    if (global_queue_len) {
      exit = false;
      path = global_queue[--global_queue_len];
      global_active++;
    } else {
      path = NULL;
      exit = !RECURSIVE || !global_active;
    }

    if (path == NULL && !exit) {
      if (pthread_cond_wait(&global_cond, &global_mutex)) {
        die("pthread_cond_wait");
      }
      goto again;
    }

    if (pthread_mutex_unlock(&global_mutex)) {
      die("pthread_mutex_unlock");
    }

    if (exit) {
      break;
    }

#if BULK
    off_t size = process_dir_fast(path, stmt);
#else
    off_t size = process_dir_slow(path, stmt);
#endif

    if (pthread_mutex_lock(&global_mutex)) {
      die("pthread_mutex_lock");
    }

    global_size += size;
    global_active--;

#if RECURSIVE
    if (pthread_cond_broadcast(&global_cond)) {
      die("pthread_cond_broadcast");
    }
#endif

    if (pthread_mutex_unlock(&global_mutex)) {
      die("pthread_mutex_unlock");
    }
  }

#if OUTPUT && PREPARE
  if (sqlite3_finalize(stmt) != SQLITE_OK) {
    die("sqlite3_finalize");
  }
#endif

  return NULL;
}


int main(int argc, char **argv) {

  if (argc < 3) {
    die("bad arguments");
  }

  const char *db_path = argv[1];

  for (int i = 2; i < argc; i++) {
    global_queue[global_queue_len++] = argv[i];
  }

#if OUTPUT
  if (sqlite3_open_v2(db_path, &conn, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX, NULL) != SQLITE_OK) {
    die("sqlite3_open");
  }
  
  char *errmsg;
  if (sqlite3_exec(conn, preamble, NULL, NULL, &errmsg) != SQLITE_OK) {
    die("sqlite3_exec: %s", errmsg);
  }
#endif

  pthread_t threads[NTHREADS];

  for (int i = 0; i < NTHREADS; i++) {
    if (pthread_create(&threads[i], NULL, threadmain, NULL)) {
      die("pthread_create");
    }
  }

  for (int i = 0; i < NTHREADS; i++) {
    if (pthread_join(threads[i], NULL)) {
      die("pthread_join");
    }
  }

#if OUTPUT
  if (sqlite3_exec(conn, postamble, NULL, NULL, &errmsg) != SQLITE_OK) {
    die("sqlite3_exec: %s", errmsg);
  }

  if (sqlite3_close(conn) != SQLITE_OK) {
    die("sqlite3_close");
  }
#endif

  printf("Total size: %lld\n", global_size);
  
  return 0;
}

