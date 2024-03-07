#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/errno.h>

#include <sqlite3.h>

const char query[] = "SELECT name, mtime, size FROM metadata ORDER BY name ASC";

void die(const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  va_end(ap);

  exit(EXIT_FAILURE);
}

int callback(void *unused, int ncols, char **cols, char **colnames) {
  printf("%s %s %s\n", cols[0], cols[1], cols[2]);
  return 0;
} 

int main(int argc, char **argv) {
  sqlite3 *conn;

  if (sqlite3_open_v2("out.db", &conn, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
    die("sqlite3_open");
  }
  
  char *errmsg;
  if (sqlite3_exec(conn, query, callback, NULL, &errmsg) != SQLITE_OK) {
    die("sqlite3_exec: %s", errmsg);
  }

  if (sqlite3_close(conn) != SQLITE_OK) {
    die("sqlite3_close");
  }

  return 0;
}