// Copyright 2015 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#if defined(__FreeBSD__)
# define HAVE_EXTATTR
# define HAVE_SYSCTLBYNAME
#elif defined(__OpenBSD__)
// No sys/extattr.h or sysctlbyname on this platform.
#else
# error This BSD is not supported
#endif

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#if defined(HAVE_EXTATTR)
# include <sys/extattr.h>
#endif
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/sysctl.h>
#include <sys/types.h>

#include <string>

#include "src/main/native/unix_jni.h"

namespace blaze_jni {

using std::string;

// See unix_jni.h.
string ErrorMessage(int error_number) {
  char buf[1024] = "";
  if (strerror_r(error_number, buf, sizeof buf) < 0) {
    snprintf(buf, sizeof buf, "strerror_r(%d): errno %d", error_number, errno);
  }

  return string(buf);
}

int portable_fstatat(int dirfd, char *name, portable_stat_struct *statbuf,
                     int flags) {
  return fstatat(dirfd, name, statbuf, flags);
}

int StatSeconds(const portable_stat_struct &statbuf, StatTimes t) {
  switch (t) {
    case STAT_ATIME:
      return statbuf.st_atime;
    case STAT_CTIME:
      return statbuf.st_ctime;
    case STAT_MTIME:
      return statbuf.st_mtime;
  }
}

int StatNanoSeconds(const portable_stat_struct &statbuf, StatTimes t) {
  switch (t) {
    case STAT_ATIME:
      return statbuf.st_atimespec.tv_nsec;
    case STAT_CTIME:
      return statbuf.st_ctimespec.tv_nsec;
    case STAT_MTIME:
      return statbuf.st_mtimespec.tv_nsec;
  }
}

ssize_t portable_getxattr(const char *path, const char *name, void *value,
                          size_t size, bool *attr_not_found) {
#if defined(HAVE_EXTATTR)
  ssize_t result =
      extattr_get_file(path, EXTATTR_NAMESPACE_SYSTEM, name, value, size);
  *attr_not_found = (errno == ENOATTR);
  return result;
#else
  *attr_not_found = true;
  return -1;
#endif
}

ssize_t portable_lgetxattr(const char *path, const char *name, void *value,
                           size_t size, bool *attr_not_found) {
#if defined(HAVE_EXTATTR)
  ssize_t result =
      extattr_get_link(path, EXTATTR_NAMESPACE_SYSTEM, name, value, size);
  *attr_not_found = (errno == ENOATTR);
  return result;
#else
  *attr_not_found = true;
  return -1;
#endif
}

extern "C" JNIEXPORT void JNICALL
Java_com_google_devtools_build_lib_unix_NativePosixFiles_transfer(JNIEnv *env, jclass clazz, jint fd_in, jint fd_out) {
  // FreeBSD has a Linux-like copy_file_range API.
  // OpenBSD doesn't seem to have a zero-copy API except for sockets.
  // We could fork the logic here, but for now always do a manual copy.
  char buf[4096];
  ssize_t nread = 0;
  while ((nread = read(fd_in, buf, sizeof(buf))) > 0) {
    ssize_t nwritten = 0;
    while (nwritten < nread) {
      int n = write(fd_out, buf + nwritten, nread - nwritten);
      if (n < 0) {
        PostException(env, errno, "write");
        return;
      }
      nwritten += n;
    }
  }
  if (nread < 0) {
    PostException(env, errno, "read");
  }
}

int portable_push_disable_sleep() {
  // Currently not supported.
  // https://wiki.freebsd.org/SuspendResume
  return -1;
}

int portable_pop_disable_sleep() {
  // Currently not supported.
  // https://wiki.freebsd.org/SuspendResume
  return -1;
}

void portable_start_suspend_monitoring() {
  // Currently not implemented.
}

void portable_start_thermal_monitoring() {
  // Currently not implemented.
}

int portable_thermal_load() {
  // Currently not implemented.
  return 0;
}

void portable_start_system_load_advisory_monitoring() {
  // Currently not implemented.
}

int portable_system_load_advisory() {
  // Currently not implemented.
  return 0;
}

void portable_start_memory_pressure_monitoring() {
  // Currently not implemented.
}

MemoryPressureLevel portable_memory_pressure() {
  // Currently not implemented.
  return MemoryPressureLevelNormal;
}

void portable_start_disk_space_monitoring() {
  // Currently not implemented.
}

void portable_start_cpu_speed_monitoring() {
  // Currently not implemented.
}

int portable_cpu_speed() {
  // Currently not implemented.
  return -1;
}

}  // namespace blaze_jni
