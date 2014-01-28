/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <jni.h>

#include <errno.h>
#include <fcntl.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <asm-x86_64/unistd.h>
#include "config.h"

#if defined(HAVE_SYNC_FILE_RANGE)
#  define my_sync_file_range sync_file_range
#elif defined(__NR_sync_file_range)
// RHEL 5 kernels have sync_file_range support, but the glibc
// included does not have the library function. We can
// still call it directly, and if it's not supported by the
// kernel, we'd get ENOSYS. See RedHat Bugzilla #518581
static int manual_sync_file_range (int fd, __off64_t from, __off64_t to, unsigned int flags)
{
#ifdef __x86_64__
  return syscall( __NR_sync_file_range, fd, from, to, flags);
#else
  return syscall (__NR_sync_file_range, fd,
    __LONG_LONG_PAIR ((long) (from >> 32), (long) from),
    __LONG_LONG_PAIR ((long) (to >> 32), (long) to),
    flags);
#endif
}
#define my_sync_file_range manual_sync_file_range
#endif

/**
 * public static native void sync_file_range(
 *   int fd, long offset, long len, int flags);
 *
 * The "00024" in the function name is an artifact of how JNI encodes
 * special characters. U+0024 is '$'.
 */
JNIEXPORT jint JNICALL
Java_org_apache_bookkeeper_util_NativeIO_sync_1file_1range(
  JNIEnv *env, jclass clazz,
  jint fd, jlong offset, jlong len, jint flags)
{
#ifndef my_sync_file_range
  errno = ENOSYS;
  return -1;
#else
  return my_sync_file_range(fd, (off_t)offset, (off_t)len, flags);
#endif
}

#if defined(HAVE_FALLOCATE)
#  define my_fallocate fallocate
#elif defined(__NR_fallocate)
static int manual_fallocate (int fd, int mode, __off64_t from, __off64_t to)
{
#ifdef __x86_64__
  return syscall( __NR_fallocate, fd, mode, from, to);
#else
  return syscall (__NR_fallocate, fd, mode,
    __LONG_LONG_PAIR ((long) (from >> 32), (long) from),
    __LONG_LONG_PAIR ((long) (to >> 32), (long) to));
#endif
}
#define my_fallocate manual_fallocate
#endif

JNIEXPORT jint JNICALL
Java_org_apache_bookkeeper_util_NativeIO_fallocate(
  JNIEnv *env, jclass clazz,
  jint fd, jint mode, jlong offset, jlong len)
{
#ifndef my_fallocate
  errno = ENOSYS;
  return -1;
#else
  return my_fallocate(fd, mode, (off_t)offset, (off_t)len);
#endif
}

JNIEXPORT jint JNICALL
Java_org_apache_bookkeeper_util_NativeIO_posix_1fadvise(
  JNIEnv *env, jclass clazz,
  jint fd, jlong offset, jlong len, jint flags)
{
#ifndef HAVE_POSIX_FADVISE
  errno = ENOSYS;
  return -1;
#else
  return posix_fadvise(fd, (off_t)offset, (off_t)len, flags);
#endif
}

JNIEXPORT jint JNICALL
Java_org_apache_bookkeeper_util_NativeIO_posix_1fallocate(
  JNIEnv *env, jclass clazz,
  jint fd, jlong offset, jlong len)
{
#ifndef HAVE_POSIX_FALLOCATE
  errno = ENOSYS;
  return -1;
#else
  return posix_fallocate(fd, (off_t)offset, (off_t)len);
#endif
}
