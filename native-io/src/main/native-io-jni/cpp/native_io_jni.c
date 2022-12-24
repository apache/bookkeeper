/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <org_apache_bookkeeper_common_util_nativeio_NativeIOJni.h>

#ifdef _WIN32

#define fsync(fd) fflush(fd)
#define strerror_r(errno,buf,len) strerror_s(buf,len,errno)

static ssize_t pread (int fd, void *buf, size_t count, off_t offset)
{
  ssize_t res;
  off_t ooffset;

  ooffset = lseek (fd, 0, SEEK_CUR);
  lseek (fd, offset, SEEK_SET);
  res = read (fd, buf, count);
  lseek (fd, ooffset, SEEK_SET);

  return res;
}

static ssize_t pwrite (int fd, void *buf, size_t count, off_t offset)
{
  ssize_t res;
  off_t ooffset;

  ooffset = lseek (fd, 0, SEEK_CUR);
  lseek (fd, offset, SEEK_SET);
  res = write (fd, buf, count);
  lseek (fd, ooffset, SEEK_SET);

  return res;
}

static int check_align(size_t align)
{
    for (size_t i = sizeof(void *); i != 0; i *= 2)
    if (align == i)
        return 0;
    return EINVAL;
}

int posix_memalign(void **ptr, size_t align, size_t size)
{
    if (check_align(align))
        return EINVAL;

    int saved_errno = errno;
    void *p = _aligned_malloc(size, align);
    if (p == NULL)
    {
        errno = saved_errno;
        return ENOMEM;
    }

    *ptr = p;
    return 0;
}

#endif

static void throwExceptionWithErrno(JNIEnv* env, const char* message) {
    char err_msg[1024];
    strerror_r(errno, err_msg, sizeof(err_msg));
    unsigned long size = strlen(message) + strlen(err_msg) + 10;
    char* str = malloc(size);
    snprintf(str, size, "%s: %s", message, err_msg);

    jstring javaMessage =  (*env)->NewStringUTF(env, str);
    free(str);

    jclass clazz =  (*env)->FindClass(env, "org/apache/bookkeeper/common/util/nativeio/NativeIOException");
    jmethodID ctorMethod =  (*env)->GetMethodID(env, clazz, "<init>", "(Ljava/lang/String;I)V");
    jobject myException =  (*env)->NewObject(env, clazz, ctorMethod, javaMessage, errno);
    (*env)->Throw(env, myException);
}

static void throwException(JNIEnv* env, const char* message) {
    (*env)->ThrowNew(env, (*env)->FindClass(env, "org/apache/bookkeeper/common/util/nativeio/NativeIOException"), message);
}

/*
 * Class:     org_apache_bookkeeper_common_util_nativeio_NativeIOJni
 * Method:    open
 * Signature: (Ljava/lang/String;II)I
 */
JNIEXPORT jint JNICALL
Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_open(
    JNIEnv *env, jclass clazz, jstring path, jint javaFlags, jint mode) {
    const char *cPath = (*env)->GetStringUTFChars(env, path, 0);

    int flags = 0;
    if (javaFlags & 0x01) {
        flags |= O_CREAT;
    }

    if (javaFlags & 0x02) {
        flags |= O_RDONLY;
    }

    if (javaFlags & 0x04) {
        flags |= O_WRONLY;
    }

    if (javaFlags & 0x08) {
        flags |= O_TRUNC;
    }

#ifdef __linux__
    if (javaFlags & 0x10) {
        flags |= O_DIRECT;
    }
#endif

#ifndef _WIN32
    if (javaFlags & 0x20) {
        flags |= O_DSYNC;
    }
#endif

    int fd = open(cPath, flags, mode);

    (*env)->ReleaseStringUTFChars(env, path, cPath);

    if (fd == -1) {
      throwExceptionWithErrno(env, "Failed to open file");
    }

    return fd;
}

/*
 * Class:     org_apache_bookkeeper_common_util_nativeio_NativeIOJni
 * Method:    fsync
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL
Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_fsync(JNIEnv * env,
                                                               jclass clazz,
                                                               jint fd) {
    int res = fsync(fd);

    if (res == -1) {
      throwExceptionWithErrno(env, "Failed to fsync");
    }

    return res;
}

/*
 * Class:     org_apache_bookkeeper_common_util_nativeio_NativeIOJni
 * Method:    fallocate
 * Signature: (IIJJ)I
 */
JNIEXPORT jint JNICALL
Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_fallocate(
    JNIEnv* env, jclass clazz,
    jint fd, jint mode, jlong offset, jlong len) {
#ifdef __linux__
    int res = fallocate(fd, mode, offset, len);
    if (res == -1) {
        throwExceptionWithErrno(env, "Failed to fallocate");
    }
    return res;
#else
    throwException(env, "fallocate is not available");
    return -1;
#endif
}

/*
 * Class:     org_apache_bookkeeper_common_util_nativeio_NativeIOJni
 * Method:    lseek
 * Signature: (IJI)J
 */
JNIEXPORT jlong JNICALL
Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_lseek(
        JNIEnv* env, jclass clazz,
        jint fd, jlong offset, jint whence) {
    int res = lseek(fd, offset, whence);

    if (res == -1) {
        throwExceptionWithErrno(env, "Failed to lseek");
    }

    return res;
}

/*
 * Class:     org_apache_bookkeeper_common_util_nativeio_NativeIOJni
 * Method:    close
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL
Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_close(JNIEnv* env, jclass clazz,
                                                               jint fd) {
    int res = close(fd);

    if (res == -1) {
         throwExceptionWithErrno(env, "Failed to close file");
    }

    return res;
}

/*
 * Class:     org_apache_bookkeeper_common_util_nativeio_NativeIOJni
 * Method:    pwrite
 * Signature: (IJIJ)I
 */
JNIEXPORT jint JNICALL Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_pwrite
    (JNIEnv* env, jclass clazz, jint fd, jlong pointer, jint count, jlong offset) {
    int res = pwrite(fd, (const void*) pointer, count, offset);

    if (res == -1) {
      throwExceptionWithErrno(env, "Failed to write on file");
    }

    return res;
}

/*
 * Class:     org_apache_bookkeeper_common_util_nativeio_NativeIOJni
 * Method:    posix_memalign
 * Signature: (II)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_posix_1memalign
    (JNIEnv* env, jclass clazz, jint alignment, jint size) {
    void* ptr;
    int res = posix_memalign(&ptr, alignment, size);

    if (res != 0) {
      throwExceptionWithErrno(env, "Failed to allocate aligned memory");
    }

    return (jlong) ptr;
}

/*
 * Class:     org_apache_bookkeeper_common_util_nativeio_NativeIOJni
 * Method:    pread
 * Signature: (IJJJ)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_pread
    (JNIEnv * env, jclass clazz, jint fd, jlong pointer, jlong size, jlong offset) {

    long res =  pread(fd, (void*) pointer, size, offset);

    if (res == -1) {
      throwExceptionWithErrno(env, "Failed to read from file");
    }

    return res;
}

/*
 * Class:     org_apache_bookkeeper_common_util_nativeio_NativeIOJni
 * Method:    free
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_free
  (JNIEnv * env, jclass clazz, jlong pointer) {
     free((const void*) pointer);
}

