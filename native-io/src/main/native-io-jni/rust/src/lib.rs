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

//! Native I/O JNI library for Apache BookKeeper
//!
//! This library provides POSIX file I/O operations via JNI for high-performance direct I/O.
//! It is a Rust implementation of the original C native_io_jni.c

use jni::objects::{JClass, JString, JThrowable};
use jni::sys::{jint, jlong};
use jni::JNIEnv;
use std::ffi::{c_void, CString};
use std::ptr;

// Java flags (from NativeIO.java)
const O_CREAT: jint = 0x01;
const O_RDONLY: jint = 0x02;
const O_WRONLY: jint = 0x04;
const O_TRUNC: jint = 0x08;
const O_DIRECT: jint = 0x10;
const O_DSYNC: jint = 0x20;

// ============================================================================
// Helper functions for exception throwing (equivalent to C functions)
// ============================================================================

/// Throws a NativeIOException with the current errno
fn throw_exception_with_errno(env: &mut JNIEnv, message: &str) {
    let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);

    throw_exception_with_error_code(env, message, errno);
}

fn throw_exception_with_error_code(env: &mut JNIEnv, message: &str, error_code: jint) {
    let err_msg = std::io::Error::from_raw_os_error(error_code).to_string();
    let full_msg = format!("{}: {}", message, err_msg);

    if let Ok(class) =
        env.find_class("org/apache/bookkeeper/common/util/nativeio/NativeIOException")
    {
        if let Ok(msg) = env.new_string(&full_msg) {
            let result = env.new_object(
                class,
                "(Ljava/lang/String;I)V",
                &[(&msg).into(), error_code.into()],
            );
            if let Ok(exception) = result {
                let _ = env.throw(JThrowable::from(exception));
            }
        }
    }
}

/// Throws a NativeIOException without errno
fn throw_exception(env: &mut JNIEnv, message: &str) {
    if let Ok(class) =
        env.find_class("org/apache/bookkeeper/common/util/nativeio/NativeIOException")
    {
        let _ = env.throw_new(class, message);
    }
}

// ============================================================================
// JNI exported functions
// ============================================================================

/// Class: org.apache.bookkeeper.common.util.nativeio.NativeIOJni
/// Method: open
/// Signature: (Ljava/lang/String;II)I
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_open(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
    java_flags: jint,
    mode: jint,
) -> jint {
    // Convert Java flags to POSIX flags (same logic as C code)
    let mut flags: i32 = 0;

    if java_flags & O_CREAT != 0 {
        flags |= libc::O_CREAT;
    }
    if java_flags & O_RDONLY != 0 {
        flags |= libc::O_RDONLY;
    }
    if java_flags & O_WRONLY != 0 {
        flags |= libc::O_WRONLY;
    }
    if java_flags & O_TRUNC != 0 {
        flags |= libc::O_TRUNC;
    }

    // O_DIRECT is Linux only
    #[cfg(target_os = "linux")]
    if java_flags & O_DIRECT != 0 {
        flags |= libc::O_DIRECT;
    }

    // O_DSYNC is not available on Windows
    #[cfg(not(target_os = "windows"))]
    if java_flags & O_DSYNC != 0 {
        flags |= libc::O_DSYNC;
    }

    // Get the path string
    let path_str: String = match env.get_string(&path) {
        Ok(s) => s.into(),
        Err(_) => {
            throw_exception(&mut env, "Failed to get path string");
            return -1;
        }
    };

    let path_cstr = match CString::new(path_str) {
        Ok(path) => path,
        Err(_) => {
            throw_exception(&mut env, "Path contains interior NUL byte");
            return -1;
        }
    };

    let fd = unsafe { libc::open(path_cstr.as_ptr(), flags, mode as libc::c_uint) };

    if fd == -1 {
        throw_exception_with_errno(&mut env, "Failed to open file");
    }

    fd
}

/// Class: org.apache.bookkeeper.common.util.nativeio.NativeIOJni
/// Method: fsync
/// Signature: (I)I
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_fsync(
    mut env: JNIEnv,
    _class: JClass,
    fd: jint,
) -> jint {
    let res = unsafe { libc::fsync(fd) };

    if res == -1 {
        throw_exception_with_errno(&mut env, "Failed to fsync");
    }

    res as jint
}

/// Class: org.apache.bookkeeper.common.util.nativeio.NativeIOJni
/// Method: fallocate
/// Signature: (IIJJ)I
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_fallocate(
    mut env: JNIEnv,
    _class: JClass,
    fd: jint,
    mode: jint,
    offset: jlong,
    len: jlong,
) -> jint {
    // fallocate is Linux only
    #[cfg(target_os = "linux")]
    {
        let res = unsafe { libc::fallocate(fd, mode, offset, len) };

        if res == -1 {
            throw_exception_with_errno(&mut env, "Failed to fallocate");
        }

        res
    }

    #[cfg(not(target_os = "linux"))]
    {
        throw_exception(&mut env, "fallocate is not available");
        -1
    }
}

/// Class: org.apache.bookkeeper.common.util.nativeio.NativeIOJni
/// Method: posix_fadvise
/// Signature: (IJJI)I
/// Note: In JNI, underscores in method names are escaped as _1
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_posix_1fadvise(
    mut env: JNIEnv,
    _class: JClass,
    fd: jint,
    offset: jlong,
    len: jlong,
    flag: jint,
) -> jint {
    // posix_fadvise is Linux only
    #[cfg(target_os = "linux")]
    {
        let res = unsafe { libc::posix_fadvise(fd, offset, len, flag) };

        if res != 0 {
            throw_exception_with_error_code(&mut env, "Failed to posix_fadvise", res);
        }

        res
    }

    #[cfg(not(target_os = "linux"))]
    {
        throw_exception(&mut env, "posix_fadvise is not available");
        -1
    }
}

/// Class: org.apache.bookkeeper.common.util.nativeio.NativeIOJni
/// Method: lseek
/// Signature: (IJI)J
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_lseek(
    mut env: JNIEnv,
    _class: JClass,
    fd: jint,
    offset: jlong,
    whence: jint,
) -> jlong {
    let res = unsafe { libc::lseek(fd, offset, whence) };

    if res == -1 {
        throw_exception_with_errno(&mut env, "Failed to lseek");
    }

    res as jlong
}

/// Class: org.apache.bookkeeper.common.util.nativeio.NativeIOJni
/// Method: close
/// Signature: (I)I
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_close(
    mut env: JNIEnv,
    _class: JClass,
    fd: jint,
) -> jint {
    let res = unsafe { libc::close(fd) };

    if res == -1 {
        throw_exception_with_errno(&mut env, "Failed to close file");
    }

    res as jint
}

/// Class: org.apache.bookkeeper.common.util.nativeio.NativeIOJni
/// Method: pwrite
/// Signature: (IJIJ)I
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_pwrite(
    mut env: JNIEnv,
    _class: JClass,
    fd: jint,
    pointer: jlong,
    count: jint,
    offset: jlong,
) -> jint {
    let res = unsafe { libc::pwrite(fd, pointer as *const c_void, count as usize, offset) };

    if res == -1 {
        throw_exception_with_errno(&mut env, "Failed to write on file");
    }

    res as jint
}

/// Class: org.apache.bookkeeper.common.util.nativeio.NativeIOJni
/// Method: posix_memalign
/// Signature: (II)J
/// Note: In JNI, underscores in method names are escaped as _1
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_posix_1memalign(
    mut env: JNIEnv,
    _class: JClass,
    alignment: jint,
    size: jint,
) -> jlong {
    let mut ptr: *mut c_void = ptr::null_mut();

    let res = unsafe { libc::posix_memalign(&mut ptr, alignment as usize, size as usize) };

    if res != 0 {
        throw_exception_with_error_code(&mut env, "Failed to allocate aligned memory", res);
        return 0;
    }

    ptr as jlong
}

/// Class: org.apache.bookkeeper.common.util.nativeio.NativeIOJni
/// Method: pread
/// Signature: (IJJJ)J
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_pread(
    mut env: JNIEnv,
    _class: JClass,
    fd: jint,
    pointer: jlong,
    size: jlong,
    offset: jlong,
) -> jlong {
    let res = unsafe { libc::pread(fd, pointer as *mut c_void, size as usize, offset) };

    if res == -1 {
        throw_exception_with_errno(&mut env, "Failed to read from file");
    }

    res as jlong
}

/// Class: org.apache.bookkeeper.common.util.nativeio.NativeIOJni
/// Method: free
/// Signature: (J)V
#[no_mangle]
pub extern "system" fn Java_org_apache_bookkeeper_common_util_nativeio_NativeIOJni_free(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    unsafe {
        libc::free(pointer as *mut c_void);
    }
}
