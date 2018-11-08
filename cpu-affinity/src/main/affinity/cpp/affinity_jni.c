/**
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

// Use different error code to differentiate non-implemented error
static const int NOT_IMPLEMENTED = -2;

#ifdef __linux__
#define _GNU_SOURCE
#include <sched.h>
#include <unistd.h>
#include <sys/syscall.h>

static int set_affinity(int cpuid) {
    cpu_set_t cpus;
    CPU_ZERO(&cpus);
    CPU_SET((size_t)cpuid, &cpus);
    int threadId = (int)syscall(SYS_gettid);
    return sched_setaffinity(threadId, sizeof(cpu_set_t), &cpus);
}

static const int IS_AVAILABLE = 1;

#else

static int set_affinity(int cpuid) { return NOT_IMPLEMENTED; }

static const int IS_AVAILABLE = 0;

#endif

#include <unistd.h>
#include <sys/types.h>
#include <errno.h>
#include <string.h>

#include <org_apache_bookkeeper_common_util_affinity_impl_CpuAffinityJni.h>

/*
 * Class:     org_apache_bookkeeper_common_util_affinity_impl_CpuAffinityJni
 * Method:    isRoot
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL
Java_org_apache_bookkeeper_common_util_affinity_impl_CpuAffinityJni_isRoot(JNIEnv *env, jclass cls) {
    return getuid() == 0;
}

/*
 * Class:     org_apache_bookkeeper_common_util_affinity_impl_CpuAffinityJni
 * Method:    isAvailable
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL
Java_org_apache_bookkeeper_common_util_affinity_impl_CpuAffinityJni_isAvailable(JNIEnv *env, jclass cls) {
    return IS_AVAILABLE == 1;
}

/*
 * Class:     org_apache_bookkeeper_common_util_affinity_impl_CpuAffinityJni
 * Method:    setAffinity
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_org_apache_bookkeeper_common_util_affinity_impl_CpuAffinityJni_setAffinity(
    JNIEnv *env, jclass cls, jint cpuid) {
    int res = set_affinity(cpuid);

    if (res == 0) {
        // Success
        return;
    } else if (res == NOT_IMPLEMENTED) {
        (*env)->ThrowNew(env, (*env)->FindClass(env, "java/lang/Exception"), "CPU affinity not implemented");
    } else {
        // Error in sched_setaffinity, get message from errno
        char buffer[1024];
        strerror_r(errno, buffer, sizeof(buffer));
        (*env)->ThrowNew(env, (*env)->FindClass(env, "java/lang/Exception"), buffer);
    }
}
