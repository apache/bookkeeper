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

//! Build script for native-io JNI library
//! Sets the install_name for macOS dylib to match Java's expected library name

fn main() {
    let target = std::env::var("TARGET").unwrap_or_default();

    // On macOS, set the install_name to libnative-io.jnilib
    // This allows the library to be loaded with System.loadLibrary("native-io")
    if target.contains("darwin") {
        println!("cargo:rustc-cdylib-link-arg=-install_name");
        println!("cargo:rustc-cdylib-link-arg=libnative-io.jnilib");
    }

    // Rebuild if the environment changes
    println!("cargo:rerun-if-env-changed=TARGET");
}
