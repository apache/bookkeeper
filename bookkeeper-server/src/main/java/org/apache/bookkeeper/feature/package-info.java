/*
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
 */

/**
 * A <a href="https://en.wikipedia.org/wiki/Feature_toggle">feature-flag</a> system
 * that is used to proportionally control what features are enabled for the system.
 *
 * <p>In other words, it is a way of altering the control in a system without restarting it.
 * It can be used during all stages of developement, its most visible use case is on production.
 * For instance, during a production release, you can enable or disable individual features,
 * control the data flow through the system, thereby minimizing risk of system failures
 * in real time.
 *
 * <p>The <i>feature provider</i> interface is pluggable and easy to integrate with
 * any configuration management system.
 */
package org.apache.bookkeeper.feature;