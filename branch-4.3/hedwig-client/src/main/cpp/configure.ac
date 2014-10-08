#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

AC_INIT([Hedwig C++ Client], [0.1], [bookkeeper-dev@zookeeper.apache.org], [hedwig-cpp], [http://zookeeper.apache.org/bookkeeper/])

AC_PREREQ([2.59])
AM_INIT_AUTOMAKE([1.9 no-define foreign])
AC_CONFIG_HEADERS([config.h])
AC_PROG_CXX
AC_LANG([C++])
AC_CONFIG_FILES([Makefile lib/Makefile test/Makefile hedwig-0.1.pc])
AC_PROG_LIBTOOL
AC_CONFIG_MACRO_DIR([m4])
PKG_CHECK_MODULES([DEPS], [liblog4cxx protobuf openssl])

GTEST_LIB_CHECK([1.5.0], [AC_MSG_RESULT([GoogleTest found, Tests Enabled])],
                         [AC_MSG_WARN([GoogleTest not found, Tests disabled])])

AX_BOOST_BASE
AX_BOOST_ASIO	  
AX_BOOST_THREAD

AC_CHECK_HEADER(tr1/memory, [AC_MSG_RESULT([Found builtin TR1 library])],[
			     AC_CHECK_HEADER(boost/tr1/memory.hpp, 
			     [AC_DEFINE(USE_BOOST_TR1, [], [Found Boost TR1 library])],
			     [AC_MSG_ERROR([TR1 not found, builtin TR1 or boost TR1 is required])])])

DX_HTML_FEATURE(ON)
DX_INIT_DOXYGEN(hedwig-c++, c-doc.Doxyfile, doc)

CXXFLAGS="$CXXFLAGS -Wall"

AC_OUTPUT

