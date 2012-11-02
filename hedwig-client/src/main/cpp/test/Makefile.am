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

if HAVE_GTEST
bin_PROGRAMS = hedwigtest
hedwigtest_SOURCES = main.cpp utiltest.cpp publishtest.cpp subscribetest.cpp pubsubtest.cpp messageboundtest.cpp messagefiltertest.cpp throttledeliverytest.cpp multiplextest.cpp
hedwigtest_CPPFLAGS = -I$(top_srcdir)/inc $(DEPS_CFLAGS) $(GTEST_CPPFLAGS) $(BOOST_CPPFLAGS)
hedwigtest_CXXFLAGS = $(GTEST_CXXFLAGS)
hedwigtest_LDADD = $(DEPS_LIBS) $(GTEST_LIBS) -L$(top_builddir)/lib -lhedwig01
hedwigtest_LDFLAGS = -no-undefined $(BOOST_ASIO_LIB) $(BOOST_LDFLAGS) $(BOOST_THREAD_LIB) $(GTEST_LDFLAGS)

check: hedwigtest
	bash ../scripts/tester.sh all

simplesslcheck: hedwigtest
	bash ../scripts/tester.sh ssl-simple-test

simplecheck: hedwigtest
	bash ../scripts/tester.sh simple-test

multiplexsslcheck: hedwigtest
	bash ../scripts/tester.sh ssl-multiplex-test

multiplexcheck: hedwigtest
	bash ../scripts/tester.sh multiplex-test

else
check:
	@echo "\n\nYou haven't configured with gtest. Run the ./configure command with --enable-gtest=<path_to_gtest>"
	@echo "i.e. ./configure --enable-gtest=/home/user/src/gtest-1.6.0"
	@echo "See the README for more info\n\n\b"
endif
