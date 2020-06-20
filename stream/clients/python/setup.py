# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import io
import os

import setuptools

# Package metadata.

name = 'apache-bookkeeper-client'
description = 'Apache BookKeeper client library'
version = '4.11.0'
# Should be one of:
# 'Development Status :: 3 - Alpha'
# 'Development Status :: 4 - Beta'
# 'Development Status :: 5 - Production/Stable'
release_status = 'Development Status :: 3 - Alpha'
dependencies = [
    'protobuf>=3.0.0',
    'requests<3.0.0dev,>=2.18.0',
    'setuptools>=34.0.0',
    'six>=1.10.0',
    'pytz',
    'futures>=3.2.0;python_version<"3.2"',
    'grpcio<1.26.0,>=1.8.2',
    'pymmh3>=0.0.3'
]
extras = {
}

# Setup boilerplate below this line.

package_root = os.path.abspath(os.path.dirname(__file__))

readme_filename = os.path.join(package_root, 'README.rst')
with io.open(readme_filename, encoding='utf-8') as readme_file:
    readme = readme_file.read()

# Only include packages under the 'bookkeeper' namespace. Do not include tests,
# benchmarks, etc.
packages = [
    package for package in setuptools.find_packages()
    if package.startswith('bookkeeper')]

# Determine which namespaces are needed.
namespaces = ['bookkeeper']

setuptools.setup(
    name=name,
    version=version,
    description=description,
    long_description=readme,
    author='Apache BookKeeper',
    author_email='dev@bookkeeper.apache.org',
    license='Apache 2.0',
    url='https://github.com/apache/bookkeeper/tree/master/stream/clients/python',
    classifiers=[
        release_status,
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Operating System :: OS Independent',
        'Topic :: Internet',
    ],
    platforms='Posix; MacOS X; Windows',
    packages=packages,
    namespace_packages=namespaces,
    install_requires=dependencies,
    extras_require=extras,
    include_package_data=True,
    zip_safe=False,
)
