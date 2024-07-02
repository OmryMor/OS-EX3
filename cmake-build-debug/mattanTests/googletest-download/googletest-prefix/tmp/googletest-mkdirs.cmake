# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION 3.5)

file(MAKE_DIRECTORY
  "/Users/omry/Documents/GitHub/OS-EX3/cmake-build-debug/mattanTests/googletest-src"
  "/Users/omry/Documents/GitHub/OS-EX3/cmake-build-debug/mattanTests/googletest-build"
  "/Users/omry/Documents/GitHub/OS-EX3/cmake-build-debug/mattanTests/googletest-download/googletest-prefix"
  "/Users/omry/Documents/GitHub/OS-EX3/cmake-build-debug/mattanTests/googletest-download/googletest-prefix/tmp"
  "/Users/omry/Documents/GitHub/OS-EX3/cmake-build-debug/mattanTests/googletest-download/googletest-prefix/src/googletest-stamp"
  "/Users/omry/Documents/GitHub/OS-EX3/cmake-build-debug/mattanTests/googletest-download/googletest-prefix/src"
  "/Users/omry/Documents/GitHub/OS-EX3/cmake-build-debug/mattanTests/googletest-download/googletest-prefix/src/googletest-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/Users/omry/Documents/GitHub/OS-EX3/cmake-build-debug/mattanTests/googletest-download/googletest-prefix/src/googletest-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/Users/omry/Documents/GitHub/OS-EX3/cmake-build-debug/mattanTests/googletest-download/googletest-prefix/src/googletest-stamp${cfgdir}") # cfgdir has leading slash
endif()
