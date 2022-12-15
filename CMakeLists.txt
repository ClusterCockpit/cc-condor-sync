project(htcondor-cc-sync-plugin LANGUAGES CXX)
cmake_minimum_required(VERSION 3.12)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT DEFINED CONDOR_SRC OR NOT EXISTS ${CONDOR_SRC})
    message(FATAL_ERROR "No valid path to HTCondor source directory provided! Provide -DCONDOR_SRC=..")
else()
    set(CONDOR_SRC ${CONDOR_SRC} CACHE STRING "Path to HTCondor source directory")
    set(CONDOR_SRC_SRC ${CONDOR_SRC}/src/)
endif()

if(NOT DEFINED CONDOR_BUILD OR NOT EXISTS ${CONDOR_BUILD})
    message(FATAL_ERROR "No valid path to HTCondor build directory provided! Provide -DCONDOR_BUILD=..")
else()
    set(CONDOR_BUILD ${CONDOR_SRC} CACHE STRING "Path to HTCondor build directory")
    set(CONDOR_BUILD_SRC ${CONDOR_BUILD}/src/)
endif()

add_subdirectory(curlpp)
set_property(TARGET curlpp_static PROPERTY POSITION_INDEPENDENT_CODE ON)

add_library(htcondor_cc_sync_plugin MODULE src/htcondor_cc_sync_plugin.cpp)

target_include_directories(htcondor_cc_sync_plugin PRIVATE
    ${CONDOR_SRC_SRC}
    ${CONDOR_SRC_SRC}condor_includes
    ${CONDOR_SRC_SRC}condor_utils
    ${CONDOR_SRC_SRC}classad
    ${CONDOR_SRC_SRC}safefile
    ${CONDOR_BUILD_SRC}condor_includes
    ${CONDOR_BUILD_SRC}condor_utils
    ${CONDOR_BUILD_SRC}safefile
    ${CURLPP_INCLUDE_DIRS})

target_link_libraries(htcondor_cc_sync_plugin PRIVATE curlpp_static)
target_link_options(htcondor_cc_sync_plugin PRIVATE -rdynamic)
set_target_properties(htcondor_cc_sync_plugin PROPERTIES ENABLE_EXPORTS 1)
target_include_directories(htcondor_cc_sync_plugin PRIVATE ${CMAKE_SOURCE_DIR}/curlpp/include)

target_compile_options(htcondor_cc_sync_plugin PRIVATE
    -DCONDOR_VERSION=\"10.0.0\" -DENABLE_STATE_DUMP -DGLIBC219=GLIBC219 -DGLIBC=GLIBC -DHAVE_CONFIG_H -DLINUX=\"LINUX_3.13.0-30-GENERIC\" -DPLATFORM=\"X86_64-Linux_17\" -DPRE_RELEASE_STR=\"\" -DWITH_OPENSSL -DX86_64=X86_64 -Dcondorapi_shared_EXPORTS -DWITH_IPV6)
