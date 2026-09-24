# This is the project CMakeLists.txt file for the test subproject
cmake_minimum_required(VERSION 3.16)

# Include the components directory of the main application:
#
set(EXTRA_COMPONENT_DIRS "./components")

# Set the components to include the tests for.
# This can be overridden from CMake cache:
# - when invoking CMake directly: cmake -D TEST_COMPONENTS="xxxxx" ..
# - when using idf.py: idf.py -T xxxxx build
#
set(TEST_COMPONENTS "esp_idf_timeseries" CACHE STRING "List of components to test")

# Production memory layout (8 MB PSRAM, PSRAM task stacks) when built inside
# esp_node_firmware; see tools/qemu/production_memory.cmake there.
if(EXISTS "${CMAKE_CURRENT_LIST_DIR}/../../../tools/qemu/production_memory.cmake")
    include("${CMAKE_CURRENT_LIST_DIR}/../../../tools/qemu/production_memory.cmake")
endif()
include($ENV{IDF_PATH}/tools/cmake/project.cmake)
# "Trim" the build. Include the minimal set of components, main, and anything it depends on.
idf_build_set_property(MINIMAL_BUILD ON)
project(unit_test_test)