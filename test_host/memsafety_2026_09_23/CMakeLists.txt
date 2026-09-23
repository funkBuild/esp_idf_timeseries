cmake_minimum_required(VERSION 3.16)
# memsafety 2026-09-23 (PROTO): seeded multi-task churn of the REAL
# esp_idf_timeseries sources on the ESP-IDF linux target (FreeRTOS tasks are
# pthreads, so the tasks genuinely run concurrently). Built by run_tests.sh.
set(_REPO "${CMAKE_CURRENT_LIST_DIR}/../../../..")
set(EXTRA_COMPONENT_DIRS
    "${_REPO}/components/esp_idf_timeseries/host_tests/linux_overlay"
    "${_REPO}/components/esp_idf_timeseries/test_app/components"
    "${_REPO}/tools/host_test/linux_overlay"
    "${_REPO}/tools/host_test/esp_idf_timeseries_test_app/managed_components"
)
include("${_REPO}/tools/host_test/linux_toolchain_shim.cmake")
idf_build_set_property(C_COMPILE_OPTIONS "SHELL:-include host_soc_caps.h" APPEND)
idf_build_set_property(C_COMPILE_OPTIONS "-I${_REPO}/tools/host_test/linux_overlay/host_soc/include" APPEND)
idf_build_set_property(C_COMPILE_OPTIONS "SHELL:-include esp_bit_defs.h" APPEND)
idf_build_set_property(C_COMPILE_OPTIONS "SHELL:-include esp_system.h" APPEND)
idf_build_set_property(C_COMPILE_OPTIONS "-Wno-error=format-truncation" APPEND)
idf_build_set_property(C_COMPILE_OPTIONS "-fgnu89-inline" APPEND)
if(DEFINED ENV{HOST_SAN_C} AND NOT "$ENV{HOST_SAN_C}" STREQUAL "")
    separate_arguments(_san_c UNIX_COMMAND "$ENV{HOST_SAN_C}")
    foreach(_f IN LISTS _san_c)
        idf_build_set_property(COMPILE_OPTIONS "${_f}" APPEND)
        idf_build_set_property(LINK_OPTIONS    "${_f}" APPEND)
    endforeach()
endif()
idf_build_set_property(MINIMAL_BUILD ON)
idf_build_set_property(COMPILE_DEFINITIONS "CONFIG_MMU_PAGE_SIZE=65536" APPEND)
set(COMPONENTS main esp_idf_timeseries)
project(tsdb_memsafety_churn)
