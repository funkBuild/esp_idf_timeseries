set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(TSDB_SOURCE_ROOT "${CMAKE_CURRENT_LIST_DIR}")
# Host regression runner can compare the same tests against a saved source tree.
if(IDF_TARGET STREQUAL "linux" AND TIMESERIES_AUDIT_SOURCE_DIR)
    set(TSDB_SOURCE_ROOT "${TIMESERIES_AUDIT_SOURCE_DIR}")
endif()
file(GLOB GORILLA_SOURCES "${TSDB_SOURCE_ROOT}/src/gorilla/*.c")
file(GLOB MAIN_SOURCES "${TSDB_SOURCE_ROOT}/src/*.c")
file(GLOB ALP_SOURCES "${TSDB_SOURCE_ROOT}/src/alp/*.c")

set(SOURCES ${GORILLA_SOURCES} ${MAIN_SOURCES} ${ALP_SOURCES})

idf_component_register(SRCS ${SOURCES}
                       REQUIRES esp_partition mbedtls esp_timer freertos espressif__zlib
                       INCLUDE_DIRS "${TSDB_SOURCE_ROOT}/include" "${TSDB_SOURCE_ROOT}/src/alp")

if(IDF_TARGET STREQUAL "linux")
    target_link_libraries(${COMPONENT_LIB} PUBLIC m)
endif()
