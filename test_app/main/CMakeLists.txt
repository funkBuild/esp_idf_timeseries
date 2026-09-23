set(test_requires spi_flash esp_idf_timeseries unity)
if(NOT IDF_TARGET STREQUAL "linux")
    list(APPEND test_requires esp_psram)
endif()

idf_component_register(SRCS "hello_world_main.c"
                    PRIV_REQUIRES ${test_requires}
                    INCLUDE_DIRS "")

if(DEFINED TIMESERIES_TEST_TAG)
    target_compile_definitions(${COMPONENT_LIB} PRIVATE
        TIMESERIES_TEST_TAG="${TIMESERIES_TEST_TAG}")
endif()
