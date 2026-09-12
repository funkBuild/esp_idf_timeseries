idf_component_register(SRCS "hello_world_main.c"
                    PRIV_REQUIRES spi_flash esp_idf_timeseries unity
                    INCLUDE_DIRS "")

if(DEFINED TIMESERIES_TEST_TAG)
    target_compile_definitions(${COMPONENT_LIB} PRIVATE
        TIMESERIES_TEST_TAG="${TIMESERIES_TEST_TAG}")
endif()
