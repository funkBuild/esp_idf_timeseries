idf_component_register(SRCS "tsdb_churn.c" PRIV_REQUIRES spi_flash esp_partition esp_idf_timeseries esp_timer)
target_link_libraries(${COMPONENT_LIB} INTERFACE "-Wl,--wrap=esp_partition_write"
                      "-Wl,--wrap=tsdb_snapshot_acquire_current" "-Wl,--wrap=timeseries_page_cache_iterator_deinit")
