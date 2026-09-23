idf_component_register(SRCS "tsdb_churn.c" "tsdb_review_phases.c" PRIV_REQUIRES spi_flash esp_partition esp_idf_timeseries esp_timer mbedtls)
target_link_libraries(${COMPONENT_LIB} INTERFACE "-Wl,--wrap=esp_partition_write"
                      "-Wl,--wrap=tsdb_snapshot_acquire_current" "-Wl,--wrap=timeseries_page_cache_iterator_deinit"
                      "-Wl,--wrap=pthread_join" "-Wl,--wrap=malloc" "-Wl,--wrap=calloc" "-Wl,--wrap=realloc"
                      "-Wl,--wrap=aligned_alloc"
                      "-Wl,--wrap=esp_log_impl_lock" "-Wl,--wrap=esp_log_impl_lock_timeout" "-Wl,--wrap=esp_log_impl_unlock"
                      "-Wl,--wrap=timeseries_page_stream_writer_init" "-Wl,--wrap=timeseries_page_stream_writer_finalize"
                      "-Wl,--wrap=tsdb_lookup_series_type_cached" "-Wl,--wrap=timeseries_entity_iterator_init"
                      "-Wl,--wrap=timeseries_page_cache_iterator_next"
                      "-Wl,--wrap=timeseries_fielddata_iterator_init_with_buffer"
                      "-Wl,--wrap=tsdb_flash_write_byte" "-Wl,--wrap=free" "-Wl,--wrap=strdup" "-Wl,--wrap=strndup")
