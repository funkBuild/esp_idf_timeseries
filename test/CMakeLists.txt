idf_component_register(SRC_DIRS "."
                       INCLUDE_DIRS "."
                       REQUIRES unity cmock esp_idf_timeseries mbedtls)

target_link_options(${COMPONENT_LIB} INTERFACE "-Wl,--wrap=timeseries_page_stream_writer_init"
    "-Wl,--wrap=vTaskDelay" "-Wl,--wrap=esp_partition_erase_range"
    "-Wl,--wrap=tsdb_flash_write_byte"
    "-Wl,--wrap=esp_partition_write"
    "-Wl,--wrap=tsdb_snapshot_compare_and_swap" "-Wl,--wrap=tsdb_snapshot_release"
    "-Wl,--wrap=timeseries_fielddata_iterator_deinit")

target_link_options(${COMPONENT_LIB} INTERFACE
    "-Wl,--wrap=timeseries_query_execute" "-Wl,--wrap=tsdb_point_order_init"
    "-Wl,--wrap=timeseries_compact_all_levels" "-Wl,--wrap=timeseries_compact_all_levels_force"
    "-Wl,--wrap=timeseries_expiration_run" "-Wl,--wrap=xTaskCreatePinnedToCore")

if(IDF_TARGET STREQUAL "linux")
    target_link_options(${COMPONENT_LIB} INTERFACE
        "-Wl,--wrap=timeseries_page_stream_writer_finalize_timestamp"
        "-Wl,--wrap=timeseries_points_iterator_next_value" "-Wl,--wrap=tsdb_pagecache_batch_sort" "-Wl,--wrap=malloc" "-Wl,--wrap=realloc" "-Wl,--wrap=calloc" "-Wl,--wrap=esp_partition_read"
        # Linux-simulator lock guards (timeseries_host_sim_guards.c)
        "-Wl,--wrap=free" "-Wl,--wrap=pthread_join"
        "-Wl,--wrap=esp_log_impl_lock" "-Wl,--wrap=esp_log_impl_lock_timeout" "-Wl,--wrap=esp_log_impl_unlock")
endif()
