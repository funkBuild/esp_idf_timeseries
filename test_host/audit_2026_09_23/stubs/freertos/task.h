#pragma once
#include "FreeRTOS.h"
// timeseries_epoch.c (linked since the page cache retires regions): the audit
// runs on one thread, so a constant task handle and no-op delays suffice.
static inline TaskHandle_t xTaskGetCurrentTaskHandle(void) { return (TaskHandle_t)1; }
static inline void vTaskDelay(TickType_t t) { (void)t; }
static inline TickType_t xTaskGetTickCount(void) { return 0; }
#ifndef pdMS_TO_TICKS
#define pdMS_TO_TICKS(ms) ((TickType_t)(ms))
#endif
