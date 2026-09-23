#pragma once
#include "FreeRTOS.h"
#define xSemaphoreTake(s, t) (pdTRUE)
#define xSemaphoreGive(s) (pdTRUE)
