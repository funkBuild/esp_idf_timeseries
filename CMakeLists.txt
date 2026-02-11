set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

file(GLOB GORILLA_SOURCES "src/gorilla/*.c")
file(GLOB MAIN_SOURCES "src/*.c")

set(SOURCES ${GORILLA_SOURCES} ${MAIN_SOURCES})

idf_component_register(SRCS ${SOURCES}
                       REQUIRES esp_partition mbedtls esp_timer freertos
                       INCLUDE_DIRS include)

