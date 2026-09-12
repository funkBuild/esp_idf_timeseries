# The IDF 5.5.5 soc/project_include.cmake applies chip toolchain flags on Linux.
# Linux's soc component has no sources; expose its headers without that hook.
idf_component_register(INCLUDE_DIRS
    "$ENV{IDF_PATH}/components/soc/include"
    "$ENV{IDF_PATH}/components/soc/linux/include")
