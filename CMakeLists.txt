cmake_minimum_required(VERSION 3.0.0 FATAL_ERROR)

project(bench CXX)
add_executable(bench benchmark.cpp)

# find_package will search for available TBBConfig using variables CMAKE_PREFIX_PATH and TBB_DIR.
find_package(arrow REQUIRED arrow_shared)

#get_cmake_property(_variableNames VARIABLES)
#list (SORT _variableNames)
#foreach (_variableName ${_variableNames})
#    message(STATUS "${_variableName}=${${_variableName}}")
#endforeach()

target_include_directories(bench PRIVATE .
                           ${CMAKE_INSTALL_PREFIX}/include)

target_link_libraries(bench arrow_shared
#                      ${arrow_IMPORTED_TARGETS}    # Link imported targets
                      ${CMAKE_DL_LIBS})  # Link "rt" library on Linux