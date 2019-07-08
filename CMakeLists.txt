cmake_minimum_required(VERSION 3.0.0 FATAL_ERROR)

project(bench CXX)
add_executable(bench benchmark.cpp)

find_package(arrow REQUIRED arrow_shared)
find_package(TBB REQUIRED tbb)

#get_cmake_property(_variableNames VARIABLES)
#list (SORT _variableNames)
#foreach (_variableName ${_variableNames})
#    message(STATUS "${_variableName}=${${_variableName}}")
#endforeach()

target_compile_features(bench PRIVATE cxx_std_17)
target_include_directories(bench PRIVATE .
                           ${CMAKE_INSTALL_PREFIX}/include)
link_directories(${CONDA_PREFIX}/lib)
target_link_libraries(bench arrow_shared
                      ${TBB_IMPORTED_TARGETS}
                      ${arrow_IMPORTED_TARGETS}    # Link imported targets
                      ${CMAKE_DL_LIBS})  # Link "rt" library on Linux
