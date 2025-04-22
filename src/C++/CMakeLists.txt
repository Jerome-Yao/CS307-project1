# Minimum CMake version required
cmake_minimum_required(VERSION 3.10)

# Project name
project(CS307Project1)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Find PostgreSQL package
find_package(PostgreSQL REQUIRED)

# Include directories
include_directories(${CMAKE_SOURCE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${CMAKE_SOURCE_DIR}/src/C++/include)
include_directories(${PostgreSQL_INCLUDE_DIRS})

# Add the executable
add_executable(load_single load_single.cpp)
add_executable(load_parallel load_parallel.cpp)

# Find pqxx library
find_library(PQXX_LIB pqxx REQUIRED)

# Link PostgreSQL and pqxx libraries to load_single
target_link_libraries(load_single 
    PRIVATE 
    ${PostgreSQL_LIBRARIES} 
    ${PQXX_LIB}
    pq  # 添加 PostgreSQL C 客户端库
)

# Link PostgreSQL and pqxx libraries to load_parallel
target_link_libraries(load_parallel 
    PRIVATE 
    ${PostgreSQL_LIBRARIES} 
    ${PQXX_LIB}
    pq  # 添加 PostgreSQL C 客户端库
)

# Add compiler flags if needed
if(CMAKE_COMPILER_IS_GNUCXX)
    target_compile_options(load_single PRIVATE -Wall -Wextra)
    target_compile_options(load_parallel PRIVATE -Wall -Wextra)
endif()

# Print PostgreSQL info for debugging
message(STATUS "PostgreSQL_INCLUDE_DIRS: ${PostgreSQL_INCLUDE_DIRS}")
message(STATUS "PostgreSQL_LIBRARIES: ${PostgreSQL_LIBRARIES}")

# 在 find_package(PostgreSQL REQUIRED) 后添加
link_directories(${PostgreSQL_LIBRARY_DIRS})