macro(slime_option var default_value description)
    if(DEFINED ENV{${var}})
        if("$ENV{${var}}" MATCHES "^(ON|OFF|TRUE|FALSE)$")
            set(_default "$ENV{${var}}")
        else()
            message(WARNING "Invalid value for env ${var}: '$ENV{${var}}', use default ${default_value}")
            set(_default ${default_value})
        endif()
    else()
        set(_default ${default_value})
    endif()
    option(${var} "${description}" ${_default})
endmacro()

function (run_python OUT EXPR ERR_MSG)
  find_package(Python3 COMPONENTS Interpreter REQUIRED)
  message("-- Run python: ${Python3_EXECUTABLE} -c \"${EXPR}\"")
  execute_process(
    COMMAND
    "${Python3_EXECUTABLE}" "-c" "${EXPR}"
    OUTPUT_VARIABLE PYTHON_OUT
    RESULT_VARIABLE PYTHON_ERROR_CODE
    ERROR_VARIABLE PYTHON_STDERR
    OUTPUT_STRIP_TRAILING_WHITESPACE)

  if(NOT PYTHON_ERROR_CODE EQUAL 0)
    message(FATAL_ERROR "${ERR_MSG}: ${Python3_EXECUTABLE} ${PYTHON_STDERR}")
  endif()
  set(${OUT} ${PYTHON_OUT} PARENT_SCOPE)
endfunction()

function(find_libraries FOUND_LIBS LIB_DIR LIB_NAMES)
  set(found_libs_temp)
  foreach(LIB_NAME IN LISTS LIB_NAMES)
    find_library(${LIB_NAME}_LIBRARY
      NAMES ${LIB_NAME} "lib${LIB_NAME}"
      HINTS "${LIB_DIR}/lib" "${LIB_DIR}"
      NO_DEFAULT_PATH
    )
    if(${LIB_NAME}_LIBRARY)
      message("-- Found ${LIB_NAME}: ${${LIB_NAME}_LIBRARY}")
      list(APPEND found_libs_temp ${${LIB_NAME}_LIBRARY})
    endif()
  endforeach()
  set(${FOUND_LIBS} ${found_libs_temp} PARENT_SCOPE)
endfunction()
