/*
 * common.c
 *
 *  Created on: Sep 18, 2014
 *      Author: fathi
 */

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <math.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <CL/cl.h>
#include <time.h>
#include "common.h"

#define MAX_FILE_SIZE (1<<30)

///
/// Reads the file from the given path. The file content will be copied to file_buffer and its size will be copied to file_size

int read_file(char *file_path, char **file_buffer, size_t *file_size) {
    FILE * program_handle;
    program_handle = fopen(file_path, "r");
    if (program_handle == NULL) {
        printf("ERROR: File %s cannot be opened!\n", file_path);
        return errno;
    }
    log_debug2("file %s opened successfully", file_path);
    fseek(program_handle, 0, SEEK_END);
    log_debug("seek successful")
    *file_size = ftell(program_handle);
    log_debug2("File size is %zd", *file_size);
    rewind(program_handle);
    log_debug("rewind successful");
    if (*file_size > MAX_FILE_SIZE) {
        *file_size = MAX_FILE_SIZE;
    }
    log_debug("going to allocate buffer...");
    *file_buffer = (char *) malloc(*file_size + 1);
    log_debug("Buffer allocated successfully");
    file_buffer[0][*file_size] = '\0';
    log_debug2("Program size is %zd.", *file_size);
    log_debug("Initializing read bufferers successful")
    fread(*file_buffer, sizeof(char), *file_size, program_handle);
    log_debug2("Reading streams successful\n%s", *file_buffer);
    int res = fclose(program_handle);
    if (res == EOF) {
        log_debug("closing streams failed.");
    } else {
        log_debug("Closing streams succeeded");
    }
    return 0;
}

int prepare_device(char* kerenel_path, char * kernel_name) {
    int err;                           // error code returned from API calls
    char **program_buffer;
    size_t *program_size;

    program_size = (size_t *) malloc(sizeof(size_t));
    program_buffer = (char **) malloc(sizeof(char *));
    err = read_file(kerenel_path, program_buffer, program_size);
    if (err) {
        log_debug("An error occurred in reading the file");
        return err;
    }
    // Connect to a compute device
    // NVIDIA devices do not accept NULL as platform ID

    log_debug("Going to connect to the device...\n");
    cl_uint max_platforms = 100;
    cl_platform_id* platform_ids;
    platform_ids = (cl_platform_id *) malloc(
            sizeof(cl_platform_id) * max_platforms);

    unsigned int *num_platforms = (unsigned int *) malloc(
            sizeof(unsigned int *));
    *num_platforms = 0;
    log_debug("Going to query platforms...");
    err = clGetPlatformIDs(max_platforms, platform_ids, num_platforms);

    log_debug2("Number of platforms = %d", *num_platforms);

    void * param_value;
    size_t param_size = 1024;
    param_value = malloc(param_size);
    size_t param_size_ret = 0;

    clGetPlatformInfo(platform_ids[0], CL_PLATFORM_NAME, param_size,
            param_value, &param_size_ret);
    log_debug2("CL_PLATFORM_NAME size %d", (int ) param_size_ret);
    log_debug2("CL_PLATFORM_NAME %s", (char * ) param_value);

    clGetPlatformInfo(platform_ids[0], CL_PLATFORM_VENDOR, param_size,
            param_value, &param_size_ret);
    log_debug2("CL_PLATFORM_VENDOR size %d", (int ) param_size_ret);
    log_debug2("CL_PLATFORM_VENDOR %s", (char * ) param_value);

    clGetPlatformInfo(platform_ids[0], CL_PLATFORM_PROFILE, param_size,
            param_value, &param_size_ret);
    log_debug2("CL_PLATFORM_PROFILE size %d", (int ) param_size_ret);
    log_debug2("CL_PLATFORM_PROFILE %s", (char * ) param_value);

    clGetPlatformInfo(platform_ids[0], CL_PLATFORM_VERSION, param_size,
            param_value, &param_size_ret);
    log_debug2("CL_PLATFORM_VERSION size %d", (int ) param_size_ret);
    log_debug2("CL_PLATFORM_VERSION %s", (char * ) param_value);

    clGetPlatformInfo(platform_ids[0], CL_PLATFORM_EXTENSIONS, param_size,
            param_value, &param_size_ret);
    log_debug2("CL_PLATFORM_EXTENSIONS size %d", (int ) param_size_ret);
    log_debug2("CL_PLATFORM_EXTENSIONS %s", (char * ) param_value);

//    err = CL_SUCCESS;
    log_debug2("Platforms queried successfully. %d found.", *num_platforms);
    if (err != CL_SUCCESS) {
        printf("Error: (Error code: %d) Failed to get platform IDs!\n", err);
        return EXIT_FAILURE;
    }

    if (*num_platforms < 1) {
        log_debug("No platform was found.");
        return EXIT_FAILURE;
    }
    unsigned int *device_count;
    device_count = (unsigned int *) malloc(sizeof(unsigned int));
// CL_DEVICE_TYPE_ALL; //CL_DEVICE_TYPE_GPU, CL_DEVICE_TYPE_CPU
    *device_count = -1;
    err = clGetDeviceIDs(platform_ids[0], CL_DEVICE_TYPE_ALL, 1, &device_id,
            device_count);
    log_debug2("device_count = %d\n", *device_count);
    if (err != CL_SUCCESS) {
        printf("Error: (Error code: %d) Failed to create a device group!\n",
                err);
        return EXIT_FAILURE;
    }

    clGetDeviceInfo(device_id, CL_DEVICE_MAX_COMPUTE_UNITS, param_size,
            param_value, &param_size_ret);
    log_debug2("CL_DEVICE_MAX_COMPUTE_UNITS size %d", (int ) param_size_ret);
    log_debug2("CL_DEVICE_MAX_COMPUTE_UNITS %d", *((int * ) param_value));

// Create a compute context
//
    context = clCreateContext(0, 1, &device_id, NULL, NULL, &err);
    if (!context) {
        printf("Error: Failed to create a compute context!");
        return EXIT_FAILURE;
    }

// Create a command commands
//
    commands = clCreateCommandQueue(context, device_id, 0, &err);
    if (!commands) {
        printf("Error: Failed to create a command commands!\n");
        return EXIT_FAILURE;
    }

// Create the compute program from the source buffer
//
    program = clCreateProgramWithSource(context, 1,
            (const char **) program_buffer, NULL, &err);
    if (!program) {
        printf("Error: Failed to create compute program!");
        return EXIT_FAILURE;
    }

// Build the program executable
//
    err = clBuildProgram(program, 0, NULL, NULL, NULL, NULL);
    if (err != CL_SUCCESS) {
        size_t len;
        char buffer[2048];

        printf("Error: Failed to build program executable!\n");
        clGetProgramBuildInfo(program, device_id, CL_PROGRAM_BUILD_LOG,
                sizeof(buffer), buffer, &len);
        printf("%s\n", buffer);
        return EXIT_FAILURE;
    }

// Create the compute kernel in the program we wish to run
//
    kernel = clCreateKernel(program, kernel_name, &err);
    if (!kernel || err != CL_SUCCESS) {
        printf("Error: Failed to create compute kernel!\n");
        exit(1);
    }

}
