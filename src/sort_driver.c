//
// A dirver that loads two tables in memory and calls a kernel to join them
// @author Meisam
// @date 2014-09-08
// @version 0.1

////////////////////////////////////////////////////////////////////////////////

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
#include "common.c"

////////////////////////////////////////////////////////////////////////////////

// Use a static data size for simplicity
//
#define DATA_SIZE (1<<16) // As much data as you can allocate
#define SCALE (1<<4)

////////////////////////////////////////////////////////////////////////////////

// Simple compute kernel which computes the hash_join of an input array
//
const char *KernelSource = "";

int main(int argc, char** argv) {

    log_debug("Start of execution.")
    int err;                            // error code returned from API calls
    char **program_buffer;
    int *program_size;

    program_size = (int*) malloc(sizeof(int));
    program_buffer = (char **) malloc(sizeof(char*));
    err = read_file(argv[1], program_buffer, program_size);
    if (err) {
        log_debug("An error ocurred in reading the file");
        return err;
    }
    int *h_data;              // The smaller table in the join
    h_data = (int *) malloc(sizeof(int) * DATA_SIZE);
    int* results;         // The join results
    results = (int *) malloc(sizeof(int) * DATA_SIZE);
    unsigned int incorrect;                // number of correct results returned

    size_t global;                     // global domain size for our calculation
    size_t local;                       // local domain size for our calculation

    cl_device_id device_id;                   // compute device id
    cl_context context;                       // compute context
    cl_command_queue commands;                // compute command queue
    cl_program program;                       // compute program
    cl_kernel kernel;                         // compute kernel

    cl_mem d_buffer;             // device memory used for the input array

    // Fill our data set with random int values
    //
    log_debug("Going to create arrays");
    int i = 0;
    int j = 0;
    unsigned int count = DATA_SIZE;
    for (i = 0; i < count; i++) {
        h_data[i] = i;
    }

    // Connect to a compute device
    //
    // Meisam: NVIDIA devices do not accept NULL as platform ID

    log_debug("Going to run the program...\n");
    cl_platform_id* platform_ids;
    platform_ids = (cl_platform_id *) malloc(sizeof(cl_platform_id) * 100);
    log_debug("... created arrays");

    unsigned int *num_platforms = (unsigned int *) malloc(
            sizeof(unsigned int *));
    *num_platforms = 0;
    log_debug("Going to query platforms...");
    err = clGetPlatformIDs(100, platform_ids, num_platforms);

    log_debug2("Number of platforms = %d\n", *num_platforms);

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
    int *device_count;
    device_count = (int *) malloc(sizeof(int));
    *device_count = 10;
    // CL_DEVICE_TYPE_ALL; //CL_DEVICE_TYPE_GPU, CL_DEVICE_TYPE_CPU
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
    KernelSource = *program_buffer;
    program = clCreateProgramWithSource(context, 1,
            (const char **) &KernelSource, NULL, &err);
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
    kernel = clCreateKernel(program, "sort", &err);
    if (!kernel || err != CL_SUCCESS) {
        printf("Error: Failed to create compute kernel!\n");
        exit(1);
    }

    clock_t begin = clock();

    // Create the input and output arrays in device memory for our calculation
    //
    const unsigned int probe_size = DATA_SIZE;
    const unsigned int build_size = DATA_SIZE;

    d_buffer = clCreateBuffer(context, CL_MEM_READ_WRITE,
            sizeof(int) * DATA_SIZE, NULL, NULL);
    /**/
    if (!d_buffer) {
        printf("Error: (%d) Failed to allocate device memory!\n", err);
        exit(1);
    }

    // Write our data set into the input array in device memory
    //
    err = clEnqueueWriteBuffer(commands, d_buffer, CL_TRUE, 0,
            sizeof(int) * count, h_data, 0, NULL, NULL);
    if (err != CL_SUCCESS) {
        printf("Error: Failed to write to source array!\n");
        return EXIT_FAILURE;
    }

    // Set the arguments to our compute kernel
    //
    err = 0;
    err |= clSetKernelArg(kernel, 0, sizeof(unsigned int), &build_size);
    err |= clSetKernelArg(kernel, 1, sizeof(cl_mem), &d_buffer);
    if (err != CL_SUCCESS) {
        printf("Error: Failed to set kernel arguments! %d\n", err);
        return EXIT_FAILURE;
    }

    // Get the maximum work group size for executing the kernel on the device
    //
    err = clGetKernelWorkGroupInfo(kernel, device_id,
    CL_KERNEL_WORK_GROUP_SIZE, sizeof(local), &local, NULL);

    if (err != CL_SUCCESS) {
        printf("Error: Failed to retrieve kernel work group info! %d\n", err);
        return EXIT_FAILURE;
    }
    if (local > DATA_SIZE) {
        local = DATA_SIZE;
    }

    // Execute the kernel over the entire range of our 1d input data set
    // using the maximum number of work group items for this device
    //
    global = DATA_SIZE;
    printf("local workgroup size=%d\n", local);
    printf("global workgroup size=%d\n", global);
    log_debug2("Global work units %d.", global);
    log_debug2("Local work units %d.", local);

    clock_t begin_kernel = clock();
    err = clEnqueueNDRangeKernel(commands, kernel, 1, NULL, &global, &local, 0,
    NULL, NULL);
    if (err) {
        printf("Error: Failed to execute kernel!\n");
        return EXIT_FAILURE;
    }

    // Wait for the command commands to get serviced before reading back results
    //
    clFinish(commands);

    // Read back the results from the device to verify the output
    //
    clock_t begin_copy_back = clock();

    err = clEnqueueReadBuffer(commands, d_buffer, CL_TRUE, 0,
            sizeof(int) * count, results, 0, NULL, NULL);
    if (err != CL_SUCCESS) {
        printf("Error: Failed to read output array! %d\n", err);
        exit(1);
    }

    clock_t end = clock();
    double elapsed_secs = (double) (end - begin) / CLOCKS_PER_SEC;
    printf("It took %3f seconds to do the computation on OpenCL\n",
            elapsed_secs);

    elapsed_secs = (double) (begin_kernel - begin) / CLOCKS_PER_SEC;
    printf("It took %3f seconds to copy data to the device\n",
            elapsed_secs);

    elapsed_secs = (double) (begin_copy_back - begin_kernel) / CLOCKS_PER_SEC;
    printf("It took %3f seconds runing the kernel on the device\n",
            elapsed_secs);

    elapsed_secs = (double) (end - begin_copy_back) / CLOCKS_PER_SEC;
    printf("It took %3f seconds to copy data back from device\n",
            elapsed_secs);
/*
    clock_t begin2 = clock();

    for (i = 0; i < DATA_SIZE - 1; i++) {
        for (j = i + 1; j < DATA_SIZE; j++) {
            if (h_data[i] < h_data[j]) {
                int tmp = h_data[j];
                h_data[j] = h_data[i];
                h_data[i] = tmp;
            }
        }
    }

    clock_t end2 = clock();
    double elapsed_secs2 = (double) (end2 - begin2) / CLOCKS_PER_SEC;
    printf("It took %3f seconds to do the computation on CPU\n", elapsed_secs2);

    // Validate our results
    //
    incorrect = 0;
    for (i = 0; i < count; i++) {
        if (results[i] != h_data[i]) {
            incorrect++;
            printf("%5d: result =%5d, h_data(reversed)=%5d.\n", i, results[i],
                    h_data[i]);
        }
    }

    // Print a brief summary detailing the results
    //
    printf("Computed '%d/%d' incorrect values!\n", incorrect, count);
*/
    // Shutdown and cleanup
    //
    clReleaseMemObject(d_buffer);
    clReleaseProgram(program);
    clReleaseKernel(kernel);
    clReleaseCommandQueue(commands);
    clReleaseContext(context);

    free(h_data);
    free(results);

    log_debug("END of execution.")
    /*
     */
    return 0;
}
