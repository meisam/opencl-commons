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

////////////////////////////////////////////////////////////////////////////////

// Use a static data size for simplicity
//
#define DATA_SIZE (1<<10) // As much data as you can allocate
#define SCALE (1<<4)

////////////////////////////////////////////////////////////////////////////////

// Simple compute kernel which computes the hash_join of an input array
//
const char *KernelSource = "";

////////////////////////////////////////////////////////////////////////////////

int read_kernel_file(char *file_path, char **program_buffer, int *program_size) {
    FILE * program_handle;
    program_handle = fopen(file_path, "r");
    if (program_handle == NULL) {
        printf("ERROR: File %s cannot be opened!\n", file_path);
        return errno;
    }
    log_debug2("file %s opened successfully", file_path);
    fseek(program_handle, 0, SEEK_END);
    log_debug("seek successful")
    *program_size = ftell(program_handle);
    rewind(program_handle);
    log_debug("rewind successful")
    program_buffer[0] = (char *) malloc(*program_size + 1);
    program_buffer[0][*program_size] = '\0';
    log_debug2("Program size is %d.", *program_size);
    log_debug("Initializing read bufferers successful")
    fread(*program_buffer, sizeof(char), *program_size, program_handle);
    log_debug2("Reading streams successful\n%s", *program_buffer);
    int res = fclose(program_handle);
    if (res == EOF) {
        log_debug("closing streams failed.");
    } else {
        log_debug("Closing streams succeeded");
    }
    return 0;
}

enum db_type {
    DECIMAL, STRING, FLOAT, DATETIME
};

struct column_t {
    char *column_name;
    int column_index;
    enum db_type column_type;
    int lenght;
};

struct table_schema_t {
    char *name;
    int column_count;
    struct column_t* columns;
};

struct table_schema_t* get_part_table_shcema() {
//    PART|P_PARTKEY:INTEGER|P_NAME:TEXT:22|P_MFGR:TEXT:6|P_CATEGORY:TEXT:7|P_BRAND1:TEXT:9|P_COLOR:TEXT:11|P_TYPE:TEXT:25|P_SIZE:INTEGER|P_CONTAINER:TEXT:10
    struct column_t *col0;
    col0 = (struct column_t *) malloc(sizeof(struct column_t));
    col0->column_name = "P_PARTKEY";
    col0->column_index = 0;
    col0->column_type = DECIMAL;
    col0->lenght = 4;

    struct column_t *col1;
    col1 = (struct column_t *) malloc(sizeof(struct column_t));
    col1->column_name = "P_NAME";
    col1->column_index = 1;
    col1->column_type = STRING;
    col1->lenght = 22;

    struct column_t *col2;
    col2 = (struct column_t *) malloc(sizeof(struct column_t));
    col2->column_name = "P_MFGR";
    col2->column_index = 2;
    col2->column_type = STRING;
    col2->lenght = 6;

    struct column_t *col3;
    col3 = (struct column_t *) malloc(sizeof(struct column_t));
    col3->column_name = "P_CATEGORY";
    col3->column_index = 3;
    col3->column_type = DECIMAL;
    col3->lenght = 7;

    struct column_t *all_columns;
    all_columns = (struct column_t *) malloc(sizeof(struct column_t) * 4);
    all_columns[0] = *col0;
    all_columns[1] = *col1;
    all_columns[2] = *col2;
    all_columns[3] = *col3;

    struct table_schema_t *part_table;

    part_table = (struct table_schema_t *) malloc(
            sizeof(struct table_schema_t));
    part_table->name = "PART";
    part_table->column_count = 4;
    part_table->columns = all_columns;

    return part_table;
}

struct table_schema_t* get_lineorder_table_shcema() {
//  LINEORDER|LO_ORDERKEY:INTEGER|LO_LINENUMBER:INTEGER|LO_CUSTKEY:INTEGER|LO_PARTKEY:INTEGER|LO_SUPPKEY:INTEGER|LO_ORDERDATE:DATE|LO_ORDERPRIORITY:TEXT:16|LO_SHIPPRIORITY:TEXT:1|LO_QUANTITY:INTEGER|LO_EXTENDEDPRICE:INTEGER|LO_ORDTOTALPRICE:INTEGER|LO_DISCOUNT:INTEGER|LO_REVENUE:INTEGER|LO_SUPPLYCOST:INTEGER|LO_TAX:INTEGER|LO_COMMITDATE:DATE|LO_SHIPMODE:TEXT:10
    struct column_t *col0;
    col0 = (struct column_t *) malloc(sizeof(struct column_t));
    col0->column_name = "LO_ORDERKEY";
    col0->column_index = 0;
    col0->column_type = DECIMAL;
    col0->lenght = 4;

    struct column_t *col1;
    col1 = (struct column_t *) malloc(sizeof(struct column_t));
    col1->column_name = "LO_LINENUMBER";
    col1->column_index = 1;
    col1->column_type = DECIMAL;
    col1->lenght = 4;

    struct column_t *col2;
    col2 = (struct column_t *) malloc(sizeof(struct column_t));
    col2->column_name = "LO_CUSTKEYE";
    col2->column_index = 2;
    col2->column_type = DECIMAL;
    col2->lenght = 4;

    struct column_t *col3;
    col3 = (struct column_t *) malloc(sizeof(struct column_t));
    col3->column_name = "LO_PARTKEY";
    col3->column_index = 3;
    col3->column_type = DECIMAL;
    col3->lenght = 4;

    struct column_t *all_columns;
    all_columns = (struct column_t *) malloc(sizeof(struct column_t) * 4);
    all_columns[0] = *col0;
    all_columns[1] = *col1;
    all_columns[2] = *col2;
    all_columns[3] = *col3;

    struct table_schema_t *lineorder_table;

    lineorder_table = (struct table_schema_t *) malloc(
            sizeof(struct table_schema_t));
    lineorder_table->name = "LINEORDER";
    lineorder_table->column_count = 4;
    lineorder_table->columns = all_columns;

    return lineorder_table;
}

int read_column(struct table_schema_t schema, int column_index, char *path) {
    FILE * column_file;
    char *full_path;
    int path_lenght = strlen(path) + strlen("/") + strlen(schema.name) + 1;
    full_path = (char *) malloc(sizeof(char) * path_lenght);
    strcpy(full_path, path);
    strcat(full_path, "/");
    strcat(full_path, schema.name);
    full_path[path_lenght - 1] = (char) ('0' + column_index);
    printf("FullPath is %s\n", full_path);
    column_file = fopen(full_path, "r");
    return -1;
}

int read_table(struct table_schema_t schema, char *path) {
    int column_index = 0;
    for (column_index = 0; column_index < schema.column_count; column_index++) {
        read_column(schema, column_index, path);
    }
    return -1;
}

int main(int argc, char** argv) {

    struct table_schema_t *lineorder_table = get_lineorder_table_shcema();
    read_table(*lineorder_table,
            "../ssdb");

    log_debug("Start of execution.")
    int err;                            // error code returned from API calls
    char **program_buffer;
    int *program_size;

    program_size = (int*) malloc(sizeof(int));
    program_buffer = (char **) malloc(sizeof(char*));
    err = read_kernel_file(argv[1], program_buffer, program_size);
    if (err) {
        log_debug("An error ocurred in reading the file");
        return err;
    }
    int *build_data;              // The smaller table in the join
    build_data = (int *) malloc(sizeof(int) * DATA_SIZE);
    int *probe_data;      // The bigger table in the join
    probe_data = (int *) malloc(sizeof(int) * DATA_SIZE * SCALE);
    char* results;         // The join results
    results = (char *) malloc(sizeof(char) * DATA_SIZE * SCALE);
    unsigned int correct;                  // number of correct results returned

    size_t global;                     // global domain size for our calculation
    size_t local;                       // local domain size for our calculation

    cl_device_id device_id;                   // compute device id
    cl_context context;                       // compute context
    cl_command_queue commands;                // compute command queue
    cl_program program;                       // compute program
    cl_kernel kernel;                         // compute kernel

    cl_mem d_build_buffer;             // device memory used for the input array
    cl_mem d_probe_buffer;            // device memory used for the output array
    cl_mem d_join_result_buffer;      // device memory used for the output array

// Fill our data set with random int values
//
    log_debug("Going to create arrays");
    int i = 0;
    unsigned int count = DATA_SIZE;
    for (i = 0; i < count; i++) {
        build_data[i] = i;
    }

    for (i = 0; i < DATA_SIZE * SCALE; i++) {
        if (i % 23 == 0) {
            probe_data[i] = i % DATA_SIZE;
        } else {
            probe_data[i] = DATA_SIZE + 1 + i;
        }
    }

// Connect to a compute device
//
// Meisam: NVIDIA devices do not accept NULL as platform ID

    log_debug("Going to run the program...\n");
    cl_uint max_platforms = 100;
    cl_platform_id* platform_ids;
    platform_ids = (cl_platform_id *) malloc(sizeof(cl_platform_id) * max_platforms);
    log_debug("... created arrays");

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
    int *device_count;
    device_count = (int *) malloc(sizeof(int));
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
    KernelSource = *program_buffer;
    program = clCreateProgramWithSource(context, 1,
            (const char **) &KernelSource,
            NULL, &err);
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

    d_build_buffer = clCreateBuffer(context, CL_MEM_READ_ONLY,
            sizeof(int) * DATA_SIZE, NULL, NULL);
    d_probe_buffer = clCreateBuffer(context, CL_MEM_READ_ONLY,
            sizeof(int) * DATA_SIZE, NULL, NULL);

    d_join_result_buffer = clCreateBuffer(context, CL_MEM_WRITE_ONLY,
            sizeof(char) * DATA_SIZE, NULL, NULL);
    /**/
    if (!d_build_buffer || !d_probe_buffer || !d_join_result_buffer) {
        printf("Error: (%d) Failed to allocate device memory!\n", err);
        exit(1);
    }

// write the build table data only once
    err = clEnqueueWriteBuffer(commands, d_build_buffer, CL_TRUE, 0,
            sizeof(int) * count, build_data, 0, NULL, NULL);
    if (err != CL_SUCCESS) {
        printf("Error: Failed to write to source array!\n");
        return EXIT_FAILURE;
    }

    for (i = 0; i < SCALE; i++) {
        log_debug2("Processing chunck %d================================", i);
        // Write our data set into the input array in device memory
        //
        err = clEnqueueWriteBuffer(commands, d_probe_buffer, CL_TRUE, 0,
                sizeof(int) * count, &probe_data[i * probe_size], 0, NULL,
                NULL);
        if (err != CL_SUCCESS) {
            printf("Error: Failed to write to source array!\n");
            return EXIT_FAILURE;
        }

        // Set the arguments to our compute kernel
        //
        err = 0;
//        err = clSetKernelArg(kernel, 0, sizeof(unsigned int), &d_probe_buffer);
        err |= clSetKernelArg(kernel, 0, sizeof(unsigned int), &build_size);
        err |= clSetKernelArg(kernel, 1, sizeof(cl_mem), &d_build_buffer);
//        err |= clSetKernelArg(kernel, 1, sizeof(unsigned int), &probe_size);
//        err |= clSetKernelArg(kernel, 4, sizeof(cl_mem), &d_join_result_buffer);
        if (err != CL_SUCCESS) {
            printf("Error: Failed to set kernel arguments! %d\n", err);
            return EXIT_FAILURE;
        }

        // Get the maximum work group size for executing the kernel on the device
        //
        err = clGetKernelWorkGroupInfo(kernel, device_id,
        CL_KERNEL_WORK_GROUP_SIZE, sizeof(local), &local, NULL);
        if (err != CL_SUCCESS) {
            printf("Error: Failed to retrieve kernel work group info! %d\n",
                    err);
            return EXIT_FAILURE;
        }

        // Execute the kernel over the entire range of our 1d input data set
        // using the maximum number of work group items for this device
        //
        global = count;
        log_debug2("Global work units %d.", global);
        log_debug2("Local work units %d.", local);
        err = clEnqueueNDRangeKernel(commands, kernel, 1, NULL, &global, &local,
                0,
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
        err = clEnqueueReadBuffer(commands, d_join_result_buffer, CL_TRUE, 0,
                sizeof(char) * count, &results[i * DATA_SIZE], 0, NULL,
                NULL);
        if (err != CL_SUCCESS) {
            printf("Error: Failed to read output array! %d\n", err);
            exit(1);
        }
    }

    clock_t end = clock();

    double elapsed_secs = (double) (end - begin) / CLOCKS_PER_SEC;

// Validate our results
//
    correct = 0;
    for (i = 0; i < count; i++) {
        if (results[i] == build_data[i]) {
            correct++;
        } else {
            if (i - correct < 10) {
                printf("%d does not match %d.\n", results[i], build_data[i]);
            }
        }
    }

// Print a brief summary detailing the results
//
    printf("Computed '%d/%d' correct values!\n", correct, count);
    printf("It took %3f seconds to do the computation", elapsed_secs);

// Shutdown and cleanup
//
    clReleaseMemObject(d_build_buffer);
    clReleaseMemObject(d_probe_buffer);
    clReleaseMemObject(d_join_result_buffer);
    clReleaseProgram(program);
    clReleaseKernel(kernel);
    clReleaseCommandQueue(commands);
    clReleaseContext(context);

    free(build_data);
    free(probe_data);
    free(results);

    log_debug("END of execution.")
    /*
     */
    return 0;
}
