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
#define HASH_TABLE_EXTRA_FACTOR (2)

////////////////////////////////////////////////////////////////////////////////

enum db_type {
    INTEGER, STRING, FLOAT, DATETIME
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
    col0->column_type = INTEGER;
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
    col3->column_type = INTEGER;
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
    col0->column_type = INTEGER;
    col0->lenght = 4;

    struct column_t *col1;
    col1 = (struct column_t *) malloc(sizeof(struct column_t));
    col1->column_name = "LO_LINENUMBER";
    col1->column_index = 1;
    col1->column_type = INTEGER;
    col1->lenght = 4;

    struct column_t *col2;
    col2 = (struct column_t *) malloc(sizeof(struct column_t));
    col2->column_name = "LO_CUSTKEYE";
    col2->column_index = 2;
    col2->column_type = INTEGER;
    col2->lenght = 4;

    struct column_t *col3;
    col3 = (struct column_t *) malloc(sizeof(struct column_t));
    col3->column_name = "LO_PARTKEY";
    col3->column_index = 3;
    col3->column_type = INTEGER;
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

int read_column(struct table_schema_t schema, int column_index, char *path,
        char *column_data, size_t *column_size) {
    char *full_path;
    size_t header_size = 3 * sizeof(long) + 3 * sizeof(int)
            + 4060 * sizeof(char);
    int path_lenght = strlen(path) + strlen("/") + strlen(schema.name) + 1;
    full_path = (char *) malloc(sizeof(char) * path_lenght + 1);
    strcpy(full_path, path);
    strcat(full_path, "/");
    strcat(full_path, schema.name);
    full_path[path_lenght - 1] = (char) ('0' + column_index);
    full_path[path_lenght] = '\0';
    log_debug("FullPath is %s\n", full_path);
    read_file(full_path, &column_data, column_size);
    log_debug("size of the file %zd\n", *column_size);
    *column_size = *column_size - header_size;
    column_data = &column_data[header_size];
    return -1;
}

struct table_data_t {
    char **column_data;
    size_t *column_sizes;
};

struct table_data_t* read_table(struct table_schema_t schema, char *path) {
    int column_index = 0;

    char **column_data = (char **) malloc(sizeof(char *) * schema.column_count);
    size_t *column_sizes = (size_t *) malloc(
            sizeof(size_t) * schema.column_count);
    for (column_index = 0; column_index < schema.column_count; column_index++) {
        read_column(schema, column_index, path, column_data[column_index],
                &column_sizes[column_index]);
        log_debug("column_size %10zd\n", column_sizes[column_index]);
    }
    struct table_data_t* table_data = (struct table_data_t *) malloc(
            sizeof(struct table_data_t));
    table_data->column_data = column_data;
    table_data->column_sizes = column_sizes;
    return table_data;
}

cl_int gpu_hash_join(cl_mem d_data_buffer, cl_mem d_hashed_data_buffer,
        size_t data_size) {
    cl_int err = 0;
    err |= clSetKernelArg(kernel, 0, sizeof(cl_mem), &d_data_buffer);
    err |= clSetKernelArg(kernel, 1, sizeof(cl_mem), &d_hashed_data_buffer);
    if (err != CL_SUCCESS) {
        printf("Error: Failed to set arguments kernel! %d\n", err);
        return err;
    }

    err = clGetKernelWorkGroupInfo(kernel, device_id,
    CL_KERNEL_WORK_GROUP_SIZE, sizeof(local), &local, NULL);
    if (err != CL_SUCCESS) {
        printf("Error: Failed to retrieve kernel work group info! %d\n", err);
        return err;
    }
    global = data_size;
    if (local > global) {
        local = global;
    }
    log_debug("Global work units %zd.", global);
    log_debug("Local work units %zd.", local);
    err = clEnqueueNDRangeKernel(commands, kernel, 1, NULL, &global, &local, 0,
    NULL, NULL);
    if (err != CL_SUCCESS) {
        printf(
                "Error: (error code %d) Build hash table failed to execute kernel!\n",
                err);
    }
    return CL_SUCCESS;
}

void build_hash_table(size_t data_size, int* data, size_t hash_table_size,
        int *hash_table) {
    cl_mem d_data_buffer;         // device memory used for the input data
    cl_mem d_hashed_data_buffer; // device memory used for the hashed output data

    d_data_buffer = clCreateBuffer(context, CL_MEM_READ_ONLY,
            sizeof(int) * data_size, NULL, NULL);
    d_hashed_data_buffer = clCreateBuffer(context, CL_MEM_WRITE_ONLY,
            sizeof(int) * hash_table_size, NULL, NULL);

    cl_int err = 0;
    err = clEnqueueWriteBuffer(commands, d_data_buffer, CL_TRUE, 0,
            sizeof(int) * data_size, data, 0, NULL, NULL);

    err = gpu_hash_join(d_data_buffer, d_hashed_data_buffer, data_size);
    err = clEnqueueReadBuffer(commands, d_hashed_data_buffer, CL_TRUE, 0,
            sizeof(int) * hash_table_size, hash_table, 0, NULL, NULL);
    if (err != CL_SUCCESS) {
        printf("Error: Failed to read output array! %d\n", err);
        return;
    }

}

cl_int gpu_count_hashed_values(cl_mem d_data_buffer,
        cl_mem d_hashed_data_buffer, size_t data_size) {
    cl_int err = 0;
    err |= clSetKernelArg(kernel, 0, sizeof(cl_mem), &d_data_buffer);
    err |= clSetKernelArg(kernel, 1, sizeof(cl_mem), &d_hashed_data_buffer);
    if (err != CL_SUCCESS) {
        printf("Error: Failed to set arguments kernel! %d\n", err);
        return err;
    }

    err = clGetKernelWorkGroupInfo(kernel, device_id,
    CL_KERNEL_WORK_GROUP_SIZE, sizeof(local), &local, NULL);
    if (err != CL_SUCCESS) {
        printf("Error: Failed to retrieve kernel work group info! %d\n", err);
        return err;
    }
    global = data_size;
    if (local > global) {
        local = global;
    }
    log_debug("Global work units %zd.", global);
    log_debug("Local work units %zd.", local);
    err = clEnqueueNDRangeKernel(commands, kernel, 1, NULL, &global, &local, 0,
    NULL, NULL);
    if (err != CL_SUCCESS) {
        printf(
                "Error: (error code %d) Build hash table failed to execute kernel!\n",
                err);
    }
    return CL_SUCCESS;
}

void count_hash_values(size_t hash_values_size, int* hash_values,
        size_t counts_size, int *counts) {
    cl_mem d_hashed_values;         // device memory used for the input data
    cl_mem d_counts; // device memory used for the hashed output data

    d_hashed_values = clCreateBuffer(context, CL_MEM_READ_ONLY,
            sizeof(int) * hash_values_size, NULL, NULL);
    d_counts = clCreateBuffer(context, CL_MEM_WRITE_ONLY,
            sizeof(int) * hash_values_size, NULL, NULL);

    cl_int err = 0;
    err = clEnqueueWriteBuffer(commands, d_hashed_values, CL_TRUE, 0,
            sizeof(int) * hash_values_size, hash_values, 0, NULL, NULL);

    err = gpu_count_hashed_values(d_hashed_values, d_counts, hash_values_size);
    err = clEnqueueReadBuffer(commands, d_counts, CL_TRUE, 0,
            sizeof(int) * counts_size, counts, 0, NULL, NULL);
    if (err != CL_SUCCESS) {
        printf("Error: Failed to read output array! %d\n", err);
        return;
    }
}

cl_int gpu_prefix_sum(cl_mem d_counts_buffer, cl_mem d_prefix_sum_buffer,
        size_t data_size) {
    cl_int err = 0;
    err |= clSetKernelArg(kernel, 0, sizeof(cl_mem), &d_counts_buffer);
    err |= clSetKernelArg(kernel, 1, sizeof(cl_mem), &d_prefix_sum_buffer);
    if (err != CL_SUCCESS) {
        printf("Error: Failed to set arguments kernel! %d\n", err);
        return err;
    }

    err = clGetKernelWorkGroupInfo(kernel, device_id,
    CL_KERNEL_WORK_GROUP_SIZE, sizeof(local), &local, NULL);
    if (err != CL_SUCCESS) {
        printf("Error: Failed to retrieve kernel work group info! %d\n", err);
        return err;
    }
    global = data_size;
    if (local > global) {
        local = global;
    }
    log_debug("Global work units %zd.", global);
    log_debug("Local work units %zd.", local);
    err = clEnqueueNDRangeKernel(commands, kernel, 1, NULL, &global, &local, 0,
    NULL, NULL);
    if (err != CL_SUCCESS) {
        printf(
                "Error: (error code %d) Build hash table failed to execute kernel!\n",
                err);
    }
    return CL_SUCCESS;
}

void prefix_sum(size_t counts_size, int* counts, unsigned long *prefix_sums) {
    cl_mem d_counts;         // device memory used for the input data
    cl_mem d_prefix_sums; // device memory used for the hashed output data

    d_counts = clCreateBuffer(context, CL_MEM_READ_ONLY,
            sizeof(int) * counts_size, NULL, NULL);
    d_prefix_sums = clCreateBuffer(context, CL_MEM_READ_WRITE,
            sizeof(unsigned long) * counts_size, NULL, NULL);

    cl_int err = 0;
    err = clEnqueueWriteBuffer(commands, d_counts, CL_TRUE, 0,
            sizeof(int) * counts_size, counts, 0, NULL, NULL);

    err = gpu_prefix_sum(d_counts, d_prefix_sums, counts_size);

    int stride = 0;
    for (stride = 1; stride < counts_size; stride <<= 1) {
        err = gpu_prefix_sum(d_counts, d_prefix_sums, counts_size);
    }

    err = clEnqueueReadBuffer(commands, d_prefix_sums, CL_TRUE, 0,
            sizeof(unsigned long) * counts_size, prefix_sums, 0, NULL, NULL);
    if (err != CL_SUCCESS) {
        printf("Error: Failed to read output array! %d\n", err);
        return;
    }
}

int __main(int argc, char **argv) {

    log_debug("Start of execution.");
    prepare_device(argv[1], "join");

    struct table_schema_t *lineorder_table = get_lineorder_table_shcema();
    struct table_data_t *lineorder_data;
    lineorder_data = read_table(*lineorder_table, "../ssdb");

    struct table_schema_t *part_table = get_part_table_shcema();
    struct table_data_t* part_data;
    part_data = read_table(*part_table, "../ssdb");

    // let't hash the third column
    size_t data_size = part_data->column_sizes[3] * sizeof(int) / sizeof(char);
    size_t hash_table_size = data_size * HASH_TABLE_EXTRA_FACTOR;

    int *hash_table = (int *) malloc(sizeof(int) * hash_table_size);
    build_hash_table(part_data->column_sizes[3],
            (int *) part_data->column_data[3], hash_table_size, hash_table);

    int *build_data;              // The smaller table in the join
    build_data = (int *) malloc(sizeof(int) * DATA_SIZE);
    int *probe_data;      // The bigger table in the join
    probe_data = (int *) malloc(sizeof(int) * DATA_SIZE * SCALE);
    char* results;         // The join results
    results = (char *) malloc(sizeof(char) * DATA_SIZE * SCALE);
    unsigned int correct;          // number of correct results returned

// Validate our results
//// Create the input and output arrays in device memory for our calculation
    cl_mem d_build_buffer;     // device memory used for the input array
    cl_mem d_probe_buffer;    // device memory used for the output array
    cl_mem d_join_result_buffer; // device memory used for the output array

    //
    const unsigned int probe_size = DATA_SIZE;
    const unsigned int build_size = DATA_SIZE;
    int count = DATA_SIZE;

    d_build_buffer = clCreateBuffer(context, CL_MEM_READ_ONLY,
            sizeof(int) * DATA_SIZE, NULL, NULL);
    d_probe_buffer = clCreateBuffer(context, CL_MEM_READ_ONLY,
            sizeof(int) * DATA_SIZE, NULL, NULL);

    d_join_result_buffer = clCreateBuffer(context, CL_MEM_WRITE_ONLY,
            sizeof(char) * DATA_SIZE, NULL, NULL);
    /**/
    if (!d_build_buffer || !d_probe_buffer || !d_join_result_buffer) {
        printf("Error: Failed to allocate device memory!\n");
        exit(1);
    }

    // write the build table data only once
    cl_int err = clEnqueueWriteBuffer(commands, d_build_buffer, CL_TRUE, 0,
            sizeof(int) * count, build_data, 0, NULL, NULL);
    if (err != CL_SUCCESS) {
        printf("Error: Failed to write to source array!\n");
        return EXIT_FAILURE;
    }
    clock_t begin = clock();

    int i;
    for (i = 0; i < SCALE; i++) {
        log_debug("Processing chunk %d================================", i);
        // Write our data set into the input array in device memory
        //
        err = clEnqueueWriteBuffer(commands, d_probe_buffer, CL_TRUE, 0,
                sizeof(int) * count, &probe_data[i * probe_size], 0,
                NULL,
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
        log_debug("Global work units %zd.", global);
        log_debug("Local work units %zd.", local);
        err = clEnqueueNDRangeKernel(commands, kernel, 1, NULL, &global, &local,
                0,
                NULL, NULL);
        if (err) {
            printf("Error: (error code %d) Failed to execute kernel!\n", err);
            return EXIT_FAILURE;
        }

        // Wait for the command commands to get serviced before reading back results
        //
        clFinish(commands);

        // Read back the results from the device to verify the output
        //
        err = clEnqueueReadBuffer(commands, d_join_result_buffer,
        CL_TRUE, 0, sizeof(char) * count, &results[i * DATA_SIZE], 0, NULL,
        NULL);
        if (err != CL_SUCCESS) {
            printf("Error: Failed to read output array! %d\n", err);
            exit(1);
        }
    }

    clock_t end = clock();

    double elapsed_secs = (double) (end - begin) / CLOCKS_PER_SEC;
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
