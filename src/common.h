/*
 * common.h
 *
 *  Created on: Sep 12, 2014
 *      Author: fathi
 */

#ifndef COMMON_H_
#define COMMON_H_

#include <CL/cl.h>

#define DEBUG 0

#define log_debug(msg)                       \
if (DEBUG) {                                 \
    printf("[DEBUG]: ");                     \
    printf((msg));                           \
    printf("\n");                            \
}                                            \

#define log_debug2(msg, code)                \
if (DEBUG) {                                 \
    printf("[DEBUG]: ");                     \
    printf((msg), (code));                   \
    printf("\n");                            \
}                                            \

cl_device_id device_id;                   // compute device id
cl_context context;                       // compute context
cl_command_queue commands;                // compute command queue
cl_program program;                       // compute program
cl_kernel kernel;                         // compute kernel
size_t global;                 // global domain size for our calculation
size_t local;                   // local domain size for our calculation

int read_file(char *file_path, char **file_buffer, size_t *file_size);

int prepare_device(char* kernel_path, char * kernel);

#endif /* COMMON_H_ */
