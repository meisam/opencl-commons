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



