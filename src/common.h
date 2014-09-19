/*
 * common.h
 *
 *  Created on: Sep 12, 2014
 *      Author: fathi
 */

#ifndef COMMON_H_
#define COMMON_H_

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

int read_file(char *file_path, char **file_buffer, size_t *file_size);

int prepare_device(int argc, char** argv);

#endif /* COMMON_H_ */
