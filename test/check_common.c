/*
 * test_common.c
 *
 *  Created on: Sep 19, 2014
 *      Author: fathi
 */

#include <stdlib.h>
#include <check.h>
#include "../src/common.h"
#include "../src/join_driver.c"

void setup(void) {
    // don't do anything
}

void teardown(void) {
    // don't do anything
}

START_TEST (sanity_check)
    {
        fail_if(1 != 1);
    }END_TEST

START_TEST (read_empty_file_check)
    {
        size_t size = -1;
        char ** content = (char **) malloc(sizeof(char *));
        read_file("test-data/empty-file", content, &size);
        ck_assert_int_eq(size, 0);
        ck_assert_str_eq(*content, "");
    }END_TEST

START_TEST (read_small_file_check)
    {
        size_t size = -1;
        char ** content = (char **) malloc(sizeof(char *));
        read_file("test-data/small-file", content, &size);
        ck_assert_int_eq(size, 1);
        ck_assert_str_eq(*content, "a");
    }END_TEST

START_TEST (read_alphabet_file_check)
    {
        size_t size = -1;
        char ** content = (char **) malloc(sizeof(char *));
        read_file("test-data/alphabet-file", content, &size);
        ck_assert_int_eq(size, 26);
        ck_assert_str_eq(*content, "abcdefghijklmnopqrstuvwxyz");
    }END_TEST

Suite *common_suite(void) {
    Suite *s = suite_create("Common");

    /* Core test case */
    TCase *tc_core = tcase_create("Core");
    tcase_add_checked_fixture(tc_core, setup, teardown);
    tcase_add_test(tc_core, sanity_check);
    tcase_add_test(tc_core, read_empty_file_check);
    tcase_add_test(tc_core, read_small_file_check);
    tcase_add_test(tc_core, read_alphabet_file_check);
    suite_add_tcase(s, tc_core);

    return s;
}
#define MAX_HASH_VALUE (1<<12)

/// A crude and naive hash implementation
int naive_hash(const int val) {
    return (val * 8191) % MAX_HASH_VALUE; // other values to use: 127, 4047, 8191
}

START_TEST (gpu_sanity_check)
    {
        fail_if(1 != 1);
    }END_TEST

START_TEST (gpu_hash_check)
    {
        ck_assert_int_eq(prepare_device("../src/join_kernel.cl", "hash"), 0);

        int data[4] = { 1, 2, 3, 4 };
        log_debug("created data");
        int * hashed_data;
        hashed_data = (int *) malloc(sizeof(int) * 4);
        log_debug("created pointer to hash table");
        log_debug("allocated hash table");
        hashed_data[0] = 100;
        hashed_data[1] = 120;
        hashed_data[2] = 120;
        hashed_data[3] = 130;
        log_debug("initialized hash table");
        int size = -1;
        build_hash_table(4, data, 4, hashed_data);
        log_debug("called build_hash_table");

        ck_assert_int_eq(hashed_data[0], naive_hash(data[0]));
        ck_assert_int_eq(hashed_data[1], naive_hash(data[1]));
        ck_assert_int_eq(hashed_data[2], naive_hash(data[2]));
        ck_assert_int_eq(hashed_data[3], naive_hash(data[3]));
        log_debug("checked all assertions");

    }END_TEST

Suite *gpu_suite(void) {
    Suite *s = suite_create("GPU");

    /* Core test case */
    TCase *tc_gpu = tcase_create("GPU");
    tcase_add_checked_fixture(tc_gpu, setup, teardown);
    tcase_add_test(tc_gpu, gpu_sanity_check);
    tcase_add_test(tc_gpu, gpu_hash_check);
    suite_add_tcase(s, tc_gpu);

    return s;
}

int main(void) {
    int number_failed;
    Suite *common_suit = common_suite();
    SRunner *sr = srunner_create(common_suit);
    Suite *gpu_suit = gpu_suite();
    srunner_add_suite(sr, gpu_suit);

    srunner_run_all(sr, CK_VERBOSE);
    number_failed = srunner_ntests_failed(sr);
    srunner_free(sr);
    return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
