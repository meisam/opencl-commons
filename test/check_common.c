/*
 * test_common.c
 *
 *  Created on: Sep 19, 2014
 *      Author: fathi
 */

#include <stdlib.h>
#include <check.h>

void setup(void) {
    // don't do anything
}

void teardown(void) {
    // don't do anything
}

START_TEST (sanity_chech) {
        fail_if(1 != 1);
}
END_TEST

Suite *common_suite(void) {
    Suite *s = suite_create("Common");

    /* Core test case */
    TCase *tc_core = tcase_create("Core");
    tcase_add_checked_fixture(tc_core, setup, teardown);
    tcase_add_test(tc_core, sanity_chech);
    suite_add_tcase(s, tc_core);

    return s;
}

START_TEST (gpu_sanity_check)
    {
        fail_if(1 != 1);
    }END_TEST

START_TEST (gpu_hash_check)
    {
        int data[4] = { 1, 2, 3, 4 };
        int hashed_data[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
        int size = -1;
        build_hash_table(4, data, 8, hashed_data);
        ck_assert_int_eq(hashed_data[0], 2);
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
    Suite *s = common_suite();
    SRunner *sr = srunner_create(s);
    srunner_run_all(sr, CK_NORMAL);
    number_failed = srunner_ntests_failed(sr);
    srunner_free(sr);
    return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
