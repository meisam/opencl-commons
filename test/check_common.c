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

int main(void) {
    int number_failed;
    Suite *s = common_suite();
    SRunner *sr = srunner_create(s);
    srunner_run_all(sr, CK_NORMAL);
    number_failed = srunner_ntests_failed(sr);
    srunner_free(sr);
    return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
