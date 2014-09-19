## Process this file with automake to produce Makefile.in

TESTS = check_common
check_PROGRAMS = check_common
check_money_SOURCES = check_common.c $(top_builddir)/src/common.h
check_money_CFLAGS = @CHECK_CFLAGS@
check_money_LDADD = $(top_builddir)/src/libgpu.la @CHECK_LIBS@

