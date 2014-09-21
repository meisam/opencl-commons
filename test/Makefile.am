## Process this file with automake to produce Makefile.in

TESTS = check_gpujoin
check_PROGRAMS = check_gpujoin
check_gpujoin_SOURCES = check_common.c $(top_builddir)/src/common.h
check_gpujoin_CFLAGS = @CHECK_CFLAGS@
check_gpujoin_LDADD = $(top_builddir)/src/libgpujoin.la @CHECK_LIBS@ -lm -lpthread -lrt
