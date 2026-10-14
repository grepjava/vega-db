/*
 * SQLite, loaded at run time.
 *
 * The system's libsqlite3 -- winsqlite3.dll on Windows -- is loaded the
 * first time it is asked for, so building needs no SQLite headers and a
 * program that never opens a database never loads the library. Each vsq_
 * function calls the
 * library's function of the same name; where the library cannot be loaded,
 * vsq_available says so and nothing else may be called.
 *
 * The handles are the library's own, left opaque.
 */
#ifndef VEGA_SQLITE_H
#define VEGA_SQLITE_H

#include <stdint.h>

typedef struct vsq_db vsq_db;
typedef struct vsq_stmt vsq_stmt;

/* Result codes. The rest are passed through as numbers. */
#define VSQ_OK 0
#define VSQ_ERROR 1
#define VSQ_BUSY 5
#define VSQ_LOCKED 6
#define VSQ_NOMEM 7
#define VSQ_READONLY 8
#define VSQ_INTERRUPT 9
#define VSQ_CONSTRAINT 19
#define VSQ_MISUSE 21
#define VSQ_RANGE 25
#define VSQ_ROW 100
#define VSQ_DONE 101

/* Open flags. */
#define VSQ_OPEN_READONLY 0x00000001
#define VSQ_OPEN_READWRITE 0x00000002
#define VSQ_OPEN_CREATE 0x00000004
#define VSQ_OPEN_NOMUTEX 0x00008000
#define VSQ_OPEN_PRIVATECACHE 0x00040000

/* Column types. */
#define VSQ_INTEGER 1
#define VSQ_FLOAT 2
#define VSQ_TEXT 3
#define VSQ_BLOB 4
#define VSQ_NULL 5

/* 1 when the library is loaded, has every function used here, and was built
 * to be used from more than one thread. */
int vsq_available(void);
/* The library's version as a number: 3045001 for 3.45.1. 0 if not loaded. */
int vsq_libversion_number(void);

/* A handle may come back with an error, and must then still be closed. */
int vsq_open(const char *path, int flags, vsq_db **db);
int vsq_close(vsq_db *db);
const char *vsq_errmsg(vsq_db *db);
int vsq_extended_errcode(vsq_db *db);
const char *vsq_errstr(int code);
int vsq_extended_result_codes(vsq_db *db, int on);
int vsq_busy_timeout(vsq_db *db, int milliseconds);
int vsq_get_autocommit(vsq_db *db);
int64_t vsq_changes(vsq_db *db);
int64_t vsq_last_insert_rowid(vsq_db *db);
void vsq_interrupt(vsq_db *db);
/* Sleeps the calling thread for at least `milliseconds`. */
void vsq_sleep(int milliseconds);

/* `tail` is where the first statement ended, within `sql`. */
int vsq_prepare(vsq_db *db, const char *sql, int bytes, vsq_stmt **stmt, const char **tail);
int vsq_step(vsq_stmt *stmt);
int vsq_reset(vsq_stmt *stmt);
int vsq_clear_bindings(vsq_stmt *stmt);
int vsq_finalize(vsq_stmt *stmt);
int vsq_stmt_readonly(vsq_stmt *stmt);
int vsq_bind_parameter_count(vsq_stmt *stmt);

/* Text and blobs are copied. An empty one binds as empty, not as NULL. */
int vsq_bind_null(vsq_stmt *stmt, int index);
int vsq_bind_int64(vsq_stmt *stmt, int index, int64_t value);
int vsq_bind_double(vsq_stmt *stmt, int index, double value);
int vsq_bind_text(vsq_stmt *stmt, int index, const char *text, int bytes);
int vsq_bind_blob(vsq_stmt *stmt, int index, const void *data, int bytes);

int vsq_column_count(vsq_stmt *stmt);
const char *vsq_column_name(vsq_stmt *stmt, int column);
int vsq_column_type(vsq_stmt *stmt, int column);
int64_t vsq_column_int64(vsq_stmt *stmt, int column);
double vsq_column_double(vsq_stmt *stmt, int column);
/* Call before vsq_column_bytes, as the library asks. */
const unsigned char *vsq_column_text(vsq_stmt *stmt, int column);
const void *vsq_column_blob(vsq_stmt *stmt, int column);
int vsq_column_bytes(vsq_stmt *stmt, int column);

#endif
