#include "vega_sqlite.h"

#include <dlfcn.h>
#include <pthread.h>
#include <stddef.h>

/* SQLITE_TRANSIENT: the library copies the bytes before the call returns. */
#define TRANSIENT ((void (*)(void *))-1)

static struct {
    int (*libversion_number)(void);
    int (*threadsafe)(void);
    int (*open_v2)(const char *, vsq_db **, int, const char *);
    int (*close_v2)(vsq_db *);
    const char *(*errmsg)(vsq_db *);
    int (*extended_errcode)(vsq_db *);
    const char *(*errstr)(int);
    int (*extended_result_codes)(vsq_db *, int);
    int (*busy_timeout)(vsq_db *, int);
    int (*get_autocommit)(vsq_db *);
    int (*changes)(vsq_db *);
    int64_t (*changes64)(vsq_db *);
    int64_t (*last_insert_rowid)(vsq_db *);
    void (*interrupt)(vsq_db *);
    int (*sleep)(int);
    int (*prepare_v2)(vsq_db *, const char *, int, vsq_stmt **, const char **);
    int (*step)(vsq_stmt *);
    int (*reset)(vsq_stmt *);
    int (*clear_bindings)(vsq_stmt *);
    int (*finalize)(vsq_stmt *);
    int (*stmt_readonly)(vsq_stmt *);
    int (*bind_parameter_count)(vsq_stmt *);
    int (*bind_null)(vsq_stmt *, int);
    int (*bind_int64)(vsq_stmt *, int, int64_t);
    int (*bind_double)(vsq_stmt *, int, double);
    int (*bind_text)(vsq_stmt *, int, const char *, int, void (*)(void *));
    int (*bind_blob)(vsq_stmt *, int, const void *, int, void (*)(void *));
    int (*bind_zeroblob)(vsq_stmt *, int, int);
    int (*column_count)(vsq_stmt *);
    const char *(*column_name)(vsq_stmt *, int);
    int (*column_type)(vsq_stmt *, int);
    int64_t (*column_int64)(vsq_stmt *, int);
    double (*column_double)(vsq_stmt *, int);
    const unsigned char *(*column_text)(vsq_stmt *, int);
    const void *(*column_blob)(vsq_stmt *, int);
    int (*column_bytes)(vsq_stmt *, int);
} lib;

static pthread_once_t load_once = PTHREAD_ONCE_INIT;
static int loaded;

static void load(void) {
    static const char *const names[] = {
        "libsqlite3.so.0", "libsqlite3.so",
        "/usr/lib/libsqlite3.dylib", "libsqlite3.dylib",
        "/opt/homebrew/opt/sqlite/lib/libsqlite3.dylib",
        "/usr/local/opt/sqlite/lib/libsqlite3.dylib", NULL,
    };
    void *h = NULL;
    for (const char *const *name = names; *name && !h; name++) {
        h = dlopen(*name, RTLD_NOW | RTLD_LOCAL);
    }
    if (!h) return;

    int complete = 1;
#define REQUIRE(field, symbol) \
    do { \
        lib.field = dlsym(h, symbol); \
        if (!lib.field) complete = 0; \
    } while (0)
    REQUIRE(libversion_number, "sqlite3_libversion_number");
    REQUIRE(threadsafe, "sqlite3_threadsafe");
    REQUIRE(open_v2, "sqlite3_open_v2");
    REQUIRE(close_v2, "sqlite3_close_v2");
    REQUIRE(errmsg, "sqlite3_errmsg");
    REQUIRE(extended_errcode, "sqlite3_extended_errcode");
    REQUIRE(errstr, "sqlite3_errstr");
    REQUIRE(extended_result_codes, "sqlite3_extended_result_codes");
    REQUIRE(busy_timeout, "sqlite3_busy_timeout");
    REQUIRE(get_autocommit, "sqlite3_get_autocommit");
    REQUIRE(changes, "sqlite3_changes");
    REQUIRE(last_insert_rowid, "sqlite3_last_insert_rowid");
    REQUIRE(interrupt, "sqlite3_interrupt");
    REQUIRE(sleep, "sqlite3_sleep");
    REQUIRE(prepare_v2, "sqlite3_prepare_v2");
    REQUIRE(step, "sqlite3_step");
    REQUIRE(reset, "sqlite3_reset");
    REQUIRE(clear_bindings, "sqlite3_clear_bindings");
    REQUIRE(finalize, "sqlite3_finalize");
    REQUIRE(stmt_readonly, "sqlite3_stmt_readonly");
    REQUIRE(bind_parameter_count, "sqlite3_bind_parameter_count");
    REQUIRE(bind_null, "sqlite3_bind_null");
    REQUIRE(bind_int64, "sqlite3_bind_int64");
    REQUIRE(bind_double, "sqlite3_bind_double");
    REQUIRE(bind_text, "sqlite3_bind_text");
    REQUIRE(bind_blob, "sqlite3_bind_blob");
    REQUIRE(bind_zeroblob, "sqlite3_bind_zeroblob");
    REQUIRE(column_count, "sqlite3_column_count");
    REQUIRE(column_name, "sqlite3_column_name");
    REQUIRE(column_type, "sqlite3_column_type");
    REQUIRE(column_int64, "sqlite3_column_int64");
    REQUIRE(column_double, "sqlite3_column_double");
    REQUIRE(column_text, "sqlite3_column_text");
    REQUIRE(column_blob, "sqlite3_column_blob");
    REQUIRE(column_bytes, "sqlite3_column_bytes");
#undef REQUIRE
    /* 3.37 and later; sqlite3_changes counts to 2^31 before that. */
    lib.changes64 = dlsym(h, "sqlite3_changes64");

    /* A library built single-threaded has no locking at all, even around its
     * own global state, and connections on different threads would corrupt
     * it. Garuda uses each connection from one thread at a time, which
     * "multi-thread" mode (threadsafe 2) and "serialized" (1) both allow. */
    if (complete && lib.threadsafe() != 0) loaded = 1;
}

int vsq_available(void) {
    pthread_once(&load_once, load);
    return loaded;
}

int vsq_libversion_number(void) {
    return vsq_available() ? lib.libversion_number() : 0;
}

int vsq_open(const char *path, int flags, vsq_db **db) {
    return lib.open_v2(path, db, flags, NULL);
}

int vsq_close(vsq_db *db) { return lib.close_v2(db); }
const char *vsq_errmsg(vsq_db *db) { return lib.errmsg(db); }
int vsq_extended_errcode(vsq_db *db) { return lib.extended_errcode(db); }
const char *vsq_errstr(int code) { return lib.errstr(code); }
int vsq_extended_result_codes(vsq_db *db, int on) { return lib.extended_result_codes(db, on); }
int vsq_busy_timeout(vsq_db *db, int milliseconds) { return lib.busy_timeout(db, milliseconds); }
int vsq_get_autocommit(vsq_db *db) { return lib.get_autocommit(db); }

int64_t vsq_changes(vsq_db *db) {
    return lib.changes64 ? lib.changes64(db) : (int64_t)lib.changes(db);
}

int64_t vsq_last_insert_rowid(vsq_db *db) { return lib.last_insert_rowid(db); }
void vsq_interrupt(vsq_db *db) { lib.interrupt(db); }
void vsq_sleep(int milliseconds) { (void)lib.sleep(milliseconds); }

int vsq_prepare(vsq_db *db, const char *sql, int bytes, vsq_stmt **stmt, const char **tail) {
    return lib.prepare_v2(db, sql, bytes, stmt, tail);
}

int vsq_step(vsq_stmt *stmt) { return lib.step(stmt); }
int vsq_reset(vsq_stmt *stmt) { return lib.reset(stmt); }
int vsq_clear_bindings(vsq_stmt *stmt) { return lib.clear_bindings(stmt); }
int vsq_finalize(vsq_stmt *stmt) { return lib.finalize(stmt); }
int vsq_stmt_readonly(vsq_stmt *stmt) { return lib.stmt_readonly(stmt); }
int vsq_bind_parameter_count(vsq_stmt *stmt) { return lib.bind_parameter_count(stmt); }

int vsq_bind_null(vsq_stmt *stmt, int index) { return lib.bind_null(stmt, index); }
int vsq_bind_int64(vsq_stmt *stmt, int index, int64_t value) { return lib.bind_int64(stmt, index, value); }
int vsq_bind_double(vsq_stmt *stmt, int index, double value) { return lib.bind_double(stmt, index, value); }

int vsq_bind_text(vsq_stmt *stmt, int index, const char *text, int bytes) {
    /* A NULL pointer would bind SQL NULL, and an empty Swift string may hand
     * over no storage at all. */
    return lib.bind_text(stmt, index, bytes > 0 && text ? text : "", bytes > 0 ? bytes : 0, TRANSIENT);
}

int vsq_bind_blob(vsq_stmt *stmt, int index, const void *data, int bytes) {
    if (bytes <= 0 || !data) return lib.bind_zeroblob(stmt, index, 0);
    return lib.bind_blob(stmt, index, data, bytes, TRANSIENT);
}

int vsq_column_count(vsq_stmt *stmt) { return lib.column_count(stmt); }
const char *vsq_column_name(vsq_stmt *stmt, int column) { return lib.column_name(stmt, column); }
int vsq_column_type(vsq_stmt *stmt, int column) { return lib.column_type(stmt, column); }
int64_t vsq_column_int64(vsq_stmt *stmt, int column) { return lib.column_int64(stmt, column); }
double vsq_column_double(vsq_stmt *stmt, int column) { return lib.column_double(stmt, column); }
const unsigned char *vsq_column_text(vsq_stmt *stmt, int column) { return lib.column_text(stmt, column); }
const void *vsq_column_blob(vsq_stmt *stmt, int column) { return lib.column_blob(stmt, column); }
int vsq_column_bytes(vsq_stmt *stmt, int column) { return lib.column_bytes(stmt, column); }
