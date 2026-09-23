#include "sdkconfig.h"
#if CONFIG_IDF_TARGET_LINUX
/*
 * Linux-simulator lock guards for the host Unity suite (test build only).
 *
 * The ESP-IDF linux FreeRTOS port runs each task on a pthread and switches
 * tasks from its SIGALRM tick handler, at any instruction. If the
 * switched-out thread holds a process-wide lock that FreeRTOS does not know
 * about, the next task that needs that lock blocks in the kernel and the
 * whole simulation stops. On the device FreeRTOS owns every lock, so this
 * cannot happen there. Three such locks are reached by this suite:
 *  - the ESP-IDF linux log lock (every ESP_LOGx call);
 *  - glibc's thread-stack lock (pthread_join reaping a deleted task, while
 *    xTaskCreate needs it in pthread_create);
 *  - the sanitizer allocator locks (see timeseries_audit_alloc.c).
 * Each is taken here with all signals blocked, as the port itself does
 * around pthread_create; the previous mask is restored afterwards.
 */
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

void tsdb_host_sim_block_signals(sigset_t *old) {
    sigset_t all;
    sigfillset(&all);
    pthread_sigmask(SIG_BLOCK, &all, old);
}

static _Thread_local sigset_t log_mask;
static _Thread_local int log_depth;
static void log_guard_enter(void) {
    sigset_t old;
    tsdb_host_sim_block_signals(&old);
    if (log_depth++ == 0) log_mask = old;
}
void __real_esp_log_impl_lock(void);
void __wrap_esp_log_impl_lock(void) {
    log_guard_enter();
    __real_esp_log_impl_lock();
}
bool __real_esp_log_impl_lock_timeout(void);
bool __wrap_esp_log_impl_lock_timeout(void) {
    log_guard_enter();
    bool locked = __real_esp_log_impl_lock_timeout();
    if (!locked && --log_depth == 0) pthread_sigmask(SIG_SETMASK, &log_mask, NULL);
    return locked;
}
void __real_esp_log_impl_unlock(void);
void __wrap_esp_log_impl_unlock(void) {
    __real_esp_log_impl_unlock();
    if (log_depth > 0 && --log_depth == 0) pthread_sigmask(SIG_SETMASK, &log_mask, NULL);
}

int __real_pthread_join(pthread_t thread, void **retval);
int __wrap_pthread_join(pthread_t thread, void **retval) {
    sigset_t old;
    tsdb_host_sim_block_signals(&old);
    int rc = __real_pthread_join(thread, retval);
    pthread_sigmask(SIG_SETMASK, &old, NULL);
    return rc;
}

void __real_free(void *ptr);
void __wrap_free(void *ptr) {
    sigset_t old;
    tsdb_host_sim_block_signals(&old);
    __real_free(ptr);
    pthread_sigmask(SIG_SETMASK, &old, NULL);
}
#endif
