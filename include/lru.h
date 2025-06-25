/* --------------------------------------------------------------------------
 *  lru.h  –  Public interface for the minimal intrusive LRU helper
 * --------------------------------------------------------------------------
 *  Embed `lru_node_t` in your own struct and manage it through the
 *  functions below.  No allocations are performed inside the helper –
 *  you own the memory, the helper only rewires pointers.
 *
 *      typedef struct my_entry {
 *          lru_node_t lru;
 *          ... other fields ...
 *      } my_entry_t;
 *
 *  Thread‑safety: none.  Call‑site must provide serialisation
 *  (e.g. mutex, critical section) when used from multiple tasks.
 * --------------------------------------------------------------------------*/

#ifndef LRU_H
#define LRU_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* --------------------------------------------------------------------------
 *  Intrusive node and list definitions
 * --------------------------------------------------------------------------*/

typedef struct lru_node_t {
  struct lru_node_t* prev;
  struct lru_node_t* next;
} lru_node_t;

/* Doubly‑linked list head/tail wrapper with a simple counter (optional). */
typedef struct {
  lru_node_t* head; /* Most‑recently‑used (MRU) */
  lru_node_t* tail; /* Least‑recently‑used (LRU) */
  size_t count;     /* number of elements, kept for stats */
} lru_list_t;

/* --------------------------------------------------------------------------
 *  Inline convenience to initialise a standalone node
 * --------------------------------------------------------------------------*/
static inline void lru_node_init(lru_node_t* n) { n->prev = n->next = NULL; }

/* --------------------------------------------------------------------------
 *  API – all operate in O(1)
 * --------------------------------------------------------------------------*/

/* Reset an LRU list to empty state */
void lru_list_init(lru_list_t* lst);

/* Insert `n` at the MRU end.  Caller guarantees `n` is detached. */
void lru_link_front(lru_list_t* lst, lru_node_t* n);

/* Detach `n` from whichever list it is on; no‑op if already detached. */
void lru_unlink(lru_list_t* lst, lru_node_t* n);

/* Promote `n` to MRU if it is already in `lst`. */
void lru_move_front(lru_list_t* lst, lru_node_t* n);

/* Remove and return the LRU element; returns NULL if list is empty. */
lru_node_t* lru_pop_tail(lru_list_t* lst);

#ifdef __cplusplus
}
#endif

#endif /* LRU_H */
