/* --------------------------------------------------------------------------
 *  lru.c  –  Minimal intrusive LRU list helper (no external dependencies)
 * --------------------------------------------------------------------------
 *  The list is intrusive: every cached object embeds an `lru_node_t` as the
 *  first (or any) member.  No heap allocation happens inside this module;
 *  it only rewires pointers.  This keeps per–entry overhead to two pointers
 *  (8 bytes on 32‑bit, 16 bytes on 64‑bit targets).
 *
 *  Typical embedding:
 *      typedef struct cache_entry {
 *          lru_node_t   lru;     // MUST be initialised via lru_node_init()
 *          … other members …
 *      } cache_entry_t;
 *
 *  Thread‑safety: none.  Caller must serialise concurrent access.
 * --------------------------------------------------------------------------*/

#include <stddef.h>
#include "lru.h"

/* ---- public list interface --------------------------------------------- */
void lru_list_init(lru_list_t* lst) {
  lst->head = lst->tail = NULL;
  lst->count = 0;
}

void lru_link_front(lru_list_t* lst, lru_node_t* n) {
  /* assume 'n' is not in any list */
  n->prev = NULL;
  n->next = lst->head;
  if (lst->head)
    lst->head->prev = n;
  else
    lst->tail = n; /* first element */
  lst->head = n;
  lst->count++;
}

void lru_unlink(lru_list_t* lst, lru_node_t* n) {
  if (n->prev)
    n->prev->next = n->next;
  else
    lst->head = n->next;

  if (n->next)
    n->next->prev = n->prev;
  else
    lst->tail = n->prev;

  n->prev = n->next = NULL;
  if (lst->count) lst->count--; /* caller guarantees n was in list */
}

void lru_move_front(lru_list_t* lst, lru_node_t* n) {
  if (lst->head == n) return; /* already MRU */
  lru_unlink(lst, n);
  lru_link_front(lst, n);
}

lru_node_t* lru_pop_tail(lru_list_t* lst) {
  lru_node_t* victim = lst->tail;
  if (victim) lru_unlink(lst, victim);
  return victim; /* NULL if list empty */
}
