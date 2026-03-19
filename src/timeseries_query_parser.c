#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include "timeseries_query_parser.h"

#define ENSURE(cond)             \
  do {                           \
    if (!(cond)) {               \
      err = ESP_ERR_INVALID_ARG; \
      goto fail;                 \
    }                            \
  } while (0)

/* local helpers --------------------------------------------------------- */
static void skip_ws(const char** p) {
  while (isspace((unsigned char)**p)) ++(*p);
}

static char* dup_n(const char* s, size_t n) {
#if defined(_POSIX_C_SOURCE) && _POSIX_C_SOURCE >= 200809L
  return strndup(s, n);
#else
  char* p = malloc(n + 1);
  if (p) {
    memcpy(p, s, n);
    p[n] = '\0';
  }
  return p;
#endif
}

#define TSDB_PARSER_MAX_FIELDS 64
#define TSDB_PARSER_MAX_TAGS   32

static esp_err_t push(const char*** arr, size_t* len, size_t max_len, const char* s) {
  if (*len >= max_len) return ESP_ERR_INVALID_ARG;
  const char** tmp = realloc(*arr, (*len + 1) * sizeof(char*));
  if (!tmp) return ESP_ERR_NO_MEM;
  tmp[*len] = s;
  *arr = tmp;
  (*len)++;
  return ESP_OK;
}

/* local: map method string to aggregation enum */
static timeseries_aggregation_method_e parse_agg_method(const char* s, size_t len) {
  if (len == 3 && strncasecmp(s, "avg", 3) == 0) return TSDB_AGGREGATION_AVG;
  if (len == 4 && strncasecmp(s, "mean", 4) == 0) return TSDB_AGGREGATION_AVG;
  if (len == 3 && strncasecmp(s, "sum", 3) == 0) return TSDB_AGGREGATION_SUM;
  if (len == 3 && strncasecmp(s, "min", 3) == 0) return TSDB_AGGREGATION_MIN;
  if (len == 3 && strncasecmp(s, "max", 3) == 0) return TSDB_AGGREGATION_MAX;
  if (len == 5 && strncasecmp(s, "count", 5) == 0) return TSDB_AGGREGATION_COUNT;
  if (len == 4 && strncasecmp(s, "last", 4) == 0) return TSDB_AGGREGATION_LAST;
  return TSDB_AGGREGATION_NONE;
}

/* public API ------------------------------------------------------------ */
esp_err_t tsdb_query_string_parse(const char* q, timeseries_query_t* out) {
  if (!q || !out) return ESP_ERR_INVALID_ARG;
  memset(out, 0, sizeof(*out));
  esp_err_t err = ESP_OK;
  const char* p = q;

  /* 1) aggregation method */
  const char* colon = strchr(p, ':');
  ENSURE(colon && colon != p);
  out->aggregate_method = parse_agg_method(p, (size_t)(colon - p));
  p = colon + 1;

  /* 2) measurement name */
  const char* paren = strchr(p, '(');
  ENSURE(paren && paren != p);
  out->measurement_name = dup_n(p, (size_t)(paren - p));
  ENSURE(out->measurement_name);
  p = paren + 1;

  /* 3) field list */
  const char* rparen = strchr(p, ')');
  ENSURE(rparen);
  if (rparen != p) {
    const char* s = p;
    while (s < rparen) {
      const char* comma = memchr(s, ',', (size_t)(rparen - s));
      const char* end = comma ? comma : rparen;

      while (s < end && isspace((unsigned char)*s)) ++s;
      const char* e = end;
      while (e > s && isspace((unsigned char)*(e - 1))) --e;
      if (e > s) {
        char* token = dup_n(s, (size_t)(e - s));
        ENSURE(token);
        err = push(&out->field_names, &out->num_fields, TSDB_PARSER_MAX_FIELDS, token);
        if (err) { free(token); goto fail; }
      }
      if (!comma) break;
      s = comma + 1;
    }
  }
  p = rparen + 1;

  /* 4) optional tag scope */
  skip_ws(&p);
  if (*p == '{') {
    ++p;
    const char* brace = strchr(p, '}');
    ENSURE(brace);
    const char* s = p;
    while (s < brace) {
      const char* comma = memchr(s, ',', (size_t)(brace - s));
      const char* end = comma ? comma : brace;
      const char* mid = memchr(s, ':', (size_t)(end - s));
      ENSURE(mid);

      const char* k1 = s;
      const char* k2 = mid;
      while (k1 < k2 && isspace((unsigned char)*k1)) ++k1;
      while (k2 > k1 && isspace((unsigned char)*(k2 - 1))) --k2;

      const char* v1 = mid + 1;
      while (v1 < end && isspace((unsigned char)*v1)) ++v1;
      const char* v2 = end;
      while (v2 > v1 && isspace((unsigned char)*(v2 - 1))) --v2;

      ENSURE(k2 > k1 && v2 > v1);

      char* key = dup_n(k1, (size_t)(k2 - k1));
      char* val = dup_n(v1, (size_t)(v2 - v1));
      if (!key || !val) {
        free(key);
        free(val);
        err = ESP_ERR_INVALID_ARG;
        goto fail;
      }

      /* grow both arrays to size num_tags+1, then store the pair */
      if (out->num_tags >= TSDB_PARSER_MAX_TAGS) {
        free(key);
        free(val);
        err = ESP_ERR_INVALID_ARG;
        goto fail;
      }
      const char** new_keys = realloc(out->tag_keys, (out->num_tags + 1) * sizeof(char*));
      if (!new_keys) {
        free(key);
        free(val);
        err = ESP_ERR_NO_MEM;
        goto fail;
      }
      out->tag_keys = new_keys;

      const char** new_vals = realloc(out->tag_values, (out->num_tags + 1) * sizeof(char*));
      if (!new_vals) {
        free(key);
        free(val);
        err = ESP_ERR_NO_MEM;
        goto fail;
      }
      out->tag_values = new_vals;

      out->tag_keys[out->num_tags] = key;
      out->tag_values[out->num_tags] = val;
      out->num_tags++; /* ← increment exactly once */

      if (!comma) break;
      s = comma + 1;
    }
    p = brace + 1;
  }

  /* 5) nothing but whitespace should remain */
  skip_ws(&p);
  ENSURE(*p == '\0');

  return ESP_OK;

fail:
  tsdb_query_string_free(out);
  if (err == ESP_OK) err = ESP_ERR_INVALID_ARG;
  return err;
}

void tsdb_query_string_free(timeseries_query_t* q) {
  for (size_t i = 0; i < q->num_tags; ++i) {
    free((void*)q->tag_keys[i]);
    free((void*)q->tag_values[i]);
  }
  for (size_t i = 0; i < q->num_fields; ++i) {
    free((void*)q->field_names[i]);
  }
  free((void*)q->tag_keys);
  free((void*)q->tag_values);
  free((void*)q->field_names);
  free((void*)q->measurement_name);
  memset(q, 0, sizeof(*q));
}
