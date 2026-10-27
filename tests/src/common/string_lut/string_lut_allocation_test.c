/* Copyright 2026 NIBE AB. Licensed under the Apache License, Version 2.0. */
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include "src/common/eebus_malloc.h"
static size_t calls, live, fail_at, deleted;
static void* Allocate(size_t size) {
  if (++calls == fail_at) return NULL;
  void* result = malloc(size);
  if (result != NULL) ++live;
  return result;
}
static void Release(void* ptr) {
  if (ptr != NULL) --live;
  free(ptr);
}
#undef EEBUS_MALLOC
#undef EEBUS_FREE
#define EEBUS_MALLOC(size) Allocate(size)
#define EEBUS_FREE(ptr) Release(ptr)
#include "src/common/vector.c"
#ifndef STRING_LUT_SOURCE
#define STRING_LUT_SOURCE "src/common/string_lut.c"
#endif
#include STRING_LUT_SOURCE
char* StringCopy(const char* src) {
  size_t size  = strlen(src) + 1;
  char* result = Allocate(size);
  if (result != NULL) memcpy(result, src, size);
  return result;
}
static void DeleteValue(void* value) {
  (void)value;
  ++deleted;
}
int main(void) {
  for (size_t failure = 1; failure <= 3; ++failure) {
    StringLut lut;
    StringLutInit(&lut);
    int value = 7;
    calls     = 0;
    fail_at   = failure;
    assert(StringLutInsert(&lut, "peer", &value, DeleteValue) == kEebusErrorMemoryAllocate);
    assert(StringLutGetSize(&lut) == 0 && deleted == 0 && live == 0);
    fail_at = 0;
    assert(StringLutInsert(&lut, "peer", &value, DeleteValue) == kEebusErrorOk);
    assert(StringLutFind(&lut, "peer") == &value);
    StringLutRelease(&lut);
    assert(live == 0 && deleted == 1);
    deleted = 0;
  }
  return 0;
}
