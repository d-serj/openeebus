/* Copyright 2026 NIBE AB. Licensed under the Apache License, Version 2.0. */
#include <assert.h>
#include <stdbool.h>
#include "src/common/eebus_malloc.h"
static bool fail_allocation;
static void* Allocate(size_t size) {
  return fail_allocation ? NULL : malloc(size);
}
#undef EEBUS_MALLOC
#define EEBUS_MALLOC(size) Allocate(size)
#ifndef VECTOR_SOURCE
#define VECTOR_SOURCE "src/common/vector.c"
#endif
#include VECTOR_SOURCE
int main(void) {
  Vector vector;
  VectorConstruct(&vector);
  int first = 1, second = 2;
  VectorPushBack(&vector, &first);
  assert(VectorGetSize(&vector) == 1);
  fail_allocation = true;
  VectorPushBack(&vector, &second);
  assert(VectorGetSize(&vector) == 1 && VectorGetElement(&vector, 0) == &first);
  fail_allocation = false;
  VectorPushBack(&vector, &second);
  assert(VectorGetSize(&vector) == 2 && VectorGetElement(&vector, 1) == &second);
  VectorDestruct(&vector);
  return 0;
}
