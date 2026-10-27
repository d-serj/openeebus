/* Copyright 2026 NIBE AB. Licensed under the Apache License, Version 2.0. */
#ifndef SRC_COMMON_OUTPUT_RECORD_H_
#define SRC_COMMON_OUTPUT_RECORD_H_

#include <stdio.h>

/* stdout's recursive stream lock also excludes ordinary printf callers. */
static inline void OutputRecordBegin(void) {
#ifdef _WIN32
  _lock_file(stdout);
#else
  flockfile(stdout);
#endif
}

static inline void OutputRecordEnd(void) {
  fflush(stdout);
#ifdef _WIN32
  _unlock_file(stdout);
#else
  funlockfile(stdout);
#endif
}

#endif  // SRC_COMMON_OUTPUT_RECORD_H_
