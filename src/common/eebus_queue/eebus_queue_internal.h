/* Copyright 2026 NIBE AB. Licensed under the Apache License, Version 2.0. */
#ifndef SRC_COMMON_EEBUS_QUEUE_EEBUS_QUEUE_INTERNAL_H_
#define SRC_COMMON_EEBUS_QUEUE_EEBUS_QUEUE_INTERNAL_H_
#include "src/common/api/eebus_queue_interface.h"
#ifdef __cplusplus
extern "C" {
#endif
/** Close a concrete queue without destroying it. Join users before deletion. */
void EebusQueueClose(EebusQueueObject* queue);
#ifdef __cplusplus
}
#endif
#endif  // SRC_COMMON_EEBUS_QUEUE_EEBUS_QUEUE_INTERNAL_H_
