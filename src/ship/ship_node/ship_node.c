/*
 * Copyright 2025 NIBE AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "node_connection_container.h"
#include "ship_node_internal.h"
#include "src/common/eebus_arguments.h"
#include "src/common/eebus_device_info.h"
#include "src/common/eebus_malloc.h"
#include "src/common/eebus_mutex/eebus_mutex.h"
#include "src/common/eebus_queue/eebus_queue.h"
#include "src/common/eebus_thread/eebus_thread.h"
#include "src/common/service_details.h"
#include "src/common/string_util.h"
#include "src/common/vector.h"
#include "src/ship/api/http_server_interface.h"
#include "src/ship/api/ship_node_interface.h"
#include "src/ship/api/ship_node_reader_interface.h"
#include "src/ship/api/tls_certificate_interface.h"
#include "src/ship/mdns/ship_mdns.h"
#include "src/ship/ship_connection/ship_connection.h"
#include "src/ship/websocket/http_server.h"
#include "src/ship/websocket/websocket_client_creator.h"

/** Set SHIP_NODE_DEBUG 1 to enable debug prints */
#ifndef SHIP_NODE_DEBUG
#define SHIP_NODE_DEBUG 0
#endif

/** Ship node debug printf(), enabled whith SHIP_NODE_DEBUG = 1 */
#if SHIP_NODE_DEBUG
#define SHIP_NODE_DEBUG_PRINTF(fmt, ...) DebugPrintf(fmt, ##__VA_ARGS__)
#else
#define SHIP_NODE_DEBUG_PRINTF(fmt, ...)
#endif  // SHIP_NODE_DEBUG

#include "src/common/eebus_queue/eebus_queue_internal.h"
#include "src/ship/ship_node/node_connection_internal.h"

typedef enum { kAttemptPending, kAttemptEstablished, kAttemptReplacing, kAttemptRetiring } AttemptState;
typedef struct ShipNodeAttempt {
  InfoProviderObject provider;
  ShipNode* owner;
  NodeConnection* peer;
  uint64_t id;
  ShipConnectionObject* connection;
  ShipRole role;
  AttemptState lifecycle;
  bool start_done;
  EebusError start_result;
  bool closed;
  bool state_pending;
  SmeState state;
  bool setup_pending;
  bool application_setup;
  bool connected_published;
  DataWriterObject* writer;
  EebusQueueObject* setup_reply;
  char* ship_id;
} ShipNodeAttempt;

typedef struct {
  int type;
} ShipNodeQueueMessage;
enum { kShipNodeQueueMsgTypeCancel, kShipNodeQueueMsgTypeMdnsEntriesFound };

static void Destruct(InfoProviderObject* self);
static void Start(ShipNodeObject* self);
static void Stop(ShipNodeObject* self);
static void RegisterRemoteSki(ShipNodeObject* self, const char* ski, bool trusted);
static void UnregisterRemoteSki(ShipNodeObject* self, const char* ski);
static void CancelPairingWithSki(ShipNodeObject* self, const char* ski);
static void ShipNodeOnMdnsEntriesFoundCallback(Vector* entries, void* ctx);
static void ShipNodeRetryTimerCallback(void* ctx);
static bool ShipNodeIsServerSupported(ShipNode* self);
static bool ShipNodeIsClientSupported(ShipNode* self);
static int ShipNodeOnWebsocketServerConnectionCallback(const char*, WebsocketCreatorObject*, void*);
static void ShipNodeQueueMsgDeallocator(void* msg) {
  UNUSED(msg);
}

static void WakeNode(ShipNode* node) {
  const ShipNodeQueueMessage wake = {0};
  EEBUS_QUEUE_SEND(node->msg_queue, &wake, 0);
}

static bool IsRemoteServiceForSkiPaired(InfoProviderObject* self, const char* ski) {
  UNUSED(self);
  UNUSED(ski);
  return false;
}
static bool IsWaitingForTrustAllowed(InfoProviderObject* self, const char* ski) {
  ShipNode* node = SHIP_NODE(self);
  return SHIP_NODE_READER_IS_WAITING_FOR_TRUST_ALLOWED(node->ship_node_reader, ski);
}
/* Owned connections use per-attempt providers. Keep the unscoped reader forwarding entry points. */
static void HandleConnectionClosed(InfoProviderObject* self, ShipConnectionObject* sc, bool error) {
  UNUSED(error);
  ShipNode* node = SHIP_NODE(self);
  EEBUS_MUTEX_LOCK(node->mutex);
  for (size_t i = 0; i < NODE_CONNECTION_CONTAINER_GET_SIZE(node->connections); ++i) {
    NodeConnection* peer        = NODE_CONNECTION(NODE_CONNECTION_CONTAINER_GET_WITH_INDEX(node->connections, i));
    ShipNodeAttempt* attempts[] = {peer->current, peer->replacement};
    for (size_t j = 0; j < 2; ++j)
      if (attempts[j] != NULL && attempts[j]->connection == sc) attempts[j]->closed = true;
  }
  EEBUS_MUTEX_UNLOCK(node->mutex);
  WakeNode(node);
}
static void ReportServiceShipId(InfoProviderObject* self, const char* ski, const char* id) {
  SHIP_NODE_READER_ON_SHIP_ID_UPDATE(SHIP_NODE(self)->ship_node_reader, ski, id);
}
static void HandleShipStateUpdate(InfoProviderObject* self, const char* ski, SmeState state, const char* err) {
  UNUSED(err);
  SHIP_NODE_READER_ON_SHIP_STATE_UPDATE(SHIP_NODE(self)->ship_node_reader, ski, state);
}
static DataReaderObject* SetupRemoteDevice(InfoProviderObject* self, const char* ski, DataWriterObject* writer) {
  return SHIP_NODE_READER_SETUP_REMOTE_DEVICE(SHIP_NODE(self)->ship_node_reader, ski, writer);
}
static const ShipNodeInterface ship_node_methods = {
    .info_provider_interface
    = {Destruct,
       IsRemoteServiceForSkiPaired, HandleConnectionClosed,
       ReportServiceShipId, IsWaitingForTrustAllowed,
       HandleShipStateUpdate, SetupRemoteDevice},
    .start                   = Start,
    .stop                    = Stop,
    .register_remote_ski     = RegisterRemoteSki,
    .unregister_remote_ski   = UnregisterRemoteSki,
    .cancel_pairing_with_ski = CancelPairingWithSki,
};

static void AdapterDestruct(InfoProviderObject* self) {
  UNUSED(self);
}
static bool AdapterPaired(InfoProviderObject* self, const char* ski) {
  return IsRemoteServiceForSkiPaired(INFO_PROVIDER_OBJECT(((ShipNodeAttempt*)self)->owner), ski);
}
static bool AdapterTrust(InfoProviderObject* self, const char* ski) {
  return IsWaitingForTrustAllowed(INFO_PROVIDER_OBJECT(((ShipNodeAttempt*)self)->owner), ski);
}
static void AdapterClosed(InfoProviderObject* self, ShipConnectionObject* connection, bool error) {
  UNUSED(connection);
  UNUSED(error);
  ShipNodeAttempt* a = (ShipNodeAttempt*)self;
  EEBUS_MUTEX_LOCK(a->owner->mutex);
  a->closed = true;
  EEBUS_MUTEX_UNLOCK(a->owner->mutex);
  WakeNode(a->owner);
}
static void AdapterId(InfoProviderObject* self, const char* ski, const char* id) {
  UNUSED(ski);
  ShipNodeAttempt* a = (ShipNodeAttempt*)self;
  char* copy         = StringCopy(id);
  EEBUS_MUTEX_LOCK(a->owner->mutex);
  StringDelete(a->ship_id);
  a->ship_id = copy;
  EEBUS_MUTEX_UNLOCK(a->owner->mutex);
  WakeNode(a->owner);
}
static void AdapterState(InfoProviderObject* self, const char* ski, SmeState state, const char* err) {
  UNUSED(ski);
  UNUSED(err);
  ShipNodeAttempt* a = (ShipNodeAttempt*)self;
  EEBUS_MUTEX_LOCK(a->owner->mutex);
  a->state         = state;
  a->state_pending = true;
  EEBUS_MUTEX_UNLOCK(a->owner->mutex);
  WakeNode(a->owner);
}
static DataReaderObject* AdapterSetup(InfoProviderObject* self, const char* ski, DataWriterObject* writer) {
  UNUSED(ski);
  ShipNodeAttempt* a = (ShipNodeAttempt*)self;
  EEBUS_MUTEX_LOCK(a->owner->mutex);
  bool valid = !a->owner->cancel && !a->peer->removing && a->lifecycle != kAttemptRetiring;
  if (valid) {
    a->writer        = writer;
    a->setup_pending = true;
  }
  EEBUS_MUTEX_UNLOCK(a->owner->mutex);
  if (!valid) return NULL;
  WakeNode(a->owner);
  DataReaderObject* reader = NULL;
  EEBUS_QUEUE_RECEIVE(a->setup_reply, &reader, kTimeoutInfinite);
  return reader;
}
static const InfoProviderInterface attempt_provider = {
    AdapterDestruct,
    AdapterPaired,
    AdapterClosed,
    AdapterId,
    AdapterTrust,
    AdapterState,
    AdapterSetup,
};

static ShipNodeAttempt* ReserveAttempt(ShipNode* node, NodeConnection* peer, ShipRole role) {
  /* Caller holds node->mutex. A candidate and a retiring connection share the second slot. */
  if (peer->replacement != NULL || peer->removing || node->cancel || node->next_attempt_id == UINT64_MAX) return NULL;
  ShipNodeAttempt* a = EEBUS_MALLOC(sizeof(*a));
  if (a == NULL) return NULL;
  *a = (ShipNodeAttempt){
      .provider  = {&attempt_provider},
      .owner     = node,
      .peer      = peer,
      .id        = ++node->next_attempt_id,
      .role      = role,
      .lifecycle = kAttemptReplacing
  };
  a->setup_reply = EebusQueueCreate(1, sizeof(DataReaderObject*), NULL);
  if (a->setup_reply == NULL) {
    EEBUS_FREE(a);
    return NULL;
  }
  peer->replacement      = a;
  peer->retry_due        = false;
  peer->retry_generation = a->id;
  if (role == kShipRoleClient) peer->collision_yields = 0;
  return a;
}

static EebusError StartAttempt(ShipNodeAttempt* a, WebsocketCreatorObject* creator) {
  a->connection
      = ShipConnectionCreate(&a->provider, a->role, a->owner->local_service_details->ship_id, a->peer->ski, "");
  EebusError err = a->connection == NULL ? kEebusErrorMemoryAllocate
                   : creator == NULL     ? kEebusErrorInit
                                         : SHIP_CONNECTION_START(a->connection, creator);
  EEBUS_MUTEX_LOCK(a->owner->mutex);
  a->start_result = err;
  a->start_done   = true;
  EEBUS_MUTEX_UNLOCK(a->owner->mutex);
  WakeNode(a->owner);
  return err;
}

static void RetireAttempt(ShipNodeAttempt* a) {
  if (a == NULL) return;
  /* Resolve synchronous setup before joining its caller. The adapter survives all callbacks. */
  EebusQueueClose(a->setup_reply);
  if (a->connection != NULL) {
    SHIP_CONNECTION_STOP(a->connection);
  }
  if (a->application_setup) SHIP_NODE_READER_ON_REMOTE_SKI_DISCONNECTED(a->owner->ship_node_reader, a->peer->ski);
  ShipConnectionDelete(a->connection);
  EebusQueueDelete(a->setup_reply);
  StringDelete(a->ship_id);
  EEBUS_FREE(a);
}

void ShipNodeConstruct(
    ShipNode* self,
    const char* ski,
    const char* role,
    const EebusDeviceInfo* device_info,
    const char* service_name,
    int port,
    const TlsCertificateObject* tsl_certificate,
    ShipNodeReaderObject* ship_node_reader,
    ServiceDetails* local_service_details
) {
  // Override "virtual function table"
  SHIP_NODE_INTERFACE(self) = &ship_node_methods;

  self->mdns = ShipMdnsCreate(ski, device_info, service_name, port, ShipNodeOnMdnsEntriesFoundCallback, self);

  static const size_t kQueueMaxMsg = 20;

  self->msg_queue = EebusQueueCreate(kQueueMaxMsg, sizeof(ShipNodeQueueMessage), ShipNodeQueueMsgDeallocator);

  self->mdns_entries          = VectorCreateWithDeallocator(MdnsEntryDeallocator);
  self->mutex                 = EebusMutexCreate();
  self->search_for_remote_ski = false;
  self->cancel                = false;
  self->next_attempt_id       = 0;
  self->connection_thread     = NULL;

  self->connections           = NodeConnectionContainerCreate();
  self->ship_node_reader      = ship_node_reader;
  self->tsl_certificate       = tsl_certificate;
  self->local_service_details = local_service_details;

  self->http_server = HttpServerCreate(port, tsl_certificate, ShipNodeOnWebsocketServerConnectionCallback, self);

  if (strcmp(role, "server") == 0) {
    self->role = kShipRoleServer;
  } else if (strcmp(role, "client") == 0) {
    self->role = kShipRoleClient;
  } else {
    self->role = kShipRoleAuto;
  }
}

ShipNodeObject* ShipNodeCreate(
    const char* ski,
    const char* role,
    const EebusDeviceInfo* device_info,
    const char* service_name,
    int port,
    const TlsCertificateObject* tls_certificate,
    ShipNodeReaderObject* ship_node_reader,
    ServiceDetails* local_service_details
) {
  ShipNode* const sn = (ShipNode*)EEBUS_MALLOC(sizeof(ShipNode));

  if (sn == NULL) return NULL;

  ShipNodeConstruct(
      sn,
      ski,
      role,
      device_info,
      service_name,
      port,
      tls_certificate,
      ship_node_reader,
      local_service_details
  );

  if (sn->mdns == NULL || sn->msg_queue == NULL || sn->mdns_entries == NULL || sn->mutex == NULL
      || sn->connections == NULL || sn->http_server == NULL) {
    if (sn->mdns != NULL) {
      SHIP_MDNS_DESTRUCT(sn->mdns);
      EEBUS_FREE(sn->mdns);
    }
    EebusQueueDelete(sn->msg_queue);
    if (sn->mdns_entries != NULL) {
      VectorDestruct(sn->mdns_entries);
      EEBUS_FREE(sn->mdns_entries);
    }
    EebusMutexDelete(sn->mutex);
    NodeConnectionContainerDelete(sn->connections);
    HttpServerDelete(sn->http_server);
    EEBUS_FREE(sn);
    return NULL;
  }
  return SHIP_NODE_OBJECT(sn);
}

static void ShipNodeRetryTimerCallback(void* ctx) {
  NodeConnection* peer = NODE_CONNECTION(ctx);
  ShipNode* node       = peer->owner;
  EEBUS_MUTEX_LOCK(node->mutex);
  if (!node->cancel && !peer->removing && peer->current == NULL && peer->replacement == NULL
      && peer->retry_arm_generation != 0 && peer->retry_arm_generation == peer->retry_generation)
    peer->retry_due = true;
  EEBUS_MUTEX_UNLOCK(node->mutex);
  WakeNode(node);
}

static void ShipNodeOnMdnsEntriesFoundCallback(Vector* entries, void* ctx) {
  ShipNode* node = ctx;
  if (entries == NULL) return;
  EEBUS_MUTEX_LOCK(node->mutex);
  if (!node->cancel) {
    VectorFreeElements(node->mdns_entries);
    VectorMove(node->mdns_entries, entries);
    node->search_for_remote_ski = true;
  }
  EEBUS_MUTEX_UNLOCK(node->mutex);
  VectorFreeElements(entries);
  VectorDestruct(entries);
  EEBUS_FREE(entries);
  WakeNode(node);
}

static bool ProcessPeer(ShipNode* node, NodeConnection* peer) {
  EEBUS_MUTEX_LOCK(node->mutex);
  ShipNodeAttempt* candidate = peer->replacement;
  if (candidate != NULL && !candidate->start_done) {
    EEBUS_MUTEX_UNLOCK(node->mutex);
    return false;
  }
  bool removing           = node->cancel || peer->removing;
  ShipNodeAttempt* retire = NULL;
  bool retry              = false;
  if (candidate != NULL && (removing || candidate->start_result != kEebusErrorOk || candidate->closed)) {
    candidate->lifecycle = kAttemptRetiring;
    retire               = candidate;
    retry                = peer->current == NULL && !removing;
  } else if (candidate != NULL) {
    retire = peer->current;
    if (retire != NULL) retire->lifecycle = kAttemptRetiring;
    peer->current            = candidate;
    peer->replacement        = retire;
    peer->connection         = candidate->connection;
    peer->is_attempt_running = true;
    peer->handshake_complete = false;
    candidate->lifecycle     = kAttemptPending;
  } else if (peer->current != NULL && (removing || peer->current->closed)) {
    retire                   = peer->current;
    retire->lifecycle        = kAttemptRetiring;
    peer->current            = NULL;
    peer->replacement        = retire;
    peer->connection         = NULL;
    peer->is_attempt_running = false;
    peer->handshake_complete = false;
    retry                    = !removing;
  }
  EEBUS_MUTEX_UNLOCK(node->mutex);
  if (retire != NULL) {
    RetireAttempt(retire);
    EEBUS_MUTEX_LOCK(node->mutex);
    if (peer->replacement == retire) peer->replacement = NULL;
    EEBUS_MUTEX_UNLOCK(node->mutex);
  }
  if (retry) {
    /* The old timer must finish before its arm identity can be reused. No node lock during a join. */
    NODE_CONNECTION_STOP_RETRY_TIMER(NODE_CONNECTION_OBJECT(peer));
    EEBUS_MUTEX_LOCK(node->mutex);
    bool idle      = !node->cancel && !peer->removing && peer->current == NULL && peer->replacement == NULL;
    uint32_t delay = 0;
    if (idle && node->next_attempt_id != UINT64_MAX) {
      delay                      = NODE_CONNECTION_ON_CONNECTION_CLOSED(NODE_CONNECTION_OBJECT(peer));
      peer->retry_generation     = ++node->next_attempt_id;
      peer->retry_arm_generation = peer->retry_generation;
      peer->retry_due            = delay == 0;
    }
    EEBUS_MUTEX_UNLOCK(node->mutex);
    if (idle && delay != 0) NODE_CONNECTION_SCHEDULE_RETRY(NODE_CONNECTION_OBJECT(peer), delay);
  }
  EEBUS_MUTEX_LOCK(node->mutex);
  ShipNodeAttempt* a = peer->current;
  bool valid         = a != NULL && !node->cancel && !peer->removing && !a->closed;
  bool setup         = valid && a->setup_pending;
  bool state         = valid && a->state_pending;
  char* id           = valid ? a->ship_id : NULL;
  if (valid) {
    a->setup_pending = false;
    a->state_pending = false;
    a->ship_id       = NULL;
  }
  SmeState next_state = valid ? a->state : kSmeStateError;
  EEBUS_MUTEX_UNLOCK(node->mutex);
  if (id != NULL) {
    SHIP_NODE_READER_ON_SHIP_ID_UPDATE(node->ship_node_reader, peer->ski, id);
    StringDelete(id);
  }
  if (setup) {
    DataReaderObject* reader = SHIP_NODE_READER_SETUP_REMOTE_DEVICE(node->ship_node_reader, peer->ski, a->writer);
    a->application_setup     = reader != NULL;
    EEBUS_QUEUE_SEND(a->setup_reply, &reader, 0);
  }
  EEBUS_MUTEX_LOCK(node->mutex);
  valid = a != NULL && peer->current == a && !node->cancel && !peer->removing && !a->closed;
  EEBUS_MUTEX_UNLOCK(node->mutex);
  if (state && valid) {
    SHIP_NODE_READER_ON_SHIP_STATE_UPDATE(node->ship_node_reader, peer->ski, next_state);
    if (next_state == kDataExchange && a->application_setup && !a->connected_published) {
      EEBUS_MUTEX_LOCK(node->mutex);
      valid = peer->current == a && !node->cancel && !peer->removing && !a->closed;
      if (!valid) {
        EEBUS_MUTEX_UNLOCK(node->mutex);
        return retire != NULL || setup || state;
      }
      a->lifecycle             = kAttemptEstablished;
      a->connected_published   = true;
      peer->handshake_complete = true;
      peer->collision_yields   = 0;
      peer->attempt_cnt        = 0;
      EEBUS_MUTEX_UNLOCK(node->mutex);
      SHIP_NODE_READER_ON_REMOTE_SKI_CONNECTED(node->ship_node_reader, peer->ski);
    }
  }
  return retire != NULL || setup || state;
}

static void DialPeer(ShipNode* node, NodeConnection* peer) {
  EEBUS_MUTEX_LOCK(node->mutex);
  MdnsEntry* found = NULL;
  if (!node->cancel && !peer->removing && peer->retry_due && peer->current == NULL && peer->replacement == NULL) {
    for (size_t i = 0; i < VectorGetSize(node->mdns_entries); ++i) {
      MdnsEntry* entry = VectorGetElement(node->mdns_entries, i);
      if (StringEqualsIgnoreCase(entry->ski, peer->ski)) {
        found = MdnsEntryCopy(entry);
        break;
      }
    }
  }
  ShipNodeAttempt* a = found != NULL ? ReserveAttempt(node, peer, kShipRoleClient) : NULL;
  EEBUS_MUTEX_UNLOCK(node->mutex);
  if (a != NULL) {
    NODE_CONNECTION_STOP_RETRY_TIMER(NODE_CONNECTION_OBJECT(peer));
    const char* uri = MdnsEntryToUri(found);
    WebsocketCreatorObject* creator
        = uri != NULL ? WebsocketClientCreatorCreate(uri, node->tsl_certificate, peer->ski) : NULL;
    SHIP_NODE_DEBUG_PRINTF("[SHIP] client connecting to %s, attempt=%" PRIu64 "\n", peer->ski, a->id);
    StartAttempt(a, creator);
    WebsocketCreatorDelete(creator);
    StringDelete((char*)uri);
  }
  MdnsEntryDelete(found);
}

static void* ShipNodeConnectionLoop(void* ctx) {
  ShipNode* node = ctx;
  for (;;) {
    bool progress = false;
    size_t i      = 0;
    for (;;) {
      EEBUS_MUTEX_LOCK(node->mutex);
      NodeConnection* peer = i < NODE_CONNECTION_CONTAINER_GET_SIZE(node->connections)
                                 ? NODE_CONNECTION(NODE_CONNECTION_CONTAINER_GET_WITH_INDEX(node->connections, i))
                                 : NULL;
      EEBUS_MUTEX_UNLOCK(node->mutex);
      if (peer == NULL) break;
      progress |= ProcessPeer(node, peer);
      EEBUS_MUTEX_LOCK(node->mutex);
      bool remove = peer->removing && peer->current == NULL && peer->replacement == NULL;
      EEBUS_MUTEX_UNLOCK(node->mutex);
      if (remove) {
        NODE_CONNECTION_STOP_RETRY_TIMER(NODE_CONNECTION_OBJECT(peer));
        EEBUS_MUTEX_LOCK(node->mutex);
        char* register_again = peer->register_pending ? StringCopy(peer->ski) : NULL;
        NODE_CONNECTION_CONTAINER_REMOVE_WITH_SKI(node->connections, peer->ski);
        EEBUS_MUTEX_UNLOCK(node->mutex);
        if (register_again != NULL) {
          RegisterRemoteSki(SHIP_NODE_OBJECT(node), register_again, true);
          StringDelete(register_again);
        }
        progress = true;
        continue;
      }
      if (ShipNodeIsClientSupported(node)) DialPeer(node, peer);
      ++i;
    }
    EEBUS_MUTEX_LOCK(node->mutex);
    bool cancel = node->cancel;
    bool active = false;
    for (size_t j = 0; j < NODE_CONNECTION_CONTAINER_GET_SIZE(node->connections); ++j) {
      NodeConnection* peer = NODE_CONNECTION(NODE_CONNECTION_CONTAINER_GET_WITH_INDEX(node->connections, j));
      active |= peer->current != NULL || peer->replacement != NULL;
    }
    bool publish     = node->search_for_remote_ski && !cancel;
    Vector* snapshot = publish ? VectorCreateWithDeallocator(MdnsEntryDeallocator) : NULL;
    if (snapshot != NULL) {
      bool complete = true;
      for (size_t j = 0; j < VectorGetSize(node->mdns_entries); ++j) {
        MdnsEntry* copy = MdnsEntryCopy(VectorGetElement(node->mdns_entries, j));
        size_t size     = VectorGetSize(snapshot);
        if (copy != NULL) VectorPushBack(snapshot, copy);
        if (copy == NULL || VectorGetSize(snapshot) == size) {
          MdnsEntryDelete(copy);
          complete = false;
          break;
        }
      }
      if (complete)
        node->search_for_remote_ski = false;
      else {
        VectorFreeElements(snapshot);
        VectorDestruct(snapshot);
        EEBUS_FREE(snapshot);
        snapshot = NULL;
      }
    }
    EEBUS_MUTEX_UNLOCK(node->mutex);
    if (snapshot != NULL) {
      SHIP_NODE_READER_ON_REMOTE_SERVICES_UPDATE(node->ship_node_reader, snapshot);
      VectorFreeElements(snapshot);
      VectorDestruct(snapshot);
      EEBUS_FREE(snapshot);
    }
    if (cancel && !active) break;
    if (progress) continue;
    ShipNodeQueueMessage wake;
    EEBUS_QUEUE_RECEIVE(node->msg_queue, &wake, kTimeoutInfinite);
  }
  return NULL;
}

static int CompareSkis(const char* a, const char* b) {
  if (a == NULL || b == NULL || strlen(a) != 40 || strlen(b) != 40) return 2;
  int result = 0;
  for (size_t i = 0; i < 40; ++i) {
    int x = a[i] >= 'A' && a[i] <= 'F' ? a[i] + 'a' - 'A' : a[i];
    int y = b[i] >= 'A' && b[i] <= 'F' ? b[i] + 'a' - 'A' : b[i];
    if (!((x >= '0' && x <= '9') || (x >= 'a' && x <= 'f')) || !((y >= '0' && y <= '9') || (y >= 'a' && y <= 'f')))
      return 2;
    if (result == 0 && x != y) result = x < y ? -1 : 1;
  }
  return result;
}

static int ShipNodeOnWebsocketServerConnectionCallback(const char* ski, WebsocketCreatorObject* creator, void* ctx) {
  ShipNode* node = ctx;
  if (!ShipNodeIsServerSupported(node)) return -1;
  EEBUS_MUTEX_LOCK(node->mutex);
  bool pairing_candidate = !node->cancel && NODE_CONNECTION_CONTAINER_GET_SIZE(node->connections) == 0;
  EEBUS_MUTEX_UNLOCK(node->mutex);
  bool allow_pairing = pairing_candidate && INFO_PROVIDER_IS_WAITING_FOR_TRUST_ALLOWED(node, ski);
  EEBUS_MUTEX_LOCK(node->mutex);
  NodeConnection* peer = NODE_CONNECTION(NODE_CONNECTION_CONTAINER_FIND_WITH_SKI(node->connections, ski));
  if (peer == NULL && NODE_CONNECTION_CONTAINER_GET_SIZE(node->connections) == 0 && !node->cancel && allow_pairing) {
    peer = NODE_CONNECTION(
        NODE_CONNECTION_CONTAINER_GET_OR_CREATE(node->connections, ski, node, ShipNodeRetryTimerCallback)
    );
  }
  const int cmp = CompareSkis(ServiceDetailsGetSki(node->local_service_details), ski);
  if (peer == NULL || cmp == 0 || cmp == 2 || node->cancel || peer->removing || peer->replacement != NULL) {
    EEBUS_MUTEX_UNLOCK(node->mutex);
    return -1;
  }
  /* A bounded refusal breaks the simultaneous-open symmetry, without choosing a permanent dialer. */
  if (peer->current != NULL && peer->current->role == kShipRoleClient && peer->current->lifecycle == kAttemptPending
      && cmp < 0 && peer->collision_yields++ == 0) {
    SHIP_NODE_DEBUG_PRINTF("[SHIP-SIMOPEN] yield to remote\n");
    EEBUS_MUTEX_UNLOCK(node->mutex);
    return -1;
  }
  SHIP_NODE_DEBUG_PRINTF(
      "%s\n",
      peer->current != NULL ? "[SHIP-SIMOPEN] take server role" : "server accept (no simultaneous open)"
  );
  ShipNodeAttempt* candidate = ReserveAttempt(node, peer, kShipRoleServer);
  EEBUS_MUTEX_UNLOCK(node->mutex);
  if (candidate == NULL) return -1;
  /* Bind the borrowed creator now. Retirement and application publication belong to the node worker. */
  EebusError err = StartAttempt(candidate, creator);
  return err == kEebusErrorOk ? 0 : -1;
}

static bool ShipNodeIsClientSupported(ShipNode* node) {
  return node->role != kShipRoleServer;
}
static bool ShipNodeIsServerSupported(ShipNode* node) {
  return node->role != kShipRoleClient;
}

static void Start(ShipNodeObject* self) {
  ShipNode* node = SHIP_NODE(self);
  if (node->connection_thread != NULL) return;
  node->cancel = false;
  for (size_t i = 0; i < NODE_CONNECTION_CONTAINER_GET_SIZE(node->connections); ++i)
    NODE_CONNECTION(NODE_CONNECTION_CONTAINER_GET_WITH_INDEX(node->connections, i))->retry_due = true;
  node->connection_thread = EebusThreadCreate(ShipNodeConnectionLoop, node, 4 * 1024);
  if (node->connection_thread == NULL) return;
  if ((ShipNodeIsServerSupported(node) && HTTP_SERVER_START(node->http_server) != kEebusErrorOk)
      || SHIP_MDNS_START(node->mdns) != kEebusErrorOk) {
    SHIP_NODE_DEBUG_PRINTF("ShipNode::Start(): backend start failed\n");
    Stop(self);
  }
}
static void Stop(ShipNodeObject* self) {
  ShipNode* node = SHIP_NODE(self);
  SHIP_NODE_DEBUG_PRINTF("ShipNode::Stop(): begin\n");
  EEBUS_MUTEX_LOCK(node->mutex);
  node->cancel = true;
  EEBUS_MUTEX_UNLOCK(node->mutex);
  WakeNode(node);
  if (node->connection_thread != NULL) {
    EEBUS_THREAD_JOIN(node->connection_thread);
    EebusThreadDelete(node->connection_thread);
    node->connection_thread = NULL;
  }
  for (size_t i = 0; i < NODE_CONNECTION_CONTAINER_GET_SIZE(node->connections); ++i)
    NODE_CONNECTION_STOP_RETRY_TIMER(NODE_CONNECTION_CONTAINER_GET_WITH_INDEX(node->connections, i));
  SHIP_MDNS_STOP(node->mdns);
  if (ShipNodeIsServerSupported(node)) HTTP_SERVER_STOP(node->http_server);
  SHIP_NODE_DEBUG_PRINTF("ShipNode::Stop(): end\n");
}
static void Destruct(InfoProviderObject* self) {
  ShipNode* node = SHIP_NODE(self);
  Stop(SHIP_NODE_OBJECT(node));
  SHIP_NODE_DEBUG_PRINTF("ShipNode::Destruct(): begin\n");
  SHIP_MDNS_DESTRUCT(node->mdns);
  EEBUS_FREE(node->mdns);
  VectorFreeElements(node->mdns_entries);
  VectorDestruct(node->mdns_entries);
  EEBUS_FREE(node->mdns_entries);
  HttpServerDelete(node->http_server);
  NodeConnectionContainerDelete(node->connections);
  EebusQueueDelete(node->msg_queue);
  EebusMutexDelete(node->mutex);
  SHIP_NODE_DEBUG_PRINTF("ShipNode::Destruct(): end\n");
}
static void RegisterRemoteSki(ShipNodeObject* self, const char* ski, bool trusted) {
  UNUSED(trusted);
  ShipNode* node = SHIP_NODE(self);
  int comparison = CompareSkis(ServiceDetailsGetSki(node->local_service_details), ski);
  if (comparison == 0 || comparison == 2) return;
  EEBUS_MUTEX_LOCK(node->mutex);
  NodeConnection* peer
      = node->cancel
            ? NULL
            : NODE_CONNECTION(
                  NODE_CONNECTION_CONTAINER_GET_OR_CREATE(node->connections, ski, node, ShipNodeRetryTimerCallback)
              );
  if (peer != NULL) {
    if (peer->removing)
      peer->register_pending = true;
    else
      peer->retry_due = true;
  }
  EEBUS_MUTEX_UNLOCK(node->mutex);
  WakeNode(node);
}
static void UnregisterRemoteSki(ShipNodeObject* self, const char* ski) {
  ShipNode* node = SHIP_NODE(self);
  EEBUS_MUTEX_LOCK(node->mutex);
  NodeConnection* peer = NODE_CONNECTION(NODE_CONNECTION_CONTAINER_FIND_WITH_SKI(node->connections, ski));
  if (peer != NULL) {
    peer->removing         = true;
    peer->register_pending = false;
    peer->retry_generation = 0;
  }
  EEBUS_MUTEX_UNLOCK(node->mutex);
  WakeNode(node);
}
static void CancelPairingWithSki(ShipNodeObject* self, const char* ski) {
  UnregisterRemoteSki(self, ski);
}
