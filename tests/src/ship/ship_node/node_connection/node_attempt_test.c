/* Copyright 2026 NIBE AB. Licensed under the Apache License, Version 2.0. */
#include <assert.h>
#include <stdio.h>
#include "src/ship/ship_connection/ship_connection.h"
static ShipConnectionObject* TestConnectionCreate(InfoProviderObject*, ShipRole, const char*, const char*, const char*);
#define ShipConnectionCreate TestConnectionCreate
#ifndef NODE_ATTEMPT_SOURCE
#define NODE_ATTEMPT_SOURCE "src/ship/ship_node/ship_node.c"
#endif
#include NODE_ATTEMPT_SOURCE
#undef ShipConnectionCreate

typedef struct {
  ShipConnectionObject object;
  InfoProviderObject* provider;
} FakeConnection;
static EebusError start_error;
static bool allocation_failure;
static unsigned connected, disconnected, stopped;
static bool late_completion;
static EebusThreadObject* setup_thread;
static DataReaderObject* setup_result;
static void* SetupThread(void* ctx) {
  ShipNodeAttempt* a = ctx;
  setup_result       = AdapterSetup(&a->provider, a->peer->ski, DATA_WRITER_OBJECT(a->connection));
  return NULL;
}
static void* ExitedConsumer(void* ctx) {
  UNUSED(ctx);
  return NULL;
}
static void StopMdns(ShipMdnsObject* self) {
  UNUSED(self);
}
static EebusError FakeStart(ShipConnectionObject* self, WebsocketCreatorObject* creator) {
  UNUSED(self);
  UNUSED(creator);
  return start_error;
}
static void FakeStop(ShipConnectionObject* self) {
  FakeConnection* fake = (FakeConnection*)self;
  ++stopped;
  if (setup_thread != NULL) {
    EEBUS_THREAD_JOIN(setup_thread);
    EebusThreadDelete(setup_thread);
    setup_thread = NULL;
  }
  if (late_completion) {
    INFO_PROVIDER_HANDLE_SHIP_STATE_UPDATE(fake->provider, "ignored", kDataExchange, NULL);
    INFO_PROVIDER_HANDLE_CONNECTION_CLOSED(fake->provider, self, false);
  }
}
static void FakeDestruct(DataWriterObject* self) {
  UNUSED(self);
}
static const ShipConnectionInterface fake_methods = {
    .data_writer_interface = {.destruct = FakeDestruct},
    .start                 = FakeStart,
    .stop                  = FakeStop,
};
static ShipConnectionObject* TestConnectionCreate(
    InfoProviderObject* provider,
    ShipRole role,
    const char* id,
    const char* ski,
    const char* remote_id
) {
  UNUSED(role);
  UNUSED(id);
  UNUSED(ski);
  UNUSED(remote_id);
  if (allocation_failure) return NULL;
  FakeConnection* fake    = EEBUS_MALLOC(sizeof(*fake));
  fake->object.interface_ = &fake_methods;
  fake->provider          = provider;
  return &fake->object;
}
static void State(ShipNodeReaderObject* reader, const char* ski, SmeState state) {
  UNUSED(reader);
  UNUSED(ski);
  UNUSED(state);
}
static void Connected(ShipNodeReaderObject* reader, const char* ski) {
  UNUSED(reader);
  UNUSED(ski);
  ++connected;
}
static void Disconnected(ShipNodeReaderObject* reader, const char* ski) {
  UNUSED(reader);
  UNUSED(ski);
  ++disconnected;
}
static const ShipNodeReaderInterface reader_methods = {
    .on_ship_state_update       = State,
    .on_remote_ski_connected    = Connected,
    .on_remote_ski_disconnected = Disconnected,
};
static const char* skis[] = {"1111111111111111111111111111111111111111", "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"};

int main(void) {
  WebsocketCreatorObject creator = {0};
  for (int ordering = 0; ordering != 2; ++ordering) {
    ShipNodeReaderObject reader = {&reader_methods};
    ShipNode node               = {.role = kShipRoleAuto, .ship_node_reader = &reader};
    node.mutex                  = EebusMutexCreate();
    node.msg_queue              = EebusQueueCreate(1, sizeof(ShipNodeQueueMessage), NULL);
    node.connections            = NodeConnectionContainerCreate();
    node.local_service_details  = ServiceDetailsCreate(skis[ordering], "local", "test", false);
    NodeConnection* peer        = NODE_CONNECTION(
        NODE_CONNECTION_CONTAINER_GET_OR_CREATE(node.connections, skis[1 - ordering], &node, ShipNodeRetryTimerCallback)
    );
    EEBUS_MUTEX_LOCK(node.mutex);
    ShipNodeAttempt* outgoing = ReserveAttempt(&node, peer, kShipRoleClient);
    EEBUS_MUTEX_UNLOCK(node.mutex);
    assert(outgoing != NULL);
    assert(StartAttempt(outgoing, &creator) == kEebusErrorOk);
    ProcessPeer(&node, peer);
    assert(peer->current == outgoing);
    peer->retry_due            = false;
    peer->retry_arm_generation = outgoing->id;
    peer->retry_generation     = outgoing->id + 1;
    ShipNodeRetryTimerCallback(peer);
    assert(!peer->retry_due);
    unsigned before = stopped;
    if (ordering == 0) assert(ShipNodeOnWebsocketServerConnectionCallback(peer->ski, &creator, &node) == -1);
    start_error = kEebusErrorThread;
    assert(ShipNodeOnWebsocketServerConnectionCallback(peer->ski, &creator, &node) == -1);
    ProcessPeer(&node, peer);
    assert(peer->current == outgoing && stopped == before + 1);
    start_error        = kEebusErrorOk;
    allocation_failure = true;
    assert(ShipNodeOnWebsocketServerConnectionCallback(peer->ski, &creator, &node) == -1);
    ProcessPeer(&node, peer);
    assert(peer->current == outgoing);
    allocation_failure = false;
    assert(ShipNodeOnWebsocketServerConnectionCallback(peer->ski, &creator, &node) == 0);
    ShipNodeAttempt* incoming = peer->replacement;
    assert(incoming->id != outgoing->id);
    assert(ShipNodeOnWebsocketServerConnectionCallback(peer->ski, &creator, &node) == -1);
    late_completion           = true;
    unsigned connected_before = connected;
    ProcessPeer(&node, peer);
    assert(peer->current == incoming && !peer->handshake_complete && connected == connected_before);
    assert(peer->replacement == NULL);
    incoming->application_setup = true;
    AdapterState(&incoming->provider, peer->ski, kDataExchange, NULL);
    ProcessPeer(&node, peer);
    assert(peer->handshake_complete && connected == connected_before + 1);
    AdapterState(&incoming->provider, peer->ski, kDataExchange, NULL);
    ProcessPeer(&node, peer);
    assert(connected == connected_before + 1);
    peer->removing = true;
    AdapterState(&incoming->provider, peer->ski, kDataExchange, NULL);
    ProcessPeer(&node, peer);
    assert(peer->current == NULL && peer->replacement == NULL && connected == connected_before + 1);
    peer->retry_due            = false;
    peer->retry_generation     = 8;
    peer->retry_arm_generation = 7;
    ShipNodeRetryTimerCallback(peer);
    assert(!peer->retry_due);
    peer->removing         = false;
    peer->collision_yields = 9;
    EEBUS_MUTEX_LOCK(node.mutex);
    ShipNodeAttempt* retried = ReserveAttempt(&node, peer, kShipRoleClient);
    EEBUS_MUTEX_UNLOCK(node.mutex);
    assert(retried != NULL && peer->collision_yields == 0);
    assert(StartAttempt(retried, &creator) == kEebusErrorOk);
    ProcessPeer(&node, peer);
    if (ordering == 0) assert(ShipNodeOnWebsocketServerConnectionCallback(peer->ski, &creator, &node) == -1);
    peer->removing = true;
    ProcessPeer(&node, peer);
    assert(peer->current == NULL && peer->replacement == NULL);
    peer->removing = false;
    assert(ShipNodeOnWebsocketServerConnectionCallback(peer->ski, &creator, &node) == 0);
    ProcessPeer(&node, peer);
    assert(peer->current != NULL);
    ShipNodeAttempt* pending_setup = peer->current;
    setup_result                   = (DataReaderObject*)1;
    setup_thread                   = EebusThreadCreate(SetupThread, pending_setup, 4096);
    assert(setup_thread != NULL);
    for (;;) {
      EEBUS_MUTEX_LOCK(node.mutex);
      bool pending = pending_setup->setup_pending;
      EEBUS_MUTEX_UNLOCK(node.mutex);
      if (pending) break;
    }
    peer->removing = true;
    ProcessPeer(&node, peer);
    assert(setup_result == NULL && setup_thread == NULL);
    assert(ShipNodeOnWebsocketServerConnectionCallback(skis[ordering], &creator, &node) == -1);
    NodeConnectionContainerDelete(node.connections);
    ServiceDetailsDelete(node.local_service_details);
    EebusQueueDelete(node.msg_queue);
    EebusMutexDelete(node.mutex);
  }
  assert(connected == 2 && disconnected == 2);
  ShipMdnsInterface mdns_methods = {.stop = StopMdns};
  ShipMdnsObject mdns            = {.interface_ = &mdns_methods};
  ShipNode full                  = {.role = kShipRoleClient, .mdns = &mdns, .cancel = true};
  full.mutex                     = EebusMutexCreate();
  full.connections               = NodeConnectionContainerCreate();
  full.msg_queue                 = EebusQueueCreate(20, sizeof(ShipNodeQueueMessage), NULL);
  ShipNodeQueueMessage message   = {0};
  for (unsigned i = 0; i < 20; ++i) assert(EEBUS_QUEUE_SEND(full.msg_queue, &message, 0) == kEebusErrorOk);
  full.connection_thread = EebusThreadCreate(ExitedConsumer, NULL, 4096);
  assert(full.connection_thread != NULL);
  Stop(SHIP_NODE_OBJECT(&full));
  EebusQueueDelete(full.msg_queue);
  NodeConnectionContainerDelete(full.connections);
  EebusMutexDelete(full.mutex);
  printf("attempt_bytes=%zu peer_bytes=%zu\n", sizeof(ShipNodeAttempt), sizeof(NodeConnection));
  puts(
      "PASS: both orderings, bounded admission, start/allocation rollback, stale completion/close, cancellation, "
      "reconnect, single publication"
  );
  return 0;
}
