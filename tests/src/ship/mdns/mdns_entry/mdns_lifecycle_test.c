/* Copyright 2026 NIBE AB. Licensed under the Apache License, Version 2.0. */
#include <assert.h>
#include "src/ship/mdns/ship_mdns_bonjour.c"
static const char* old_ski = "1111111111111111111111111111111111111111";
static const char* new_ski = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
static unsigned notifications;
static void Snapshot(Vector* entries, void* ctx) {
  UNUSED(ctx);
  ++notifications;
  VectorFreeElements(entries);
  VectorDestruct(entries);
  EEBUS_FREE(entries);
}
static void Text(TXTRecordRef* txt, const char* ski) {
  TXTRecordCreate(txt, 0, NULL);
  const char* keys[]   = {"txtvers", "id", "path", "ski", "register"};
  const char* values[] = {"1", "test-id", "/ship/", ski, "false"};
  for (size_t i = 0; i < 5; ++i)
    assert(TXTRecordSetValue(txt, keys[i], (uint8_t)strlen(values[i]), values[i]) == kDNSServiceErr_NoError);
}
static void Resolve(ActiveResolveEntry* service, const char* ski, const char* hostname, uint16_t port) {
  TXTRecordRef txt;
  Text(&txt, ski);
  service->resolving_revision = service->revision;
  MdnsResolveServiceCallback(
      NULL,
      kDNSServiceFlagsMoreComing,
      service->entry->iface,
      0,
      "test._ship._tcp.local.",
      hostname,
      htons(port),
      TXTRecordGetLength(&txt),
      TXTRecordGetBytesPtr(&txt),
      service
  );
  TXTRecordDeallocate(&txt);
}
static void Notify(Mdns* mdns) {
  EEBUS_MUTEX_LOCK(mdns->provider_mutex);
  MdnsNotifyFoundEntries(mdns);
  EEBUS_MUTEX_UNLOCK(mdns->provider_mutex);
}
int main(void) {
  Mdns mdns            = {.ski = "3333333333333333333333333333333333333333", .on_entries_found_cb = Snapshot};
  mdns.provider_mutex  = EebusMutexCreateRecursive();
  mdns.found_entries   = VectorCreateWithDeallocator(MdnsEntryDeallocator);
  mdns.active_resolves = VectorCreateWithDeallocator(MdnsActiveResolveEntryDeallocator);
  MdnsBrowseServicesCallback(NULL, kDNSServiceFlagsAdd, 2, 0, "peer", "_ship._tcp", "local.", &mdns);
  MdnsBrowseServicesCallback(NULL, kDNSServiceFlagsAdd, 3, 0, "peer", "_ship._tcp", "local.", &mdns);
  assert(VectorGetSize(mdns.active_resolves) == 2);
  ActiveResolveEntry* service     = VectorGetElement(mdns.active_resolves, 0);
  ActiveResolveEntry* other_iface = VectorGetElement(mdns.active_resolves, 1);
  Resolve(service, old_ski, "old.local.", 4710);
  Resolve(other_iface, old_ski, "old.local.", 4710);
  assert(VectorGetSize(mdns.found_entries) == 2); /* same SKI on two interfaces */
  Notify(&mdns);
  assert(notifications == 1);
  Notify(&mdns);
  assert(notifications == 1); /* no unchanged snapshot recovery */
  TXTRecordRef txt;
  Text(&txt, new_ski);
  MdnsRecordCallback(
      NULL,
      kDNSServiceFlagsAdd | kDNSServiceFlagsMoreComing,
      2,
      0,
      "peer",
      kDNSServiceType_TXT,
      kDNSServiceClass_IN,
      TXTRecordGetLength(&txt),
      TXTRecordGetBytesPtr(&txt),
      120,
      service
  );
  assert(VectorGetSize(mdns.found_entries) == 2);
  bool fresh = false;
  for (size_t i = 0; i < VectorGetSize(mdns.found_entries); ++i) {
    MdnsEntry* entry = VectorGetElement(mdns.found_entries, i);
    fresh |= entry->iface == 2 && StringEqualsIgnoreCase(entry->ski, new_ski);
  }
  assert(fresh);
  Notify(&mdns);
  assert(notifications == 1);
  MdnsRecordCallback(
      NULL,
      kDNSServiceFlagsAdd,
      2,
      0,
      "peer",
      kDNSServiceType_TXT,
      kDNSServiceClass_IN,
      TXTRecordGetLength(&txt),
      TXTRecordGetBytesPtr(&txt),
      120,
      service
  );
  Notify(&mdns);
  assert(notifications == 2);
  TXTRecordDeallocate(&txt);
  uint8_t srv[] = {0, 0, 0, 0, 0x12, 0x34, 0};
  MdnsRecordCallback(
      NULL,
      kDNSServiceFlagsAdd,
      2,
      0,
      "peer",
      kDNSServiceType_SRV,
      kDNSServiceClass_IN,
      sizeof(srv),
      srv,
      120,
      service
  );
  assert(VectorGetSize(mdns.found_entries) == 1 && service->dirty);
  Resolve(service, new_ski, "new.local.", 4910);
  MdnsEntry* updated = VectorGetElement(mdns.found_entries, 1);
  assert(strcmp(updated->host, "new.local.") == 0 && updated->port == 4910);
  /* Removing interface 2 preserves interface 3 and invalidates pending callbacks. */
  MdnsBrowseServicesCallback(NULL, 0, 2, 0, "peer", "_ship._tcp", "local.", &mdns);
  assert(service->removed && VectorGetSize(mdns.found_entries) == 1);
  Resolve(service, old_ski, "stale.local.", 1234);
  assert(VectorGetSize(mdns.found_entries) == 1);
  MdnsBrowseServicesCallback(NULL, kDNSServiceFlagsAdd, 2, 0, "peer", "_ship._tcp", "local.", &mdns);
  assert(VectorGetSize(mdns.active_resolves) == 3);
  ActiveResolveEntry* readded = VectorGetElement(mdns.active_resolves, 2);
  Resolve(readded, new_ski, "readded.local.", 4990);
  assert(VectorGetSize(mdns.found_entries) == 2);
  const uint8_t invalid[] = {3, 'b', 'a', 'd'};
  MdnsRecordCallback(
      NULL,
      kDNSServiceFlagsAdd,
      2,
      0,
      "peer",
      kDNSServiceType_TXT,
      kDNSServiceClass_IN,
      sizeof(invalid),
      invalid,
      120,
      readded
  );
  assert(VectorGetSize(mdns.found_entries) == 1);
  MdnsBrowserReset(&mdns);
  assert(VectorGetSize(mdns.active_resolves) == 0 && VectorGetSize(mdns.found_entries) == 0);
  VectorDestruct(mdns.active_resolves);
  EEBUS_FREE(mdns.active_resolves);
  VectorDestruct(mdns.found_entries);
  EEBUS_FREE(mdns.found_entries);
  EebusMutexDelete(mdns.provider_mutex);
  puts(
      "PASS: service/interface identity, retained TXT change, SRV endpoint invalidation, unchanged snapshots, "
      "removal/re-addition, stale completion, invalid TXT, cleanup"
  );
  return 0;
}
