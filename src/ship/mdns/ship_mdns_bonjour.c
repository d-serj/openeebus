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
/**
 * @file
 * @brief Mdns implementation
 */

// Useful information about dns-sd.
//
// See example code at:
// https://github.com/xbmc/mDNSResponder/blob/master/Clients/dns-sd.c
//
// Running "dns-sd -B _services._dns-sd._udp" will return a list of all available
// service types that are currently being advertised (the list is per interface,
// so there will be some redundancy). If this is done on a Mac with no active
// network connection, the list will of course only contain services running on that machine.
//
// Using that list, information about the individual services types can be requested
// by running e.g. "dns-sd -B _home-sharing._tcp" (which lists iTunes Home Sharing instances),
// and then, given an instance name, "dns-sd -L "Wes Campaigne’s Library" _home-sharing._tcp"
// will provide an information for a particular instance.
//
// Example of dns-sd output:
//-------------------------------------------------------------------------------------------//
// % dns-sd -B _ship._tcp
// Browsing for _ship._tcp
// DATE: ---Fri 19 Apr 2024---
// 18:41:25.166  ...STARTING...
// Timestamp     A/R    Flags  if Domain               Service Type         Instance Name
// 18:41:25.167  Add        2  13 local.               _ship._tcp.          Demo-EVSE-234567890
//
// % dns-sd -L "Demo-EVSE-234567890" _ship._tcp
// Lookup Demo-EVSE-234567890._ship._tcp.local
// DATE: ---Fri 19 Apr 2024---
// 18:41:44.184  ...STARTING...
// 18:41:44.185  Demo-EVSE-234567890._ship._tcp.local. can be reached at
// DESKTOP-IAKQS71.local.:4769 (interface 13)
// txtvers=1 path=/ship/ id=Demo-EVSE-234567890 ski=41c98b1bbe5fc7657ce311981951f12d304ab419
// brand=Demo model=EVSE type=ChargingStation register=false
//-------------------------------------------------------------------------------------------//
//
// From example above it is obvious that using dns-sd library the lookup can be done in
// two stages:
// 1. Call DNSServiceBrowse() with type "_ship._tcp"
// 2. Call DNSServiceResolve() using name obtained in [1] and same type "_ship._tcp"
// Each step shall be followed by event handling with timeout (see MdnsProcessResults() function
// from https://github.com/xbmc/mDNSResponder/blob/master/Clients/dns-sd.c)
//
// Note that when using libwebsockets, "DESKTOP-IAKQS71.local.:4769" shall be used without
// a dot after "local"!
// An example of working uri is: "wss://DESKTOP-IAKQS71.local:4769"
//
// To announce the mDNS entry use:
// dns-sd -R NIBE-06920619238006 _ship._tcp. local 7711 path=/ship/
//     ski=41c98b1bbe5fc7657ce311981951f12d304ab419
//     txtvers=1 id=NIBE-06920619238006 register=false model=nibe-n type=ControlBox brand=NIBE

#include <errno.h>
#include <stdatomic.h>
#ifdef _WIN32
// clang-format off
#include <winsock2.h>
#include <ws2tcpip.h>
#include <Iphlpapi.h>
#include <process.h>
// clang-format on
#else
#include <arpa/inet.h>
#include <sys/select.h>
#include <unistd.h>
#endif
#include <dns_sd.h>
#include <string.h>
#include <time.h>

#include "src/common/array_util.h"
#include "src/common/debug.h"
#include "src/common/eebus_arguments.h"
#include "src/common/eebus_device_info.h"
#include "src/common/eebus_mutex/eebus_mutex.h"
#include "src/common/eebus_thread/eebus_thread.h"
#include "src/common/string_util.h"
#include "src/common/vector.h"
#include "src/ship/api/mdns_entry.h"
#include "src/ship/api/ship_mdns_interface.h"
#include "src/ship/mdns/mdns_debug.h"

/** mDNS debug printf(), enabled whith MDNS_DEBUG = 1 */
#if MDNS_DEBUG
#define MDNS_DEBUG_PRINTF(fmt, ...) DebugPrintf(fmt, ##__VA_ARGS__)
#else
#define MDNS_DEBUG_PRINTF(fmt, ...)
#endif  // MDNS_DEBUG

/** Delay before retrying to re-establish a dead mDNS session */
static const uint32_t kMdnsRecoveryDelaySeconds = 5;

static const char* kShipServiceType   = "_ship._tcp";
static const char* kShipServicePath   = "/ship/";
static const char* kShipServiceTxtVer = "1";

static const struct timeval select_timeout = {
    .tv_sec  = 0,
    .tv_usec = 100000,
};

typedef struct Mdns Mdns;

typedef struct ActiveResolveEntry ActiveResolveEntry;
/** Tracks a single pending DNSServiceResolve call */
struct ActiveResolveEntry {
  /** The DNSServiceRef for this resolve */
  DNSServiceRef service_ref;
  /** The MdnsEntry being resolved */
  MdnsEntry* entry;
  /** The owning Mdns instance */
  Mdns* owner;
  /** Whether the resolve is done */
  bool done;
  DNSServiceRef txt_ref;
  DNSServiceRef srv_ref;
  bool removed;
  bool dirty;
  bool endpoint_valid;
  bool watches_failed;
  bool txt_batch;
  bool srv_batch;
  bool resolve_failed;
  uint64_t revision;
  uint64_t resolving_revision;
  uint32_t retry_at;
  uint32_t resolve_deadline;
  uint8_t* txt_data;
  size_t txt_size;
  uint8_t* srv_data;
  size_t srv_size;
};

struct Mdns {
  /** Implements the Mdns Interface */
  ShipMdnsObject obj;
  const char* ski;
  EebusDeviceInfo* device_info;
  const char* service_name;
  int port;
  atomic_bool autoaccept;

  OnMdnsEntriesFoundCallback on_entries_found_cb;
  void* context;

  EebusThreadObject* thread;
  DNSServiceRef dns_service_browser_ref;
  DNSServiceRef dns_service_register_ref;
  Vector* active_resolves;
  Vector* found_entries;

  atomic_bool cancel;
  EebusMutexObject* provider_mutex;
  bool snapshot_dirty;
  bool browse_batch;
  size_t refresh_cursor;

  /** Set when the mDNS sessions must be recovered */
  bool needs_recovery;
  /** Earliest monotonic time (seconds) at which a session recovery attempt may run */
  uint32_t next_recovery_time_seconds;
};

#define MDNS(obj) ((Mdns*)(obj))

#define TRY_SET_TXT_RECORD_VALUE(record, key, value)                                                 \
  {                                                                                                  \
    const DNSServiceErrorType error = TXTRecordSetValue(record, key, (uint8_t)strlen(value), value); \
    if (error != kDNSServiceErr_NoError) {                                                           \
      MDNS_DEBUG_PRINTF("TXTRecordSetValue(%s) returned error %d\n", key, error);                    \
      return error;                                                                                  \
    }                                                                                                \
  }

static void Destruct(ShipMdnsObject* self);
static EebusError Start(ShipMdnsObject* self);
static void Stop(ShipMdnsObject* self);
static EebusError RegisterService(ShipMdnsObject* self);
static EebusError RegisterServiceInternal(ShipMdnsObject* self);
static void DeregisterService(ShipMdnsObject* self);
static void SetAutoaccept(ShipMdnsObject* self, bool autoaccept);

static const ShipMdnsInterface mdns_methods = {
    .destruct           = Destruct,
    .start              = Start,
    .stop               = Stop,
    .register_service   = RegisterService,
    .deregister_service = DeregisterService,
    .set_autoaccept     = SetAutoaccept,
};

static void MdnsConstruct(
    Mdns* self,
    const char* ski,
    const EebusDeviceInfo* device_info,
    const char* service_name,
    int port,
    OnMdnsEntriesFoundCallback cb,
    void* ctx
);
static ActiveResolveEntry* MdnsActiveResolveEntryCreate(Mdns* owner, MdnsEntry* entry);
static void MdnsActiveResolveEntryDestroy(ActiveResolveEntry* resolve);
static void MdnsActiveResolveEntryDeallocator(void* resolve);

static bool MdnsProcessActiveResolveEntry(ActiveResolveEntry* resolve, const fd_set* readfds);
static void MdnsProcessActiveResolves(Mdns* self, const fd_set* readfds);

static inline uint16_t OpaquePortToUint16(uint16_t opaque_port);
static void MdnsResolveServiceCallback(
    DNSServiceRef service_ref,
    const DNSServiceFlags flags,
    uint32_t iface,
    DNSServiceErrorType err,
    const char* name,
    const char* host,
    uint16_t opaque_port,
    uint16_t txt_record_size,
    const unsigned char* txt_record,
    void* ctx
);
static void MdnsBrowseServicesCallback(
    DNSServiceRef service_ref,
    DNSServiceFlags flags,
    uint32_t iface,
    DNSServiceErrorType err,
    const char* name,
    const char* type,
    const char* domain,
    void* ctx
);
static void MdnsBrowseServices(Mdns* self);
static int MdnsPrepareSockFd(DNSServiceRef service_ref, fd_set* readfds, int* max_fd);
static void MdnsDispatchReadyFds(Mdns* mdns, const fd_set* readfds, int browse_fd, int register_fd);
static void MdnsNotifyFoundEntries(Mdns* mdns);
static void* MdnsBrowserLoop(void* parameters);
static void MdnsRegisterServiceCallback(
    DNSServiceRef ref,
    DNSServiceFlags flags,
    DNSServiceErrorType error,
    const char* name,
    const char* type,
    const char* domain,
    void* ctx
);
static DNSServiceErrorType MdnsCreateTextRecord(TXTRecordRef* txt_record, const Mdns* mdns);
static void MdnsBrowserReset(Mdns* self);
static void MdnsRecoverSessions(Mdns* self, uint32_t now_seconds);

void MdnsConstruct(
    Mdns* self,
    const char* ski,
    const EebusDeviceInfo* device_info,
    const char* service_name,
    int port,
    OnMdnsEntriesFoundCallback cb,
    void* ctx
) {
  *self = (Mdns){0};
  atomic_init(&self->cancel, false);
  atomic_init(&self->autoaccept, false);
  // Override "virtual functions table"
  SHIP_MDNS_INTERFACE(self) = &mdns_methods;

  self->ski                 = ski;
  self->device_info         = EebusDeviceInfoCopy(device_info);
  self->service_name        = service_name;
  self->port                = port;
  self->autoaccept          = false;
  self->on_entries_found_cb = cb;
  self->context             = ctx;

  self->provider_mutex           = EebusMutexCreateRecursive();
  self->snapshot_dirty           = false;
  self->refresh_cursor           = 0;
  self->thread                   = NULL;
  self->dns_service_browser_ref  = NULL;
  self->dns_service_register_ref = NULL;
  self->found_entries            = VectorCreateWithDeallocator(MdnsEntryDeallocator);
  self->active_resolves          = VectorCreateWithDeallocator(MdnsActiveResolveEntryDeallocator);

  self->cancel                     = false;
  self->needs_recovery             = false;
  self->next_recovery_time_seconds = 0;
}

ShipMdnsObject* ShipMdnsCreate(
    const char* ski,
    const EebusDeviceInfo* device_info,
    const char* service_name,
    int port,
    OnMdnsEntriesFoundCallback cb,
    void* ctx
) {
  Mdns* const mdns = (Mdns*)EEBUS_MALLOC(sizeof(Mdns));
  if (mdns == NULL) {
    return NULL;
  }

  MdnsConstruct(mdns, ski, device_info, service_name, port, cb, ctx);
  if (mdns->provider_mutex == NULL || mdns->device_info == NULL || mdns->active_resolves == NULL
      || mdns->found_entries == NULL) {
    SHIP_MDNS_DESTRUCT(SHIP_MDNS_OBJECT(mdns));
    EEBUS_FREE(mdns);
    return NULL;
  }

  return SHIP_MDNS_OBJECT(mdns);
}

static ActiveResolveEntry* MdnsActiveResolveEntryCreate(Mdns* owner, MdnsEntry* entry) {
  ActiveResolveEntry* resolve = EEBUS_MALLOC(sizeof(*resolve));
  if (resolve == NULL) return NULL;
  *resolve = (ActiveResolveEntry){.entry = entry, .owner = owner, .dirty = true};
  return resolve;
}

static void MdnsActiveResolveEntryDestroy(ActiveResolveEntry* resolve) {
  if (resolve == NULL) return;
  if (resolve->service_ref != NULL) DNSServiceRefDeallocate(resolve->service_ref);
  if (resolve->txt_ref != NULL) DNSServiceRefDeallocate(resolve->txt_ref);
  if (resolve->srv_ref != NULL) DNSServiceRefDeallocate(resolve->srv_ref);
  MdnsEntryDelete(resolve->entry);
  EEBUS_FREE(resolve->txt_data);
  EEBUS_FREE(resolve->srv_data);
  EEBUS_FREE(resolve);
}

static void MdnsActiveResolveEntryDeallocator(void* resolve) {
  MdnsActiveResolveEntryDestroy((ActiveResolveEntry*)resolve);
}

static uint32_t MdnsGetCurrentTimeSeconds(void);
static bool SameService(const MdnsEntry* a, const MdnsEntry* b) {
  return a->iface == b->iface && StringEqualsIgnoreCase(a->name, b->name)
         && StringEqualsIgnoreCase(a->domain, b->domain);
}
static void RemoveCachedService(ActiveResolveEntry* service) {
  Vector* entries = service->owner->found_entries;
  for (size_t i = 0; i < VectorGetSize(entries);) {
    MdnsEntry* entry = VectorGetElement(entries, i);
    if (SameService(entry, service->entry)) {
      VectorRemove(entries, entry);
      MdnsEntryDelete(entry);
      service->owner->snapshot_dirty = true;
    } else
      ++i;
  }
}
static bool SameMetadata(const MdnsEntry* a, const MdnsEntry* b) {
  if (a->port != b->port) return false;
  const char* av[] = {a->host, a->txtvers, a->id, a->path, a->ski, a->reg, a->brand, a->type, a->model};
  const char* bv[] = {b->host, b->txtvers, b->id, b->path, b->ski, b->reg, b->brand, b->type, b->model};
  for (size_t i = 0; i < sizeof(av) / sizeof(av[0]); ++i) {
    if (av[i] == NULL || bv[i] == NULL) {
      if (av[i] != bv[i]) return false;
    } else if (strcmp(av[i], bv[i]) != 0)
      return false;
  }
  return true;
}
static void PublishMetadata(ActiveResolveEntry* service, MdnsEntry* fresh) {
  Mdns* owner = service->owner;
  if (!MdnsEntryIsValid(fresh) || StringEqualsIgnoreCase(fresh->ski, owner->ski)) {
    RemoveCachedService(service);
    MdnsEntryDelete(fresh);
    return;
  }
  for (size_t i = 0; i < VectorGetSize(owner->found_entries); ++i) {
    MdnsEntry* old = VectorGetElement(owner->found_entries, i);
    if (SameService(old, fresh) && SameMetadata(old, fresh)) {
      MdnsEntryDelete(fresh);
      return;
    }
  }
  RemoveCachedService(service);
  size_t before = VectorGetSize(owner->found_entries);
  VectorPushBack(owner->found_entries, fresh);
  if (before == VectorGetSize(owner->found_entries)) {
    MdnsEntryDelete(fresh);
    service->dirty = true;
  } else
    owner->snapshot_dirty = true;
}
static bool UpdateRecord(uint8_t** previous, size_t* previous_size, const void* data, size_t size) {
  if (*previous_size == size && (size == 0 || (*previous != NULL && memcmp(*previous, data, size) == 0))) return false;
  uint8_t* copy = size == 0 ? NULL : ArrayCopy(data, size, 1);
  if (size != 0 && copy == NULL) return true;
  EEBUS_FREE(*previous);
  *previous      = copy;
  *previous_size = size;
  return true;
}
static void MdnsRecordCallback(
    DNSServiceRef ref,
    DNSServiceFlags flags,
    uint32_t iface,
    DNSServiceErrorType error,
    const char* name,
    uint16_t type,
    uint16_t cls,
    uint16_t length,
    const void* data,
    uint32_t ttl,
    void* ctx
) {
  UNUSED(ref);
  UNUSED(iface);
  UNUSED(name);
  UNUSED(cls);
  UNUSED(ttl);
  ActiveResolveEntry* service = ctx;
  if (service->removed) return;
  if (type == kDNSServiceType_TXT)
    service->txt_batch = error == kDNSServiceErr_NoError && (flags & kDNSServiceFlagsMoreComing);
  else
    service->srv_batch = error == kDNSServiceErr_NoError && (flags & kDNSServiceFlagsMoreComing);
  if (error != kDNSServiceErr_NoError) {
    service->dirty          = true;
    service->watches_failed = true;
    service->endpoint_valid = false;
    ++service->revision;
    service->retry_at = MdnsGetCurrentTimeSeconds() + kMdnsRecoveryDelaySeconds;
    RemoveCachedService(service);
    return;
  }
  uint8_t** previous    = type == kDNSServiceType_TXT ? &service->txt_data : &service->srv_data;
  size_t* previous_size = type == kDNSServiceType_TXT ? &service->txt_size : &service->srv_size;
  if (!(flags & kDNSServiceFlagsAdd)) {
    if (*previous_size != length || (*previous != NULL && memcmp(*previous, data, length) != 0)) return;
    EEBUS_FREE(*previous);
    *previous               = NULL;
    *previous_size          = 0;
    service->dirty          = true;
    service->endpoint_valid = false;
    ++service->revision;
    RemoveCachedService(service);
    return;
  }
  if (!UpdateRecord(previous, previous_size, data, length)) return;
  ++service->revision;
  if (type == kDNSServiceType_TXT && service->endpoint_valid) {
    MdnsEntry* fresh = MdnsEntryCreate(service->entry->name, service->entry->domain, service->entry->iface);
    if (fresh != NULL && MdnsEntrySetHost(fresh, service->entry->host) == kEebusErrorOk) {
      MdnsEntrySetPort(fresh, service->entry->port);
      if (MdnsEntryParseTxtRecord(fresh, data, length) == kEebusErrorOk && MdnsEntryIsValid(fresh)) {
        PublishMetadata(service, fresh);
        return;
      }
    }
    MdnsEntryDelete(fresh);
  }
  service->dirty          = true;
  service->endpoint_valid = false;
  RemoveCachedService(service);
}

static void MdnsBrowserReset(Mdns* mdns) {
  mdns->browse_batch   = false;
  mdns->refresh_cursor = 0;
  if (mdns->dns_service_browser_ref != NULL) {
    DNSServiceRefDeallocate(mdns->dns_service_browser_ref);
    mdns->dns_service_browser_ref = NULL;
  }

  if (mdns->found_entries != NULL) {
    if (VectorGetSize(mdns->found_entries) != 0) mdns->snapshot_dirty = true;
    VectorFreeElements(mdns->found_entries);
    VectorClear(mdns->found_entries);
  }

  if (mdns->active_resolves != NULL) {
    VectorFreeElements(mdns->active_resolves);
    VectorClear(mdns->active_resolves);
  }
}

static void Destruct(ShipMdnsObject* self) {
  SHIP_MDNS_STOP(self);

  Mdns* const mdns = MDNS(self);
  MdnsBrowserReset(mdns);

  if (mdns->dns_service_register_ref != NULL) {
    DNSServiceRefDeallocate(mdns->dns_service_register_ref);
    mdns->dns_service_register_ref = NULL;
  }

  if (mdns->found_entries != NULL) {
    VectorFreeElements(mdns->found_entries);
    VectorDestruct(mdns->found_entries);
    EEBUS_FREE(mdns->found_entries);
    mdns->found_entries = NULL;
  }

  if (mdns->active_resolves != NULL) {
    VectorFreeElements(mdns->active_resolves);
    VectorDestruct(mdns->active_resolves);
    EEBUS_FREE(mdns->active_resolves);
    mdns->active_resolves = NULL;
  }

  EebusMutexDelete(mdns->provider_mutex);
  mdns->provider_mutex = NULL;
  EebusDeviceInfoDelete(mdns->device_info);
  mdns->device_info = NULL;
}

static bool MdnsProcessActiveResolveEntry(ActiveResolveEntry* resolve, const fd_set* readfds) {
  if (resolve->removed) return false;
  DNSServiceRef refs[] = {resolve->service_ref, resolve->txt_ref, resolve->srv_ref};
  for (size_t i = 0; i < 3; ++i) {
    if (refs[i] == NULL) continue;
    int fd = DNSServiceRefSockFD(refs[i]);
    if (fd < 0 || fd >= FD_SETSIZE) {
      resolve->dirty          = true;
      resolve->endpoint_valid = false;
      if (i == 0) {
        resolve->done           = true;
        resolve->resolve_failed = true;
      } else
        resolve->watches_failed = true;
      RemoveCachedService(resolve);
      continue;
    }
    if (FD_ISSET(fd, readfds) && DNSServiceProcessResult(refs[i]) != kDNSServiceErr_NoError) {
      resolve->dirty          = true;
      resolve->endpoint_valid = false;
      if (i == 0) {
        resolve->done           = true;
        resolve->resolve_failed = true;
      } else
        resolve->watches_failed = true;
      resolve->retry_at = MdnsGetCurrentTimeSeconds() + kMdnsRecoveryDelaySeconds;
      RemoveCachedService(resolve);
    }
  }
  return false;
}

static void MdnsProcessActiveResolves(Mdns* self, const fd_set* readfds) {
  for (size_t i = 0; i < VectorGetSize(self->active_resolves); ++i)
    MdnsProcessActiveResolveEntry(VectorGetElement(self->active_resolves, i), readfds);
}

static uint16_t OpaquePortToUint16(uint16_t opaque_port) {
  union {
    uint16_t s;
    uint8_t b[2];
  } port = {opaque_port};

  return ((uint16_t)port.b[0]) << 8 | port.b[1];
}

static void MdnsResolveServiceCallback(
    DNSServiceRef service_ref,
    const DNSServiceFlags flags,
    uint32_t iface,
    DNSServiceErrorType err,
    const char* name,
    const char* host,
    uint16_t opaque_port,
    uint16_t txt_record_size,
    const unsigned char* txt_record,
    void* ctx
) {
  UNUSED(service_ref);
  UNUSED(flags);
  UNUSED(iface);
  UNUSED(name);
  ActiveResolveEntry* resolve = ctx;
  if (resolve == NULL || resolve->removed) return;
  resolve->done           = true;
  resolve->resolve_failed = err != kDNSServiceErr_NoError;
  if (err != kDNSServiceErr_NoError || resolve->resolving_revision != resolve->revision) {
    resolve->dirty = true;
    return;
  }
  MdnsEntry* fresh = MdnsEntryCreate(resolve->entry->name, resolve->entry->domain, resolve->entry->iface);
  if (fresh == NULL) {
    resolve->dirty = true;
    return;
  }
  MdnsEntrySetPort(fresh, OpaquePortToUint16(opaque_port));
  if (MdnsEntrySetHost(fresh, host) != kEebusErrorOk
      || MdnsEntryParseTxtRecord(fresh, (const char*)txt_record, txt_record_size) != kEebusErrorOk
      || !MdnsEntryIsValid(fresh)) {
    MdnsEntryDelete(fresh);
    resolve->dirty          = true;
    resolve->resolve_failed = true;
    RemoveCachedService(resolve);
    return;
  }
  MdnsEntry* key = MdnsEntryCreate(fresh->name, fresh->domain, fresh->iface);
  if (key == NULL || MdnsEntrySetHost(key, host) != kEebusErrorOk) {
    MdnsEntryDelete(key);
    MdnsEntryDelete(fresh);
    resolve->dirty = true;
    return;
  }
  MdnsEntrySetPort(key, fresh->port);
  MdnsEntryDelete(resolve->entry);
  resolve->entry          = key;
  resolve->endpoint_valid = true;
  PublishMetadata(resolve, fresh);
}

static void MdnsBrowseServicesCallback(
    DNSServiceRef service_ref,
    DNSServiceFlags flags,
    uint32_t iface,
    DNSServiceErrorType err,
    const char* name,
    const char* type,
    const char* domain,
    void* ctx
) {
  UNUSED(service_ref);
  UNUSED(type);
  Mdns* mdns = ctx;
  if (mdns == NULL) return;
  mdns->browse_batch = err == kDNSServiceErr_NoError && (flags & kDNSServiceFlagsMoreComing);
  if (err != kDNSServiceErr_NoError) {
    mdns->needs_recovery = true;
    return;
  }
  MdnsEntry key = {.name = name, .domain = domain, .iface = iface};
  for (size_t i = 0; i < VectorGetSize(mdns->active_resolves); ++i) {
    ActiveResolveEntry* service = VectorGetElement(mdns->active_resolves, i);
    if (!service->removed && SameService(service->entry, &key)) {
      if (!(flags & kDNSServiceFlagsAdd)) {
        service->removed = true;
        RemoveCachedService(service);
      }
      return;
    }
  }
  if (!(flags & kDNSServiceFlagsAdd)) return;
  MdnsEntry* entry = MdnsEntryCreate(name, domain, iface);
  if (entry == NULL) {
    mdns->needs_recovery = true;
    return;
  }
  ActiveResolveEntry* service = MdnsActiveResolveEntryCreate(mdns, entry);
  if (service == NULL) {
    MdnsEntryDelete(entry);
    mdns->needs_recovery = true;
    return;
  }
  size_t before = VectorGetSize(mdns->active_resolves);
  VectorPushBack(mdns->active_resolves, service);
  if (before == VectorGetSize(mdns->active_resolves)) {
    MdnsActiveResolveEntryDestroy(service);
    mdns->needs_recovery = true;
  }
}

static void MdnsBrowseServices(Mdns* self) {
  if (self->dns_service_browser_ref != NULL) {
    DNSServiceRefDeallocate(self->dns_service_browser_ref);
    self->dns_service_browser_ref = NULL;
  }

  const DNSServiceErrorType err = DNSServiceBrowse(
      &self->dns_service_browser_ref,
      0,                             // No flags
      kDNSServiceInterfaceIndexAny,  // Browse on all network interfaces
      kShipServiceType,              // Browse for SHIP TCP services
      NULL,                          // Browse on the default domain (e.g. local.)
      MdnsBrowseServicesCallback,    // Callback function when Bonjour events occur
      self
  );  // Pass mDNS instance to callback

  if (err != kDNSServiceErr_NoError) {
    MDNS_DEBUG_PRINTF("DNSServiceBrowse() returned error %d\n", err);
    return;
  }

  if (self->dns_service_browser_ref == NULL) {
    MDNS_DEBUG_PRINTF("DNSServiceBrowse() created no service ref\n");
    return;
  }
}

static int MdnsPrepareSockFd(DNSServiceRef service_ref, fd_set* readfds, int* max_fd) {
  if (service_ref == NULL) {
    return -1;
  }

  const int fd = DNSServiceRefSockFD(service_ref);
  if (fd < 0 || fd >= FD_SETSIZE) {
    return -1;
  }

  FD_SET(fd, readfds);
  if (fd > *max_fd) {
    *max_fd = fd;
  }

  return fd;
}

static void MdnsDispatchReadyFds(Mdns* mdns, const fd_set* readfds, int browse_fd, int register_fd) {
  if ((browse_fd >= 0) && FD_ISSET(browse_fd, readfds)) {
    DNSServiceErrorType err = DNSServiceProcessResult(mdns->dns_service_browser_ref);
    if (err != kDNSServiceErr_NoError) {
      MDNS_DEBUG_PRINTF("dns_service_browser_ref processing error %d\n", err);
      mdns->needs_recovery = true;
    }
  }

  if ((register_fd >= 0) && FD_ISSET(register_fd, readfds)) {
    DNSServiceErrorType err = DNSServiceProcessResult(mdns->dns_service_register_ref);
    if (err != kDNSServiceErr_NoError) {
      MDNS_DEBUG_PRINTF("dns_service_register_ref processing error %d\n", err);
      mdns->needs_recovery = true;
    }
  }

  MdnsProcessActiveResolves(mdns, readfds);
}

static void MdnsNotifyFoundEntries(Mdns* mdns) {
  if (!mdns->snapshot_dirty || mdns->browse_batch) return;
  for (size_t i = 0; i < VectorGetSize(mdns->active_resolves); ++i) {
    ActiveResolveEntry* service = VectorGetElement(mdns->active_resolves, i);
    if (!service->removed && (service->txt_batch || service->srv_batch)) return;
  }
  Vector* copy = VectorCreateWithDeallocator(MdnsEntryDeallocator);
  if (copy == NULL) return;
  for (size_t i = 0; i < VectorGetSize(mdns->found_entries); ++i) {
    MdnsEntry* entry = MdnsEntryCopy(VectorGetElement(mdns->found_entries, i));
    size_t before    = VectorGetSize(copy);
    if (entry != NULL) VectorPushBack(copy, entry);
    if (entry == NULL || before == VectorGetSize(copy)) {
      MdnsEntryDelete(entry);
      VectorFreeElements(copy);
      VectorDestruct(copy);
      EEBUS_FREE(copy);
      return;
    }
  }
  mdns->snapshot_dirty = false;
  EEBUS_MUTEX_UNLOCK(mdns->provider_mutex);
  mdns->on_entries_found_cb(copy, mdns->context);
  EEBUS_MUTEX_LOCK(mdns->provider_mutex);
}

static uint32_t MdnsGetCurrentTimeSeconds() {
#ifdef _WIN32
  return (uint32_t)(GetTickCount64() / 1000);
#else
  struct timespec now;
  clock_gettime(CLOCK_MONOTONIC, &now);
  return (uint32_t)now.tv_sec;
#endif  // _WIN32
}

static void MdnsRecoverSessions(Mdns* mdns, uint32_t now_seconds) {
  if (mdns->needs_recovery == false) {
    return;
  }

  // Back off between attempts
  if ((int32_t)(now_seconds - mdns->next_recovery_time_seconds) < 0) {
    return;
  }

  MDNS_DEBUG_PRINTF("Recovering mDNS sessions\n");

  bool recovered = (RegisterService(SHIP_MDNS_OBJECT(mdns)) == kEebusErrorOk);

  MdnsBrowserReset(mdns);
  MdnsBrowseServices(mdns);
  recovered = recovered && (mdns->dns_service_browser_ref != NULL);

  mdns->needs_recovery             = !recovered;
  mdns->next_recovery_time_seconds = now_seconds + kMdnsRecoveryDelaySeconds;
}

static void MaintainServices(Mdns* mdns, uint32_t now) {
  size_t i       = 0;
  bool resolving = false;
  while (i < VectorGetSize(mdns->active_resolves)) {
    ActiveResolveEntry* service = VectorGetElement(mdns->active_resolves, i);
    if (service->removed) {
      VectorRemove(mdns->active_resolves, service);
      MdnsActiveResolveEntryDestroy(service);
      continue;
    }
    if (service->service_ref != NULL && (service->done || (int32_t)(now - service->resolve_deadline) >= 0)) {
      DNSServiceRefDeallocate(service->service_ref);
      service->service_ref = NULL;
      if (!service->done) service->dirty = true;
      service->retry_at = (!service->done || service->resolve_failed) ? now + kMdnsRecoveryDelaySeconds : now;
    }
    if (service->watches_failed) {
      if (service->txt_ref != NULL) DNSServiceRefDeallocate(service->txt_ref);
      if (service->srv_ref != NULL) DNSServiceRefDeallocate(service->srv_ref);
      service->txt_ref = service->srv_ref = NULL;
      service->watches_failed             = false;
      service->txt_batch = service->srv_batch = false;
      service->retry_at                       = now + kMdnsRecoveryDelaySeconds;
    }
    resolving |= service->service_ref != NULL;
    ++i;
  }
  size_t count            = VectorGetSize(mdns->active_resolves);
  unsigned watcher_budget = 2;
  size_t cursor           = mdns->refresh_cursor;
  for (size_t n = 0; n < count; ++n) {
    size_t index                = (cursor + n) % count;
    ActiveResolveEntry* service = VectorGetElement(mdns->active_resolves, index);
    if ((int32_t)(now - service->retry_at) < 0) continue;
    char fullname[kDNSServiceMaxDomainName];
    if (DNSServiceConstructFullName(fullname, service->entry->name, kShipServiceType, service->entry->domain)) continue;
    DNSServiceRef* refs[] = {&service->txt_ref, &service->srv_ref};
    uint16_t types[]      = {kDNSServiceType_TXT, kDNSServiceType_SRV};
    for (size_t j = 0; j < 2 && watcher_budget; ++j) {
      if (*refs[j] != NULL) continue;
      --watcher_budget;
      DNSServiceErrorType err = DNSServiceQueryRecord(
          refs[j],
          0,
          service->entry->iface,
          fullname,
          types[j],
          kDNSServiceClass_IN,
          MdnsRecordCallback,
          service
      );
      int fd = *refs[j] == NULL ? -1 : DNSServiceRefSockFD(*refs[j]);
      if (err != kDNSServiceErr_NoError || fd < 0 || fd >= FD_SETSIZE) {
        if (*refs[j] != NULL) DNSServiceRefDeallocate(*refs[j]);
        *refs[j]          = NULL;
        service->dirty    = true;
        service->retry_at = now + kMdnsRecoveryDelaySeconds;
      }
    }
    if (!resolving && (service->dirty || service->txt_ref == NULL || service->srv_ref == NULL)) {
      service->dirty              = false;
      service->done               = false;
      service->resolving_revision = service->revision;
      service->resolve_deadline   = now + kMdnsRecoveryDelaySeconds;
      DNSServiceErrorType err     = DNSServiceResolve(
          &service->service_ref,
          0,
          service->entry->iface,
          service->entry->name,
          kShipServiceType,
          service->entry->domain,
          MdnsResolveServiceCallback,
          service
      );
      int fd = service->service_ref == NULL ? -1 : DNSServiceRefSockFD(service->service_ref);
      if (err != kDNSServiceErr_NoError || fd < 0 || fd >= FD_SETSIZE) {
        if (service->service_ref != NULL) DNSServiceRefDeallocate(service->service_ref);
        service->service_ref = NULL;
        service->dirty       = true;
        service->retry_at    = now + kMdnsRecoveryDelaySeconds;
      } else
        resolving = true;
      mdns->refresh_cursor = (index + 1) % count;
    }
  }
}

static void* MdnsBrowserLoop(void* parameters) {
  Mdns* mdns = parameters;
  EEBUS_MUTEX_LOCK(mdns->provider_mutex);
  MdnsBrowseServices(mdns);
  if (mdns->dns_service_browser_ref == NULL) mdns->needs_recovery = true;
  while (!mdns->cancel) {
    uint32_t now = MdnsGetCurrentTimeSeconds();
    MdnsRecoverSessions(mdns, now);
    MaintainServices(mdns, now);
    fd_set readfds;
    FD_ZERO(&readfds);
    int maxfd       = -1;
    int browse_fd   = MdnsPrepareSockFd(mdns->dns_service_browser_ref, &readfds, &maxfd);
    int register_fd = MdnsPrepareSockFd(mdns->dns_service_register_ref, &readfds, &maxfd);
    if ((mdns->dns_service_browser_ref != NULL && browse_fd < 0)
        || (mdns->dns_service_register_ref != NULL && register_fd < 0))
      mdns->needs_recovery = true;
    for (size_t i = 0; i < VectorGetSize(mdns->active_resolves); ++i) {
      ActiveResolveEntry* service = VectorGetElement(mdns->active_resolves, i);
      MdnsPrepareSockFd(service->service_ref, &readfds, &maxfd);
      MdnsPrepareSockFd(service->txt_ref, &readfds, &maxfd);
      MdnsPrepareSockFd(service->srv_ref, &readfds, &maxfd);
    }
    struct timeval timeout = select_timeout;
    int n                  = select(maxfd + 1, &readfds, NULL, NULL, &timeout);
    if (n > 0)
      MdnsDispatchReadyFds(mdns, &readfds, browse_fd, register_fd);
    else if (n < 0 && errno != EINTR)
      mdns->needs_recovery = true;
    MdnsNotifyFoundEntries(mdns);
    /* Give public registration operations an opportunity between batches. */
    EEBUS_MUTEX_UNLOCK(mdns->provider_mutex);
    EEBUS_MUTEX_LOCK(mdns->provider_mutex);
  }
  MdnsBrowserReset(mdns);
  EEBUS_MUTEX_UNLOCK(mdns->provider_mutex);
  return NULL;
}

static void MdnsRegisterServiceCallback(
    DNSServiceRef ref,
    DNSServiceFlags flags,
    DNSServiceErrorType error,
    const char* name,
    const char* type,
    const char* domain,
    void* ctx
) {
  UNUSED(ref);
  UNUSED(flags);
  UNUSED(name);
  UNUSED(type);
  UNUSED(domain);

  Mdns* const mdns = (Mdns*)ctx;

  if (error == kDNSServiceErr_NoError) {
    MDNS_DEBUG_PRINTF("Service registered: %s.%s.%s\n", name, type, domain);
  } else {
    MDNS_DEBUG_PRINTF("Error registering service: %d\n", error);
    if (mdns != NULL) {
      mdns->needs_recovery = true;
    }
  }
}

static DNSServiceErrorType MdnsCreateTextRecord(TXTRecordRef* txt_record, const Mdns* mdns) {
  TXTRecordCreate(txt_record, 0, NULL);

  TRY_SET_TXT_RECORD_VALUE(txt_record, "txtvers", kShipServiceTxtVer);
  TRY_SET_TXT_RECORD_VALUE(txt_record, "id", mdns->service_name);
  TRY_SET_TXT_RECORD_VALUE(txt_record, "path", kShipServicePath);
  TRY_SET_TXT_RECORD_VALUE(txt_record, "ski", mdns->ski);

  const char* register_str = mdns->autoaccept ? "true" : "false";
  TRY_SET_TXT_RECORD_VALUE(txt_record, "register", register_str);
  TRY_SET_TXT_RECORD_VALUE(txt_record, "brand", mdns->device_info->brand);
  TRY_SET_TXT_RECORD_VALUE(txt_record, "type", mdns->device_info->type);
  TRY_SET_TXT_RECORD_VALUE(txt_record, "model", mdns->device_info->model);

  return kDNSServiceErr_NoError;
}

static EebusError RegisterServiceInternal(ShipMdnsObject* self) {
  Mdns* const mdns = MDNS(self);

  DNSServiceErrorType dns_service_err = kDNSServiceErr_NoError;

  TXTRecordRef txt_record;
  dns_service_err = MdnsCreateTextRecord(&txt_record, mdns);
  if (dns_service_err != kDNSServiceErr_NoError) {
    MDNS_DEBUG_PRINTF("MdnsCreateTextRecord() returned error %d\n", dns_service_err);
    return kEebusErrorInit;
  }

  if (mdns->dns_service_register_ref != NULL) {
    DNSServiceRefDeallocate(mdns->dns_service_register_ref);
    mdns->dns_service_register_ref = NULL;
  }

  dns_service_err = DNSServiceRegister(
      &mdns->dns_service_register_ref,
      0,                                  // No flags
      kDNSServiceInterfaceIndexAny,       // Register on all network interfaces
      mdns->service_name,                 // Service name
      kShipServiceType,                   // Service type
      NULL,                               // default domain (e.g. local.)
      NULL,                               // default host name
      htons(mdns->port),                  // Port number
      TXTRecordGetLength(&txt_record),    // TXT record length
      TXTRecordGetBytesPtr(&txt_record),  // TXT record
      MdnsRegisterServiceCallback,        // Callback function
      mdns                                // Callback context (owning Mdns instance)
  );

  TXTRecordDeallocate(&txt_record);
  if (dns_service_err != kDNSServiceErr_NoError) {
    MDNS_DEBUG_PRINTF("DNSServiceRegister() returned error %d\n", dns_service_err);
    return kEebusErrorActivate;
  }

  if (mdns->dns_service_register_ref == NULL) {
    MDNS_DEBUG_PRINTF("DNSServiceRegister() failed to create a service reference\n");
    return kEebusErrorMemoryAllocate;
  }

  return kEebusErrorOk;
}

static EebusError Start(ShipMdnsObject* self) {
  Mdns* const mdns = MDNS(self);

  if (mdns->thread != NULL) return kEebusErrorOk;
  mdns->cancel   = false;
  EebusError ret = RegisterService(self);
  if (ret != kEebusErrorOk) {
    MDNS_DEBUG_PRINTF("RegisterService() returned error %d\n", ret);
    return ret;
  }

  if (mdns->on_entries_found_cb == NULL) {
    MDNS_DEBUG_PRINTF("No callback function set\n");
    DeregisterService(self);
    return kEebusErrorInit;
  }

  mdns->thread = EebusThreadCreate(MdnsBrowserLoop, mdns, 4096);
  if (mdns->thread == NULL) {
    MDNS_DEBUG_PRINTF("EebusThreadCreate() failed\n");
    DeregisterService(self);
    return kEebusErrorThread;
  }

  return kEebusErrorOk;
}

static void DeregisterService(ShipMdnsObject* self) {
  Mdns* mdns = MDNS(self);
  if (mdns->provider_mutex == NULL) return;
  EEBUS_MUTEX_LOCK(mdns->provider_mutex);
  if (mdns->dns_service_register_ref != NULL) {
    DNSServiceRefDeallocate(mdns->dns_service_register_ref);
    mdns->dns_service_register_ref = NULL;
  }
  EEBUS_MUTEX_UNLOCK(mdns->provider_mutex);
}

static void Stop(ShipMdnsObject* self) {
  Mdns* const mdns = MDNS(self);

  mdns->cancel = true;

  if (mdns->thread != NULL) {
    EEBUS_THREAD_JOIN(mdns->thread);
    EebusThreadDelete(mdns->thread);
    mdns->thread = NULL;
  }

  DeregisterService(self);
}

static void SetAutoaccept(ShipMdnsObject* self, bool autoaccept) {
  Mdns* const mdns = MDNS(self);
  mdns->autoaccept = autoaccept;
}

static EebusError RegisterService(ShipMdnsObject* self) {
  Mdns* mdns = MDNS(self);
  EEBUS_MUTEX_LOCK(mdns->provider_mutex);
  EebusError err = RegisterServiceInternal(self);
  EEBUS_MUTEX_UNLOCK(mdns->provider_mutex);
  return err;
}
