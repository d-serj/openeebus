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
#include <pthread.h>
#include <string.h>
#include <time.h>

#include "src/common/debug.h"
#include "src/common/eebus_arguments.h"
#include "src/common/eebus_device_info.h"
#include "src/common/eebus_thread/eebus_thread.h"
#include "src/common/vector.h"
#include "src/ship/api/mdns_entry.h"
#include "src/ship/api/ship_mdns_interface.h"
#include "src/ship/ship_connection/types.h"

/** Set MDNS_DEBUG 1 to enable debug prints */
#define MDNS_DEBUG 0

/** mDNS debug printf(), enabled whith MDNS_DEBUG = 1 */
#if MDNS_DEBUG
#define MDNS_DEBUG_PRINTF(fmt, ...) DebugPrintf(fmt, ##__VA_ARGS__)
#else
#define MDNS_DEBUG_PRINTF(fmt, ...)
#endif  // MDNS_DEBUG

static const char* kShipServiceType   = "_ship._tcp";
static const char* kShipServicePath   = "/ship/";
static const char* kShipServiceTxtVer = "1";

static const struct timeval select_timeout = {
    .tv_sec  = 0,
    .tv_usec = 100000,
};

typedef struct Mdns Mdns;

/** Tracks a single pending DNSServiceResolve call */
typedef struct {
  /** The DNSServiceRef for this resolve */
  DNSServiceRef service_ref;
  /** The MdnsEntry being resolved */
  MdnsEntry* entry;
  /** The owning Mdns instance */
  Mdns* owner;
  /** Whether the resolve is done */
  bool done;
} ActiveResolveEntry;

struct Mdns {
  /** Implements the Mdns Interface */
  ShipMdnsObject obj;
  const char* ski;
  EebusDeviceInfo* device_info;
  const char* service_name;
  int port;
  bool autoaccept;

  OnMdnsEntriesFoundCallback on_entries_found_cb;
  void* context;

  EebusThreadObject* thread;
  DNSServiceRef dns_service_browser_ref;
  DNSServiceRef dns_service_register_ref;
  Vector* active_resolves;
  Vector* found_entries;
  pthread_cond_t mdns_browse_cond;
  pthread_mutex_t mdns_browse_mutex;

  bool is_browsing;
  bool service_registered;
  bool service_browse_done;
  bool cancel;
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
static void MdnsRemoveActiveResolve(Mdns* mdns, ActiveResolveEntry* resolve);
static bool MdnsHasMatchingEntry(const Mdns* mdns, const MdnsEntry* candidate);
static void MdnsProcessActiveResolves(Mdns* self, fd_set* readfds);
static void MdnsProcessResults(Mdns* self);
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
static void MdnsSleepRandomInterval(Mdns* self);

void MdnsConstruct(
    Mdns* self,
    const char* ski,
    const EebusDeviceInfo* device_info,
    const char* service_name,
    int port,
    OnMdnsEntriesFoundCallback cb,
    void* ctx
) {
  // Override "virtual functions table"
  SHIP_MDNS_INTERFACE(self) = &mdns_methods;

  self->ski                 = ski;
  self->device_info         = EebusDeviceInfoCopy(device_info);
  self->service_name        = service_name;
  self->port                = port;
  self->autoaccept          = false;
  self->on_entries_found_cb = cb;
  self->context             = ctx;

  self->thread                   = NULL;
  self->dns_service_browser_ref  = NULL;
  self->dns_service_register_ref = NULL;
  self->found_entries            = VectorCreateWithDeallocator(MdnsEntryDeallocator);
  self->active_resolves          = VectorCreateWithDeallocator(MdnsActiveResolveEntryDeallocator);

  pthread_cond_init(&self->mdns_browse_cond, NULL);
  pthread_mutex_init(&self->mdns_browse_mutex, NULL);

  self->is_browsing         = false;
  self->service_registered  = false;
  self->service_browse_done = false;
  self->cancel              = false;

  // Seed random number generator
  srand((int)time(NULL));
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

  MdnsConstruct(mdns, ski, device_info, service_name, port, cb, ctx);

  return SHIP_MDNS_OBJECT(mdns);
}

static ActiveResolveEntry* MdnsActiveResolveEntryCreate(Mdns* owner, MdnsEntry* entry) {
  ActiveResolveEntry* const resolve = (ActiveResolveEntry*)EEBUS_MALLOC(sizeof(ActiveResolveEntry));
  if (resolve == NULL) {
    return NULL;
  }

  resolve->service_ref = NULL;
  resolve->entry       = entry;
  resolve->owner       = owner;
  resolve->done        = false;

  return resolve;
}

static void MdnsActiveResolveEntryDestroy(ActiveResolveEntry* resolve) {
  if (resolve == NULL) {
    return;
  }

  if (resolve->service_ref != NULL) {
    DNSServiceRefDeallocate(resolve->service_ref);
    resolve->service_ref = NULL;
  }

  if (resolve->entry != NULL) {
    MdnsEntryDelete(resolve->entry);
    resolve->entry = NULL;
  }

  EEBUS_FREE(resolve);
}

void MdnsActiveResolveEntryDeallocator(void* resolve) {
  MdnsActiveResolveEntryDestroy((ActiveResolveEntry*)resolve);
}

static void MdnsRemoveActiveResolve(Mdns* mdns, ActiveResolveEntry* resolve) {
  if ((mdns == NULL) || (resolve == NULL)) {
    return;
  }

  VectorRemove(mdns->active_resolves, resolve);
  MdnsActiveResolveEntryDestroy(resolve);
}

static bool MdnsHasMatchingEntry(const Mdns* mdns, const MdnsEntry* candidate) {
  if ((mdns == NULL) || (mdns->found_entries == NULL) || (candidate == NULL)) {
    return false;
  }

  const char* const candidate_ski = MdnsEntryGetSki(candidate);
  if ((candidate_ski == NULL) || (candidate_ski[0] == '\0')) {
    return false;
  }

  const size_t found_count = VectorGetSize(mdns->found_entries);
  for (size_t i = 0; i < found_count; ++i) {
    MdnsEntry* const existing = (MdnsEntry*)VectorGetElement(mdns->found_entries, i);
    if (existing == NULL) {
      continue;
    }

    const char* const existing_ski = MdnsEntryGetSki(existing);
    if ((existing_ski != NULL) && (existing_ski[0] != '\0') && (strcmp(candidate_ski, existing_ski) == 0)) {
      return true;
    }
  }

  return false;
}

void MdnsBrowserReset(Mdns* mdns) {
  if (mdns->dns_service_browser_ref != NULL) {
    DNSServiceRefDeallocate(mdns->dns_service_browser_ref);
    mdns->dns_service_browser_ref = NULL;
  }

  if (mdns->found_entries != NULL) {
    VectorFreeElements(mdns->found_entries);
    VectorClear(mdns->found_entries);
  }

  if (mdns->active_resolves != NULL) {
    VectorFreeElements(mdns->active_resolves);
    VectorClear(mdns->active_resolves);
  }
}

void Destruct(ShipMdnsObject* self) {
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

  pthread_mutex_destroy(&mdns->mdns_browse_mutex);
  pthread_cond_destroy(&mdns->mdns_browse_cond);

  EebusDeviceInfoDelete(mdns->device_info);
  mdns->device_info = NULL;
}

void MdnsProcessActiveResolves(Mdns* self, fd_set* readfds) {
  const size_t resolves_num = VectorGetSize(self->active_resolves);
  if (resolves_num == 0) {
    return;
  }

  ActiveResolveEntry** resolves_to_remove
      = (ActiveResolveEntry**)EEBUS_MALLOC(resolves_num * sizeof(ActiveResolveEntry*));
  if (resolves_to_remove == NULL) {
    return;
  }

  uint32_t resolves_to_remove_idx = 0;

  for (size_t i = 0; i < resolves_num; ++i) {
    ActiveResolveEntry* const resolve = (ActiveResolveEntry*)VectorGetElement(self->active_resolves, i);
    if ((resolve == NULL) || (resolve->service_ref == NULL)) {
      continue;
    }

    if (resolve->done) {
      resolves_to_remove[resolves_to_remove_idx++] = resolve;
      continue;
    }

    int fd = DNSServiceRefSockFD(resolve->service_ref);
    if ((fd >= 0) && FD_ISSET(fd, readfds)) {
      MDNS_DEBUG_PRINTF("Processing resolve ref: %p\n", resolve->service_ref);
      DNSServiceErrorType err = DNSServiceProcessResult(resolve->service_ref);
      if (err != kDNSServiceErr_NoError) {
        MDNS_DEBUG_PRINTF("DNSServiceProcessResult resolve error %d\n", err);
        resolve->done = true;
      }
    }

    if (resolve->done) {
      resolves_to_remove[resolves_to_remove_idx++] = resolve;
    }
  }

  // Remove completed resolve refs
  const uint32_t resolves_to_remove_num = resolves_to_remove_idx;
  for (uint32_t i = 0; i < resolves_to_remove_num; ++i) {
    ActiveResolveEntry* const resolve = resolves_to_remove[i];
    if (resolve != NULL) {
      MDNS_DEBUG_PRINTF("Removing resolve ref: %p\n", resolve->service_ref);
      MdnsRemoveActiveResolve(self, resolve);
    }
  }

  EEBUS_FREE(resolves_to_remove);
}

void MdnsProcessResults(Mdns* self) {
  int max_fd = -1;
  fd_set readfds;
  bool stop_handling = false;

  while (!stop_handling && !self->cancel) {
    // 1. Set up the fd_set as usual here.
    FD_ZERO(&readfds);

    // 2. browse fd
    int browse_fd = DNSServiceRefSockFD(self->dns_service_browser_ref);
    if (browse_fd >= 0) {
      FD_SET(browse_fd, &readfds);
      if (browse_fd > max_fd) max_fd = browse_fd;
    }

    // 3. resolve fds
    for (size_t i = 0; i < VectorGetSize(self->active_resolves); ++i) {
      ActiveResolveEntry* const resolve = (ActiveResolveEntry*)VectorGetElement(self->active_resolves, i);
      if ((resolve == NULL) || (resolve->service_ref == NULL)) {
        continue;
      }

      int fd = DNSServiceRefSockFD(resolve->service_ref);
      if (fd >= 0) {
        FD_SET(fd, &readfds);
        if (fd > max_fd) {
          max_fd = fd;
        }
      }
    }

    // 4. Set up the timeout. Note: passing constant to select() directly leads to crash!
    struct timeval tv = select_timeout;

    const int result = select(max_fd + 1, &readfds, (fd_set*)NULL, (fd_set*)NULL, &tv);
    if (result > 0) {
      // browse
      if (browse_fd >= 0 && FD_ISSET(browse_fd, &readfds)) {
        DNSServiceErrorType err = DNSServiceProcessResult(self->dns_service_browser_ref);
        if (err) {
          MDNS_DEBUG_PRINTF("DNSServiceProcessResult returned %d\n", err);
          stop_handling = true;
        }
      }

      MdnsProcessActiveResolves(self, &readfds);
    } else if (result == 0) {
      stop_handling = true;
    } else {
      MDNS_DEBUG_PRINTF("select() returned %d errno %d %s\n", result, errno, strerror(errno));
      if (errno != EINTR) {
        stop_handling = true;
      }
    }
  }
}

uint16_t OpaquePortToUint16(uint16_t opaque_port) {
  union {
    uint16_t s;
    uint8_t b[2];
  } port = {opaque_port};

  return ((uint16_t)port.b[0]) << 8 | port.b[1];
}

void MdnsResolveServiceCallback(
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
  UNUSED(iface);

  MDNS_DEBUG_PRINTF("%s(), %s, ", __func__, name);

  ActiveResolveEntry* const resolve = (ActiveResolveEntry*)ctx;
  if ((resolve == NULL) || (resolve->owner == NULL)) {
    return;
  }

  if (err != kDNSServiceErr_NoError) {
    if (err == kDNSServiceErr_NoSuchRecord) {
      MDNS_DEBUG_PRINTF(" no Such Record\n");
    } else {
      MDNS_DEBUG_PRINTF(" error code: %d\n", err);
    }

    resolve->done = true;
    return;
  }

  const uint16_t port = OpaquePortToUint16(opaque_port);

  MDNS_DEBUG_PRINTF(" can be reached at %s:%u (interface %u)\n", host, port, iface);
  MDNS_DEBUG_PRINTF(", flags: %X\n", flags);

  // Don't show degenerate TXT records containing nothing but a single empty string
  if ((txt_record != NULL) && (txt_record_size > 1)) {
    MDNS_DEBUG_PRINTF(", txt_record: %s\n", txt_record);
  }

  Mdns* const mdns = resolve->owner;

  if (flags & kDNSServiceFlagsMoreComing) {
    MDNS_DEBUG_PRINTF("%s(), more coming...\n", __func__);
    return;
  }

  MdnsEntry* const entry = resolve->entry;
  if (entry == NULL) {
    MDNS_DEBUG_PRINTF("%s(), NULL mDNS entry\n", __func__);
    resolve->done = true;
    return;
  }

  MdnsEntrySetHost(entry, host);
  MdnsEntrySetPort(entry, port);
  MdnsEntryParseTxtRecord(entry, (const char*)txt_record, txt_record_size);
  resolve->done = true;

  const bool is_own_entry = (strcmp(entry->ski, mdns->ski) == 0);
  const bool is_valid     = MdnsEntryIsValid(entry);

  if (is_valid && !is_own_entry) {
    if (MdnsHasMatchingEntry(mdns, entry)) {
      MDNS_DEBUG_PRINTF("Ignoring duplicate entry: %s\n", entry->name);
      return;
    }

    // Transfer ownership of entry to found_entries vector
    VectorPushBack(mdns->found_entries, entry);
    resolve->entry = NULL;
  } else {
    char entry_info[1024];
    MDNS_DEBUG_PRINTF("Ignoring invalid or own entry: %s\n", MdnsEntryPrint(entry_info, sizeof(entry_info), entry));
    UNUSED(entry_info);
  }
}

void MdnsBrowseServicesCallback(
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

  if (err != kDNSServiceErr_NoError) {
    MDNS_DEBUG_PRINTF("Bonjour browser error occurred: %d\n", err);
    return;
  }

#if DEBUG_MDNS
  const char* const action = (flags & kDNSServiceFlagsAdd) ? "ADD" : "RMV";
  const char* const more   = (flags & kDNSServiceFlagsMoreComing) ? " (MORE)" : "";

  MDNS_DEBUG_PRINTF("%s %30s.%s%s on interface %d%s\n", action, name, type, domain, (int)iface, more);
#endif

  if (!(flags & kDNSServiceFlagsAdd)) {
    // We only care about added services
    return;
  }

  Mdns* const mdns = (Mdns*)ctx;
  if (mdns == NULL) {
    MDNS_DEBUG_PRINTF("mDNS browse callback with NULL context\n");
    return;
  }

  // Ignore own service
  if (strcmp(name, mdns->service_name) == 0) {
    MDNS_DEBUG_PRINTF("Ignoring own service: %s\n", name);
    return;
  }

  MdnsEntry* const entry = MdnsEntryCreate(name, domain, iface);
  if (entry == NULL) {
    MDNS_DEBUG_PRINTF("Failed to create mDNS entry\n");
    return;
  }

  ActiveResolveEntry* const resolve = MdnsActiveResolveEntryCreate(mdns, entry);
  if (resolve == NULL) {
    MDNS_DEBUG_PRINTF("Failed to allocate resolve entry\n");
    MdnsEntryDelete(entry);
    return;
  }

  DNSServiceRef service_resolve_ref = NULL;
  const DNSServiceErrorType res_err = DNSServiceResolve(
      &service_resolve_ref,
      0,
      iface,
      name,
      kShipServiceType,
      domain,
      MdnsResolveServiceCallback,
      resolve
  );

  if (res_err != kDNSServiceErr_NoError) {
    MDNS_DEBUG_PRINTF("DNSServiceResolve() returned error %d\n", res_err);
    if (service_resolve_ref != NULL) {
      DNSServiceRefDeallocate(service_resolve_ref);
      service_resolve_ref = NULL;
    }
    MdnsActiveResolveEntryDestroy(resolve);
    return;
  }

  if (service_resolve_ref == NULL) {
    MDNS_DEBUG_PRINTF("DNSServiceResolve() failed to create a service ref\n");
    MdnsActiveResolveEntryDestroy(resolve);
    return;
  }

  resolve->service_ref = service_resolve_ref;
  VectorPushBack(mdns->active_resolves, resolve);
  MDNS_DEBUG_PRINTF("Added resolve ref: %p\n", resolve->service_ref);
}

void MdnsBrowseServices(Mdns* self) {
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

void MdnsSleepRandomInterval(Mdns* self) {
  uint8_t update_interval
      = kMdnsBrowseIntervalMinSeconds + rand() % (kMdnsBrowseIntervalMaxSeconds - kMdnsBrowseIntervalMinSeconds);
  struct timespec timeout;
#ifdef _WIN32
  // Windows doesn't have clock_gettime, use time() + conversion
  time_t now      = time(NULL);
  timeout.tv_sec  = now + update_interval;
  timeout.tv_nsec = 0;
#else
  clock_gettime(CLOCK_REALTIME, &timeout);
  timeout.tv_sec += update_interval;
  timeout.tv_nsec = 0;
#endif
  MDNS_DEBUG_PRINTF("mDNS entry update interval: %us\n", update_interval);

  pthread_mutex_lock(&self->mdns_browse_mutex);
  pthread_cond_timedwait(&self->mdns_browse_cond, &self->mdns_browse_mutex, &timeout);
  pthread_mutex_unlock(&self->mdns_browse_mutex);
}

void* MdnsBrowserLoop(void* parameters) {
  Mdns* const mdns = (Mdns*)parameters;

  while (!mdns->cancel) {
    MdnsBrowseServices(mdns);
    MdnsProcessResults(mdns);
    MDNS_DEBUG_PRINTF("Number of found entries: %d\n", VectorGetSize(mdns->found_entries));
    mdns->on_entries_found_cb(mdns->found_entries, mdns->context);

    MdnsSleepRandomInterval(mdns);
    MdnsBrowserReset(mdns);
  }

  mdns->service_browse_done = true;

  return NULL;
}

void MdnsRegisterServiceCallback(
    DNSServiceRef ref,
    DNSServiceFlags flags,
    DNSServiceErrorType error,
    const char* name,
    const char* type,
    const char* domain,
    void* ctx
) {
  if (error == kDNSServiceErr_NoError) {
    MDNS_DEBUG_PRINTF("Service registered: %s.%s.%s\n", name, type, domain);
  } else {
    MDNS_DEBUG_PRINTF("Error registering service: %d\n", error);
  }
}

DNSServiceErrorType MdnsCreateTextRecord(TXTRecordRef* txt_record, const Mdns* mdns) {
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

EebusError RegisterService(ShipMdnsObject* self) {
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
      NULL                                // No callback context
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

  mdns->service_registered = true;

  return kEebusErrorOk;
}

EebusError Start(ShipMdnsObject* self) {
  Mdns* const mdns = MDNS(self);

  EebusError ret = RegisterService(self);
  if (ret != kEebusErrorOk) {
    MDNS_DEBUG_PRINTF("RegisterService() returned error %d\n", ret);
    return ret;
  }

  if (mdns->on_entries_found_cb == NULL) {
    MDNS_DEBUG_PRINTF("No callback function set\n");
    return kEebusErrorInit;
  }

  mdns->thread = EebusThreadCreate(MdnsBrowserLoop, mdns, 4096);
  if (mdns->thread == NULL) {
    MDNS_DEBUG_PRINTF("EebusThreadCreate() failed\n");
    return kEebusErrorThread;
  }

  return kEebusErrorOk;
}

void DeregisterService(ShipMdnsObject* self) {
  Mdns* const mdns = MDNS(self);

  if (mdns->dns_service_register_ref != NULL) {
    DNSServiceRefDeallocate(mdns->dns_service_register_ref);
    mdns->dns_service_register_ref = NULL;
  }

  mdns->service_registered = false;
}

void Stop(ShipMdnsObject* self) {
  Mdns* const mdns = MDNS(self);

  pthread_mutex_lock(&mdns->mdns_browse_mutex);
  mdns->cancel = true;
  pthread_cond_signal(&mdns->mdns_browse_cond);
  pthread_mutex_unlock(&mdns->mdns_browse_mutex);

  DeregisterService(self);
  if (mdns->thread != NULL) {
    EEBUS_THREAD_JOIN(mdns->thread);
    EebusThreadDelete(mdns->thread);
    mdns->thread = NULL;
  }
}

void SetAutoaccept(ShipMdnsObject* self, bool autoaccept) {
  Mdns* const mdns = MDNS(self);
  mdns->autoaccept = autoaccept;
}
