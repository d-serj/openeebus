/* Copyright 2026 NIBE AB. Licensed under the Apache License, Version 2.0. */
#include <assert.h>
#include <libwebsockets.h>
static unsigned destroyed, cleared, closed;
static void ClearUser(struct lws* wsi, void* user) {
  assert(wsi != NULL && user == NULL);
  ++cleared;
}
#define lws_set_wsi_user ClearUser
#define lws_service(context, timeout) ((void)(context), (void)(timeout), -1)
#define lws_sul_schedule(context, tsi, sul, cb, us) ((void)(context), (void)(tsi), (void)(sul), (void)(cb), (void)(us))
#define lws_sul_cancel(sul) ((void)(sul))
#define lws_context_destroy(context) ((void)(context), (void)++destroyed)
#ifndef HTTP_SERVER_SOURCE
#define HTTP_SERVER_SOURCE "src/ship/websocket/http_server.c"
#endif
#include HTTP_SERVER_SOURCE
static void OnClose(WebsocketCallbackType type, const void* data, size_t size, void* ctx) {
  (void)data;
  (void)size;
  (void)ctx;
  assert(type == kWebsocketCallbackTypeClose);
  ++closed;
}
int main(void) {
  HttpServer server = {0};
  server.mutex      = EebusMutexCreateRecursive();
  server.lws_ctx    = (struct lws_context*)1;
  VectorConstructWithDeallocator(&server.wsi_ws_entries, WsiWsEntryDelete);
  Websocket ws;
  assert(WebsocketConstruct(&ws, OnClose, NULL) == kEebusErrorOk);
  ws.wsi     = (struct lws*)2;
  ws.lws_ctx = server.lws_ctx;
  VectorPushBack(&server.wsi_ws_entries, WsiWsEntryCreate(ws.wsi, WEBSOCKET_OBJECT(&ws)));
  HttpServerConnectionLoop(&server);
  assert(server.lws_ctx == NULL && VectorGetSize(&server.wsi_ws_entries) == 0);
  bool detached = false;
  assert(EEBUS_QUEUE_RECEIVE(ws.retirement_reply, &detached, 0) == kEebusErrorOk && detached);
  assert(closed == 1 && cleared == 1 && destroyed == 1);
  assert(ws.wsi == NULL && ws.lws_ctx == NULL && ws.is_closed);
  WebsocketDestruct(WEBSOCKET_OBJECT(&ws));
  VectorDestruct(&server.wsi_ws_entries);
  EebusMutexDelete(server.mutex);
  return 0;
}
