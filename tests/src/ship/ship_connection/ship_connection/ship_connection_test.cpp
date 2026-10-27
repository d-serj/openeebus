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
#include "src/ship/ship_connection/ship_connection.h"

#include <gtest/gtest.h>

#include <string_view>

#include "src/common/eebus_malloc.h"
#include "tests/src/ship/ship_connection/ship_connection/ship_connection_test_suite.h"

using std::literals::string_view_literals::operator""sv;

TEST_F(ShipConnectionTestSuite, ShipConnectionCreationTest) {
  EXPECT_EQ(sc.role, kShipRoleAuto);
  EXPECT_EQ(SHIP_CONNECTION_GET_SHIP_STATE(&sc, nullptr), kCmiStateClientSend);
  EXPECT_EQ(sc.sme_error, kEebusErrorOk);
  EXPECT_EQ(std::string_view(SHIP_CONNECTION_GET_REMOTE_SKI(&sc)), kShipRemoteSki);
  ExpectConnectionClose("", false);
}

TEST_F(ShipConnectionTestSuite, CancelledTimeoutWakeDoesNotTimeoutNewReceive) {
  sc.timer_arms[1].pending = true;
  const ShipConnectionQueueMessage wake{.type = kShipConnectionQueueMsgTypeWake, .msg_buf = {}};
  ASSERT_EQ(EEBUS_QUEUE_SEND(sc.msg_queue, &wake, 0), kEebusErrorOk);
  EXPECT_CALL(*spr_timer_mock->gmock, Stop(sc.send_prolongation_request_timer));
  ShipConnectionTimerStop(&sc, sc.send_prolongation_request_timer);
  ShipConnectionQueueMessage data{.type = kShipConnectionQueueMsgTypeDataReceived, .msg_buf = {}};
  ASSERT_EQ(MessageBufferInitHelper(&data.msg_buf, "{}"sv), kEebusErrorOk);
  ASSERT_EQ(EEBUS_QUEUE_SEND(sc.msg_queue, &data, 0), kEebusErrorOk);
  EXPECT_CALL(*wfr_timer_mock->gmock, Start(sc.wait_for_ready_timer, 1000, false));
  EXPECT_CALL(*wfr_timer_mock->gmock, Stop(sc.wait_for_ready_timer));
  MessageBuffer received{};
  EXPECT_EQ(ShipConnectionReceive(&sc, &received, 1000), kEebusErrorOk);
  EXPECT_NE(received.data, nullptr);
  MessageBufferRelease(&received);
  ExpectConnectionClose("", false);
}
