"""
Shared helpers for three-node (HEMS + HP + EV charger) tests.
"""

import re
import time


def wait_for_entity_addresses(hems, count: int, timeout: float) -> list:
    """Poll 'ma_mpc list' until at least `count` entity address strings appear.

    Returns the list of formatted entity address strings in connection order.
    The list header line is 'ma_mpc connected remotes (N):' and each address
    is printed on its own line indented with two spaces.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        position = hems.mark()
        hems.send("ma_mpc list")
        while time.monotonic() < deadline:
            lines = hems.records_since(position)
            header = next((re.search(r"ma_mpc connected remotes \((\d+)\):", line) for line in lines if "connected remotes" in line), None)
            if header:
                advertised = int(header[1])
                addresses = [line.strip() for line in lines if re.match(r"^\s{2}\S", line)]
                if len(addresses) == advertised:
                    if advertised >= count: return addresses
                    break
            if not hems.is_alive(): raise AssertionError("HEMS exited while listing entities")
            time.sleep(min(0.05, max(0, deadline - time.monotonic())))
    return []


def identify_mpc_sender(sender, hems, value, timeout):
    from _records import record_entity
    position = hems.mark()
    sender.send(f"mu_mpc set power_total {value}")
    line = hems.wait_for_new_line(f"MA MPC Measurement received: power_total = {value},", position, timeout)
    entity = record_entity(line)
    assert entity is not None, f"Missing entity in {line!r}"
    return entity


def wait_for_peer_readiness(hems, marker, skis, timeout, after=0):
    deadline = time.monotonic() + timeout
    return all(hems.wait_for_new(marker, after, max(0, deadline - time.monotonic()), peer_ski=ski)
               for ski in skis)
