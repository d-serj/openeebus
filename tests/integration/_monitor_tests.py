"""
Shared base class for monitoring use-case tests (MPC, MGCP, …).

Mirrors the _lp_tests.py pattern: domain logic lives here, conftest.py
stays focused on pytest infrastructure.
"""

from conftest import parse_field, DEFAULT_PROPAGATION, DEFAULT_SETTLE


class MeasurementVerifier:
    """Base class for a monitoring use case (MPC, MGCP, …).

    Subclass, set the three class-level string attributes, override the five
    marker/log methods, and call run() to drive the full set/get/verify cycle.
    A module-level singleton is the typical pattern:

        class _MpcConfig(MeasurementVerifier):
            hp_prefix = "mu_mpc"; hems_prefix = "ma_mpc"; hp_label = "MU"
            def receive_marker(self, n): return f"MA MPC Measurement received: {n}"
            …
        _CFG = _MpcConfig()

        def _set_and_verify(hp, hems, measurements):
            _CFG.run(hp, hems, measurements)
    """

    hp_prefix:   str
    hems_prefix: str
    hp_label:    str
    hems_label:  str   = "MA"
    propagation: float = DEFAULT_PROPAGATION
    settle:      float = DEFAULT_SETTLE

    def receive_marker(self, name: str) -> str: raise NotImplementedError
    def hp_settle(self,      name: str) -> str: raise NotImplementedError
    def hems_settle(self,    name: str) -> str: raise NotImplementedError
    def hp_log(self,   hp,   name: str) -> str: raise NotImplementedError
    def hems_log(self, hems, name: str) -> str: raise NotImplementedError

    def run(self, hp, hems, measurements):
        for name, value in measurements:
            pos = hems.mark()
            hp.send(f"{self.hp_prefix} set {name} {value}")
            assert hems.wait_for_new(self.receive_marker(name), pos, self.propagation, entity=hems.peer_entity, expected=f"{value},")
        pos_hp = hp.mark()
        for name, _ in measurements:
            hp.send(f"{self.hp_prefix} get {name}")
        pos_hems = hems.mark()
        for name, _ in measurements:
            hems.send(f"{self.hems_prefix} get {name}")

        for name, expected in measurements:
            hp_line = hp.wait_for_new_line(self.hp_settle(name), pos_hp, self.settle)
            hems_line = hems.wait_for_new_line(self.hems_settle(name), pos_hems, self.settle, entity=hems.peer_entity)
            hp_val = parse_field(hp_line, "value")
            hems_val = parse_field(hems_line, "value")
            assert hp_val == str(expected), f"{name}: expected {expected}, got {hp_val!r}"
            assert hems_val == str(expected), f"{name}: expected {expected}, got {hems_val!r}"
