"""Loaded automatically (via PYTHONPATH) into every benchmark/test process.

Blocks every network connection that is not to this machine, so a benchmark can never reach a
printer, the filament store or anything else. Each blocked attempt is counted in
$BENCH_NET_LOG so the tests can prove that nothing tried.
"""
import ipaddress
import os
import socket

_real_connect = socket.socket.connect
_real_connect_ex = socket.socket.connect_ex


def _local(address):
    host = address[0] if isinstance(address, tuple) else address
    if not isinstance(host, str):
        return True                      # AF_UNIX and friends
    if host in ("localhost", ""):
        return True
    try:
        return ipaddress.ip_address(host.split("%")[0]).is_loopback
    except ValueError:
        return False                     # a DNS name: resolving it would already be a leak


def _note(address):
    path = os.environ.get("BENCH_NET_LOG")
    if path:
        with open(path, "a") as f:
            # The host name only (a benchmark has no real printer addresses), for --show-blocked.
            host = address[0] if isinstance(address, tuple) else address
            import traceback
            caller = ">".join(x.name for x in traceback.extract_stack()[:-3]
                              if "ams_tracker" in x.filename or "farmlord" in x.filename) or "?"
            f.write(f"{host}\t{caller}\n")


def _connect(self, address):
    if not _local(address):
        _note(address)
        raise OSError("network blocked by benchmark guard")
    return _real_connect(self, address)


def _connect_ex(self, address):
    if not _local(address):
        _note(address)
        return 111
    return _real_connect_ex(self, address)


def _getaddrinfo(host, *a, **k):
    if not _local((host,)):
        _note((host,))
        raise socket.gaierror("name resolution blocked by benchmark guard")
    return _real_getaddrinfo(host, *a, **k)


def _sendto(self, data, *rest):
    address = rest[-1]
    if not _local(address):
        _note(address)
        raise OSError("network blocked by benchmark guard")
    return _real_sendto(self, data, *rest)


_real_sendto = socket.socket.sendto
socket.socket.sendto = _sendto
_real_getaddrinfo = socket.getaddrinfo
socket.socket.connect = _connect
socket.socket.connect_ex = _connect_ex
socket.getaddrinfo = _getaddrinfo
