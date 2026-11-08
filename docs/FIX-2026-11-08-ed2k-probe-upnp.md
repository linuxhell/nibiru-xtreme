# ED2K probe port and UPnP discovery — 2026-11-08

The user diagnostic from ADM 0.8.13 still showed Low ID and repeated `upnp_no_gateway` results. Earlier diagnostics had shown successful forwarding after the server assigned Low ID. The ADM ordering fix remains necessary, but it does not repair peer handshake fields or lost SSDP requests.

## Confirmed defects

- Incoming HELLOANSWER used the shared peer encoder with a hardcoded TCP port of zero. The listener now passes the port actually bound, including an ephemeral fallback. Outgoing transfers without a supplied listener continue to use zero rather than invent a port.
- igd-next 0.18 sends one batch of M-SEARCH requests per search call. One lost batch could exhaust the entire discovery interval. The engine now schedules three searches within one shared deadline, and consults distinct local IPv4 addresses as well as the default route. Returning or timing out drops outstanding futures and sockets. Existing responder/description URL trust validation remains enabled.
- Previously the status counted only completed ED2K hellos. Accepted TCP connections are now counted separately so an open TCP socket can be distinguished from a successful protocol handshake.

## Reference and verification

The current aMule peer implementation writes its configured listening port in both HELLO and HELLOANSWER through `SendHelloTypePacket`:
https://github.com/amule-org/amule/blob/ec12f72bf7e22f018c3287314031ca3520b4289d/src/BaseClient.cpp

Regressions assert the listener port directly from a real HELLOANSWER packet, simulate a router dropping the first SSDP search batch, and check interface selection bounds and unchanged gateway URL validation. Existing incoming obfuscation, callback and upload tests stay enabled.

These tests do not prove that a particular public server will assign High ID on the user's network. Manual port forwarding, Windows filtering, routing and server behavior may still affect reachability. The actual ID returned by the server remains authoritative.
