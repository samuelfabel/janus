//! RESP protocol instance: decode → kernel → encode → callback.
use std::sync::{Arc, Mutex};

use janus_kernel::{
    is_write, lock_kernel, slot, ERR_SLOT_MIGRATING, ERR_SLOT_NOT_OWNED, Kernel, Role,
};
use janus_serializer::{DecodeOutcome, Serializer};
use janus_types::{Command, Response};

use crate::{AskEndpointMap, Protocol, ProtocolError, SlotEndpointMap};

const ERR_NOAUTH: &str = "NOAUTH Authentication required.";
const ERR_NOPERM: &str = "NOPERM this connection is read-only";
const ERR_SLOT_MAP_MISSING: &str = "ERR slot map not configured";

/// RESP protocol instance bound to a kernel and serializer.
///
/// Multi-connection servers use [`RespProtocol::shared`] with one
/// process-wide `Arc<Mutex<Kernel>>`. [`RespProtocol::new`] wraps a
/// private mutex for single-connection / unit fixtures.
///
/// The connection role is per instance (one per connection). While the
/// Kernel requires a password and this connection has no role, only `AUTH`
/// reaches the Kernel; every other command gets [`ERR_NOAUTH`]. A successful
/// `AUTH` sets the role from the password used; a failed one keeps the current
/// role. A [`Role::ReadOnly`] connection gets [`ERR_NOPERM`] for write
/// commands, which never reach the Kernel (so they are not queued in `MULTI`).
///
/// When composition injects a [`SlotEndpointMap`], ownership misses become
/// `-MOVED <slot> <endpoint>` and `CLUSTER SLOTS` returns the configured map.
/// An optional [`AskEndpointMap`] rewrites migrating-slot domain signals into
/// `-ASK <slot> <endpoint>`. `ASKING` sets a one-shot per-connection flag so the
/// next keyed command may hit an IMPORTING slot on this node.
pub struct RespProtocol<S>
where
    S: Serializer,
{
    kernel: Arc<Mutex<Kernel>>,
    serializer: S,
    role: Option<Role>,
    slot_map: Option<Arc<Mutex<SlotEndpointMap>>>,
    ask_map: Option<Arc<Mutex<AskEndpointMap>>>,
    /// One-shot Redis `ASKING` flag for the next domain command on this connection.
    asking: bool,
}

impl<S> RespProtocol<S>
where
    S: Serializer,
{
    /// Wrap `kernel` in a private mutex (single-connection / tests).
    pub fn new(kernel: Kernel, serializer: S) -> Self {
        Self::shared(Arc::new(Mutex::new(kernel)), serializer)
    }

    /// Canonical multi-connection path: share one process-wide kernel mutex.
    pub fn shared(kernel: Arc<Mutex<Kernel>>, serializer: S) -> Self {
        RespProtocol {
            kernel,
            serializer,
            role: None,
            slot_map: None,
            ask_map: None,
            asking: false,
        }
    }

    /// Attach a composition-owned slot→endpoint map (MOVED + CLUSTER SLOTS).
    ///
    /// Accepts a shared mutex so the control plane can reassign slots after
    /// resharding without rebuilding every connection.
    pub fn with_slot_map(mut self, map: Arc<Mutex<SlotEndpointMap>>) -> Self {
        self.slot_map = Some(map);
        self
    }

    /// Attach ASK destinations for in-flight slot migrations.
    pub fn with_ask_map(mut self, map: Arc<Mutex<AskEndpointMap>>) -> Self {
        self.ask_map = Some(map);
        self
    }
}

/// Run `command` on the locked kernel, applying the per-connection gates:
/// NOAUTH first, then NOPERM for writes on a read-only connection.
/// Execute one domain command with the authentication and authorization gates
/// associated with a protocol session.
///
/// Other protocol frontends can reuse the same per-connection role behavior
/// while keeping wire decoding and response encoding at their own boundary.
pub fn execute_gated(
    kernel: &mut Kernel,
    role: &mut Option<Role>,
    command: &Command<'_>,
) -> Response {
    execute_gated_asking(kernel, role, command, false)
}

impl<S> Protocol for RespProtocol<S>
where
    S: Serializer,
{
    fn execute(
        &mut self,
        buffer: &[u8],
        mut callback: impl FnMut(&[u8]),
    ) -> Result<usize, ProtocolError> {
        // Normative loop (LAYERS §2 / STREAM-PROCESSING §4):
        // offset ← 0
        // loop
        //   match decode(buffer[offset..])
        //     Incomplete → return Ok(offset)
        //     Err(e)     → return Err(e)
        //     Ok(cmd, n) → kernel; encode; callback; offset += n
        //
        // Concurrency (V5): decode and encode stay outside the kernel mutex;
        // lock only for Kernel::execute (poison → into_inner via lock_kernel).
        let mut offset = 0usize;

        while offset < buffer.len() {
            match self.serializer.decode_one(&buffer[offset..]) {
                DecodeOutcome::Incomplete => return Ok(offset),
                DecodeOutcome::Ok { command, consumed } => {
                    let response = self.dispatch(&command);
                    let encoded = self.serializer.encode(&response);
                    callback(&encoded);
                    offset += consumed;
                }
                DecodeOutcome::Invalid { message } => {
                    return Err(ProtocolError::Invalid { message });
                }
                DecodeOutcome::UnknownCommand { name } => {
                    return Err(ProtocolError::UnknownCommand { name });
                }
            }
        }

        Ok(offset)
    }
}

impl<S> RespProtocol<S>
where
    S: Serializer,
{
    fn dispatch(&mut self, command: &Command<'_>) -> Response {
        if matches!(command, Command::ClusterSlots) {
            return self.cluster_slots();
        }
        if matches!(command, Command::Asking) {
            return self.handle_asking();
        }
        let asking = self.asking;
        self.asking = false;
        let response = {
            let mut kernel = lock_kernel(&self.kernel);
            execute_gated_asking(&mut kernel, &mut self.role, command, asking)
        };
        rewrite_redirects(
            command,
            response,
            self.slot_map.as_deref(),
            self.ask_map.as_deref(),
        )
    }

    fn handle_asking(&mut self) -> Response {
        {
            let kernel = lock_kernel(&self.kernel);
            if kernel.auth_required() && self.role.is_none() {
                return Response::Error(ERR_NOAUTH.to_string());
            }
        }
        self.asking = true;
        Response::Empty
    }

    fn cluster_slots(&mut self) -> Response {
        {
            let kernel = lock_kernel(&self.kernel);
            if kernel.auth_required() && self.role.is_none() {
                return Response::Error(ERR_NOAUTH.to_string());
            }
        }
        match self.slot_map.as_ref() {
            Some(map) => map
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .cluster_slots_response(),
            None => Response::Error(ERR_SLOT_MAP_MISSING.to_string()),
        }
    }
}

/// Run `command` on the locked kernel with optional one-shot IMPORTING access.
pub fn execute_gated_asking(
    kernel: &mut Kernel,
    role: &mut Option<Role>,
    command: &Command<'_>,
    asking: bool,
) -> Response {
    if kernel.auth_required() {
        match (*role, command) {
            (None, Command::Auth { .. }) => {}
            (None, _) => return Response::Error(ERR_NOAUTH.to_string()),
            (Some(Role::ReadOnly), cmd) if is_write(cmd) => {
                return Response::Error(ERR_NOPERM.to_string());
            }
            _ => {}
        }
    }
    let response = if asking {
        kernel.execute_asking(command)
    } else {
        kernel.execute(command)
    };
    if let Command::Auth { password } = command {
        if response == Response::Empty {
            *role = kernel.role_for(password);
        }
    }
    response
}

/// Translate domain slot misses into Redis `-MOVED` / `-ASK` when maps are present.
fn rewrite_redirects(
    command: &Command<'_>,
    response: Response,
    slot_map: Option<&Mutex<SlotEndpointMap>>,
    ask_map: Option<&Mutex<AskEndpointMap>>,
) -> Response {
    match response {
        Response::Error(msg) if msg == ERR_SLOT_MIGRATING => {
            let Some(key) = command_key(command) else {
                return Response::Error(msg);
            };
            let key_slot = slot(key);
            let endpoint = ask_map.and_then(|m| {
                m.lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .endpoint_for_slot(key_slot)
                    .map(str::to_string)
            });
            match endpoint {
                Some(endpoint) => Response::Error(format!("ASK {key_slot} {endpoint}")),
                None => Response::Error(msg),
            }
        }
        Response::Error(msg) if msg == ERR_SLOT_NOT_OWNED => {
            let Some(map) = slot_map else {
                return Response::Error(msg);
            };
            let Some(key) = command_key(command) else {
                return Response::Error(msg);
            };
            let key_slot = slot(key);
            let endpoint = map
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .endpoint_for_slot(key_slot)
                .map(str::to_string);
            match endpoint {
                Some(endpoint) => Response::Error(format!("MOVED {key_slot} {endpoint}")),
                None => Response::Error(msg),
            }
        }
        other => other,
    }
}

fn command_key<'a>(command: &Command<'a>) -> Option<&'a [u8]> {
    match command {
        Command::Set { key, .. }
        | Command::Get { key }
        | Command::Delete { key }
        | Command::Expire { key, .. }
        | Command::Ttl { key } => Some(*key),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier, Mutex};
    use std::thread;

    use janus_kernel::{Kernel, SlotOwnership};
    use janus_serializer::RespSerializer;
    use janus_storage::memory::MemoryStorageEngine;

    fn protocol() -> RespProtocol<RespSerializer> {
        RespProtocol::new(Kernel::new(MemoryStorageEngine::new()), RespSerializer)
    }

    const SET_A: &[u8] = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n";
    const GET_A: &[u8] = b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n";
    const SET_B: &[u8] = b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n";
    const SET_KEY_VALUE: &[u8] = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";

    /// S1 — one complete SET → 1 callback `+OK`
    #[test]
    fn s1_one_complete_set() {
        let mut p = protocol();
        let mut responses = Vec::new();
        let offset = p
            .execute(SET_KEY_VALUE, |out| responses.push(out.to_vec()))
            .unwrap();
        assert_eq!(offset, SET_KEY_VALUE.len());
        assert_eq!(responses, vec![b"+OK\r\n".to_vec()]);
    }

    /// S2 — SET+GET same buffer → 2 callbacks in order
    #[test]
    fn s2_set_then_get_same_buffer() {
        let mut p = protocol();
        let mut buf = SET_A.to_vec();
        buf.extend_from_slice(GET_A);
        let mut responses = Vec::new();
        let offset = p.execute(&buf, |out| responses.push(out.to_vec())).unwrap();
        assert_eq!(offset, buf.len());
        assert_eq!(
            responses,
            vec![b"+OK\r\n".to_vec(), b"$1\r\n1\r\n".to_vec()]
        );
    }

    /// S3 — fragment mid-bulk → 0 callbacks; offset 0 (sobra = everything)
    #[test]
    fn s3_fragment_mid_bulk() {
        let mut p = protocol();
        // Truncate inside the key bulk of SET
        let partial = &SET_KEY_VALUE[..SET_KEY_VALUE.len() - 8];
        let mut responses = Vec::new();
        let offset = p
            .execute(partial, |out| responses.push(out.to_vec()))
            .unwrap();
        assert_eq!(offset, 0);
        assert!(responses.is_empty());
    }

    /// S4 — complete on second execute after Incomplete
    #[test]
    fn s4_complete_on_second_execute() {
        let mut p = protocol();
        let split = SET_KEY_VALUE.len() - 5;
        let first = &SET_KEY_VALUE[..split];
        let second = &SET_KEY_VALUE[split..];

        let mut responses = Vec::new();
        let offset1 = p
            .execute(first, |out| responses.push(out.to_vec()))
            .unwrap();
        assert_eq!(offset1, 0);
        assert!(responses.is_empty());

        let mut combined = first.to_vec();
        combined.extend_from_slice(second);
        let offset2 = p
            .execute(&combined, |out| responses.push(out.to_vec()))
            .unwrap();
        assert_eq!(offset2, combined.len());
        assert_eq!(responses, vec![b"+OK\r\n".to_vec()]);
    }

    /// S5 — 2 complete + 1 incomplete → 2 callbacks; offset = end of second
    #[test]
    fn s5_two_complete_plus_incomplete() {
        let mut p = protocol();
        let incomplete = &SET_B[..10];
        let mut buf = SET_A.to_vec();
        buf.extend_from_slice(GET_A);
        buf.extend_from_slice(incomplete);

        let mut responses = Vec::new();
        let offset = p.execute(&buf, |out| responses.push(out.to_vec())).unwrap();
        assert_eq!(offset, SET_A.len() + GET_A.len());
        assert_eq!(
            responses,
            vec![b"+OK\r\n".to_vec(), b"$1\r\n1\r\n".to_vec()]
        );
        assert_eq!(&buf[offset..], incomplete);
    }

    /// S6 — `*3` without body → Incomplete (offset 0)
    #[test]
    fn s6_star3_without_body() {
        let mut p = protocol();
        let mut responses = Vec::new();
        let offset = p
            .execute(b"*3\r\n", |out| responses.push(out.to_vec()))
            .unwrap();
        assert_eq!(offset, 0);
        assert!(responses.is_empty());
    }

    /// S7 — command `FOO` → Err (not Incomplete)
    #[test]
    fn s7_unknown_command_foo() {
        let mut p = protocol();
        let input = b"*2\r\n$3\r\nFOO\r\n$3\r\nkey\r\n";
        let mut responses = Vec::new();
        let err = p
            .execute(input, |out| responses.push(out.to_vec()))
            .unwrap_err();
        assert!(responses.is_empty());
        match err {
            ProtocolError::UnknownCommand { name } => assert_eq!(name, b"FOO"),
            other => panic!("unexpected {other:?}"),
        }
    }

    /// Incomplete + append + second execute completes one command
    #[test]
    fn incomplete_append_second_execute() {
        let mut p = protocol();
        let mut buffer = SET_KEY_VALUE[..12].to_vec();
        let mut responses = Vec::new();
        assert_eq!(
            p.execute(&buffer, |out| responses.push(out.to_vec()))
                .unwrap(),
            0
        );
        assert!(responses.is_empty());

        buffer.extend_from_slice(&SET_KEY_VALUE[12..]);
        let offset = p
            .execute(&buffer, |out| responses.push(out.to_vec()))
            .unwrap();
        assert_eq!(offset, buffer.len());
        assert_eq!(responses, vec![b"+OK\r\n".to_vec()]);
    }

    /// Two SETs + GET in one buffer → 3 callbacks with correct payloads
    #[test]
    fn two_sets_and_get_three_callbacks() {
        let mut p = protocol();
        let mut buf = SET_A.to_vec();
        buf.extend_from_slice(SET_B);
        buf.extend_from_slice(GET_A);

        let mut responses = Vec::new();
        let offset = p.execute(&buf, |out| responses.push(out.to_vec())).unwrap();
        assert_eq!(offset, buf.len());
        assert_eq!(
            responses,
            vec![
                b"+OK\r\n".to_vec(),
                b"+OK\r\n".to_vec(),
                b"$1\r\n1\r\n".to_vec(),
            ]
        );
    }

    #[test]
    fn empty_buffer_is_ok_zero() {
        let mut p = protocol();
        let mut n = 0;
        assert_eq!(p.execute(b"", |_| n += 1).unwrap(), 0);
        assert_eq!(n, 0);
    }

    #[test]
    fn invalid_framing_is_err() {
        let mut p = protocol();
        let err = p.execute(b"+OK\r\n", |_| {}).unwrap_err();
        assert!(matches!(err, ProtocolError::Invalid { .. }));
    }

    #[test]
    fn shared_kernel_two_threads_set_then_get() {
        let kernel = Arc::new(Mutex::new(Kernel::new(MemoryStorageEngine::new())));
        let barrier = Arc::new(Barrier::new(2));

        let set_kernel = Arc::clone(&kernel);
        let set_barrier = Arc::clone(&barrier);
        let setter = thread::spawn(move || {
            let mut p = RespProtocol::shared(set_kernel, RespSerializer);
            let mut responses = Vec::new();
            p.execute(SET_KEY_VALUE, |out| responses.push(out.to_vec()))
                .unwrap();
            assert_eq!(responses, vec![b"+OK\r\n".to_vec()]);
            set_barrier.wait();
        });

        let get_kernel = Arc::clone(&kernel);
        let get_barrier = Arc::clone(&barrier);
        let getter = thread::spawn(move || {
            get_barrier.wait();
            let mut p = RespProtocol::shared(get_kernel, RespSerializer);
            let mut responses = Vec::new();
            let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
            p.execute(get, |out| responses.push(out.to_vec())).unwrap();
            assert_eq!(responses, vec![b"$5\r\nvalue\r\n".to_vec()]);
        });

        setter.join().expect("setter");
        getter.join().expect("getter");
    }

    #[test]
    fn shared_kernel_interleaved_frames_no_panic() {
        let kernel = Arc::new(Mutex::new(Kernel::new(MemoryStorageEngine::new())));
        let mut handles = Vec::new();
        for id in 0u8..4 {
            let k = Arc::clone(&kernel);
            handles.push(thread::spawn(move || {
                let mut p = RespProtocol::shared(k, RespSerializer);
                let key = [b'k', id];
                let set = format!(
                    "*3\r\n$3\r\nSET\r\n$2\r\n{}\r\n$1\r\n{}\r\n",
                    std::str::from_utf8(&key).unwrap(),
                    id
                );
                let get = format!(
                    "*2\r\n$3\r\nGET\r\n$2\r\n{}\r\n",
                    std::str::from_utf8(&key).unwrap()
                );
                for _ in 0..20 {
                    let mut responses = Vec::new();
                    p.execute(set.as_bytes(), |out| responses.push(out.to_vec()))
                        .unwrap();
                    assert_eq!(responses, vec![b"+OK\r\n".to_vec()]);
                    responses.clear();
                    p.execute(get.as_bytes(), |out| responses.push(out.to_vec()))
                        .unwrap();
                    let expected = format!("$1\r\n{}\r\n", id);
                    assert_eq!(responses, vec![expected.as_bytes().to_vec()]);
                }
            }));
        }
        for h in handles {
            h.join().expect("worker");
        }
    }

    const AUTH_OK: &[u8] = b"*2\r\n$4\r\nAUTH\r\n$6\r\ns3cret\r\n";
    const AUTH_WRONG: &[u8] = b"*2\r\n$4\r\nAUTH\r\n$5\r\nwrong\r\n";
    const NOAUTH: &[u8] = b"-NOAUTH Authentication required.\r\n";

    fn protected_kernel() -> Kernel {
        Kernel::new(MemoryStorageEngine::new()).with_requirepass("s3cret")
    }

    fn run(p: &mut RespProtocol<RespSerializer>, input: &[u8]) -> Vec<Vec<u8>> {
        let mut responses = Vec::new();
        let offset = p
            .execute(input, |out| responses.push(out.to_vec()))
            .unwrap();
        assert_eq!(offset, input.len());
        responses
    }

    #[test]
    fn requirepass_gates_until_auth_succeeds() {
        let mut p = RespProtocol::new(protected_kernel(), RespSerializer);
        assert_eq!(run(&mut p, GET_A), vec![NOAUTH.to_vec()]);
        assert_eq!(run(&mut p, SET_A), vec![NOAUTH.to_vec()]);
        assert_eq!(
            run(&mut p, AUTH_WRONG),
            vec![b"-ERR invalid password\r\n".to_vec()]
        );
        assert_eq!(run(&mut p, GET_A), vec![NOAUTH.to_vec()]);
        assert_eq!(run(&mut p, AUTH_OK), vec![b"+OK\r\n".to_vec()]);
        assert_eq!(run(&mut p, GET_A), vec![b"$-1\r\n".to_vec()]);
        assert_eq!(run(&mut p, SET_A), vec![b"+OK\r\n".to_vec()]);
        assert_eq!(run(&mut p, GET_A), vec![b"$1\r\n1\r\n".to_vec()]);
    }

    #[test]
    fn gate_applies_per_command_within_one_buffer() {
        let mut p = RespProtocol::new(protected_kernel(), RespSerializer);
        let mut buf = GET_A.to_vec();
        buf.extend_from_slice(AUTH_OK);
        buf.extend_from_slice(GET_A);
        assert_eq!(
            run(&mut p, &buf),
            vec![NOAUTH.to_vec(), b"+OK\r\n".to_vec(), b"$-1\r\n".to_vec()]
        );
    }

    #[test]
    fn authentication_is_per_connection() {
        let kernel = Arc::new(Mutex::new(protected_kernel()));
        let mut first = RespProtocol::shared(Arc::clone(&kernel), RespSerializer);
        let mut second = RespProtocol::shared(kernel, RespSerializer);

        assert_eq!(run(&mut first, AUTH_OK), vec![b"+OK\r\n".to_vec()]);
        assert_eq!(run(&mut first, SET_A), vec![b"+OK\r\n".to_vec()]);
        assert_eq!(run(&mut second, GET_A), vec![NOAUTH.to_vec()]);
    }

    #[test]
    fn without_requirepass_commands_are_not_gated() {
        let mut p = protocol();
        assert_eq!(run(&mut p, SET_A), vec![b"+OK\r\n".to_vec()]);
        assert_eq!(
            run(&mut p, AUTH_OK),
            vec![b"-ERR AUTH called without any password configured\r\n".to_vec()]
        );
        assert_eq!(run(&mut p, GET_A), vec![b"$1\r\n1\r\n".to_vec()]);
    }

    const AUTH_RO: &[u8] = b"*2\r\n$4\r\nAUTH\r\n$4\r\nr3ad\r\n";
    const NOPERM: &[u8] = b"-NOPERM this connection is read-only\r\n";
    const DEL_A: &[u8] = b"*2\r\n$3\r\nDEL\r\n$1\r\na\r\n";
    const EXPIRE_A: &[u8] = b"*3\r\n$6\r\nEXPIRE\r\n$1\r\na\r\n$2\r\n10\r\n";
    const TTL_A: &[u8] = b"*2\r\n$3\r\nTTL\r\n$1\r\na\r\n";
    const SAVE: &[u8] = b"*1\r\n$4\r\nSAVE\r\n";
    const MULTI: &[u8] = b"*1\r\n$5\r\nMULTI\r\n";
    const EXEC: &[u8] = b"*1\r\n$4\r\nEXEC\r\n";
    const INFO: &[u8] = b"*1\r\n$4\r\nINFO\r\n";
    const TRACES: &[u8] = b"*1\r\n$6\r\nTRACES\r\n";

    fn roles_kernel() -> Arc<Mutex<Kernel>> {
        Arc::new(Mutex::new(protected_kernel().with_readonly_pass("r3ad")))
    }

    fn connect(kernel: &Arc<Mutex<Kernel>>, auth: &[u8]) -> RespProtocol<RespSerializer> {
        let mut p = RespProtocol::shared(Arc::clone(kernel), RespSerializer);
        assert_eq!(run(&mut p, auth), vec![b"+OK\r\n".to_vec()]);
        p
    }

    #[test]
    fn readonly_connection_reads_and_gets_noperm_on_writes() {
        let kernel = roles_kernel();
        let mut full = connect(&kernel, AUTH_OK);
        assert_eq!(run(&mut full, SET_A), vec![b"+OK\r\n".to_vec()]);

        let mut ro = connect(&kernel, AUTH_RO);
        assert_eq!(run(&mut ro, GET_A), vec![b"$1\r\n1\r\n".to_vec()]);
        assert_eq!(run(&mut ro, TTL_A), vec![b":-1\r\n".to_vec()]);
        for write in [SET_B, DEL_A, EXPIRE_A, SAVE] {
            assert_eq!(run(&mut ro, write), vec![NOPERM.to_vec()]);
        }
        assert_eq!(run(&mut ro, GET_A), vec![b"$1\r\n1\r\n".to_vec()]);
        assert_eq!(run(&mut ro, TTL_A), vec![b":-1\r\n".to_vec()]);
    }

    #[test]
    fn full_connection_writes() {
        let kernel = roles_kernel();
        let mut full = connect(&kernel, AUTH_OK);
        assert_eq!(run(&mut full, SET_A), vec![b"+OK\r\n".to_vec()]);
        assert_eq!(run(&mut full, EXPIRE_A), vec![b":1\r\n".to_vec()]);
        assert_eq!(run(&mut full, DEL_A), vec![b":1\r\n".to_vec()]);
    }

    #[test]
    fn denied_write_inside_multi_is_not_queued() {
        let kernel = roles_kernel();
        let mut full = connect(&kernel, AUTH_OK);
        assert_eq!(run(&mut full, SET_A), vec![b"+OK\r\n".to_vec()]);

        let mut ro = connect(&kernel, AUTH_RO);
        assert_eq!(run(&mut ro, MULTI), vec![b"+OK\r\n".to_vec()]);
        assert_eq!(run(&mut ro, SET_B), vec![NOPERM.to_vec()]);
        assert_eq!(run(&mut ro, GET_A), vec![b"+QUEUED\r\n".to_vec()]);
        assert_eq!(run(&mut ro, EXEC), vec![b"*1\r\n$1\r\n1\r\n".to_vec()]);
    }

    #[test]
    fn roles_are_isolated_per_connection() {
        let kernel = roles_kernel();
        let mut ro = connect(&kernel, AUTH_RO);
        let mut full = connect(&kernel, AUTH_OK);
        let mut anonymous = RespProtocol::shared(Arc::clone(&kernel), RespSerializer);

        assert_eq!(run(&mut ro, SET_A), vec![NOPERM.to_vec()]);
        assert_eq!(run(&mut full, SET_A), vec![b"+OK\r\n".to_vec()]);
        assert_eq!(run(&mut ro, GET_A), vec![b"$1\r\n1\r\n".to_vec()]);
        assert_eq!(run(&mut anonymous, GET_A), vec![NOAUTH.to_vec()]);
    }

    #[test]
    fn last_successful_auth_sets_the_role() {
        let kernel = roles_kernel();
        let mut p = connect(&kernel, AUTH_RO);
        assert_eq!(run(&mut p, SET_A), vec![NOPERM.to_vec()]);
        assert_eq!(
            run(&mut p, AUTH_WRONG),
            vec![b"-ERR invalid password\r\n".to_vec()]
        );
        assert_eq!(run(&mut p, SET_A), vec![NOPERM.to_vec()]);
        assert_eq!(run(&mut p, AUTH_OK), vec![b"+OK\r\n".to_vec()]);
        assert_eq!(run(&mut p, SET_A), vec![b"+OK\r\n".to_vec()]);
        assert_eq!(run(&mut p, AUTH_RO), vec![b"+OK\r\n".to_vec()]);
        assert_eq!(run(&mut p, SET_B), vec![NOPERM.to_vec()]);
    }

    #[test]
    fn readonly_pass_alone_requires_auth_and_grants_readonly() {
        let kernel = Kernel::new(MemoryStorageEngine::new()).with_readonly_pass("r3ad");
        let mut p = RespProtocol::new(kernel, RespSerializer);
        assert_eq!(run(&mut p, GET_A), vec![NOAUTH.to_vec()]);
        assert_eq!(run(&mut p, AUTH_RO), vec![b"+OK\r\n".to_vec()]);
        assert_eq!(run(&mut p, GET_A), vec![b"$-1\r\n".to_vec()]);
        assert_eq!(run(&mut p, SET_A), vec![NOPERM.to_vec()]);
    }

    #[test]
    fn denied_writes_do_not_count_metrics_or_spans() {
        let kernel = roles_kernel();
        let mut full = connect(&kernel, AUTH_OK);
        let mut ro = connect(&kernel, AUTH_RO);
        assert_eq!(run(&mut ro, SET_A), vec![NOPERM.to_vec()]);
        assert_eq!(run(&mut ro, DEL_A), vec![NOPERM.to_vec()]);

        let info = run(&mut full, INFO).concat();
        let info = String::from_utf8(info).unwrap();
        assert!(info.contains("total_commands:3"), "unexpected INFO: {info}");

        let traces = run(&mut full, TRACES).concat();
        let traces = String::from_utf8(traces).unwrap();
        assert!(!traces.contains("SET"), "unexpected TRACES: {traces}");
        assert!(!traces.contains("DEL"), "unexpected TRACES: {traces}");
    }

    const CLUSTER_SLOTS: &[u8] = b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n";
    const GET_FOO: &[u8] = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
    const SET_FOO: &[u8] = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$1\r\n1\r\n";
    const SET_USER_A: &[u8] = b"*3\r\n$3\r\nSET\r\n$7\r\n{user}a\r\n$1\r\n1\r\n";
    const SET_USER_B: &[u8] = b"*3\r\n$3\r\nSET\r\n$7\r\n{user}b\r\n$1\r\n2\r\n";
    const SET_BAR: &[u8] = b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$1\r\n2\r\n";

    fn sharded_protocol() -> RespProtocol<RespSerializer> {
        // foo → 12182, bar → 5061; only own bar's slot locally.
        let bar_slot = janus_kernel::slot(b"bar");
        let kernel = Kernel::new(MemoryStorageEngine::new())
            .with_owned_slots(SlotOwnership::from_slots([bar_slot]));
        let map = Arc::new(Mutex::new(SlotEndpointMap::from_ranges([
            (0u16, 8191, "127.0.0.1:7000"),
            (8192, 16383, "127.0.0.1:7001"),
        ])));
        RespProtocol::new(kernel, RespSerializer).with_slot_map(map)
    }

    #[test]
    fn remote_slot_becomes_moved_with_endpoint() {
        let mut p = sharded_protocol();
        let out = run(&mut p, GET_FOO);
        assert_eq!(out, vec![b"-MOVED 12182 127.0.0.1:7001\r\n".to_vec()]);
    }

    #[test]
    fn owned_slot_succeeds() {
        let mut p = sharded_protocol();
        assert_eq!(run(&mut p, SET_BAR), vec![b"+OK\r\n".to_vec()]);
    }

    #[test]
    fn cluster_slots_returns_configured_map() {
        let mut p = sharded_protocol();
        let encoded = run(&mut p, CLUSTER_SLOTS).concat();
        let body = String::from_utf8_lossy(&encoded);
        assert!(body.starts_with("*2\r\n"), "got {body:?}");
        assert!(body.contains("127.0.0.1"));
        assert!(body.contains(":7000\r\n") || body.contains("7000"));
        assert!(body.contains("7001"));
    }

    #[test]
    fn cluster_slots_without_map_errors() {
        let mut p = protocol();
        assert_eq!(
            run(&mut p, CLUSTER_SLOTS),
            vec![b"-ERR slot map not configured\r\n".to_vec()]
        );
    }

    #[test]
    fn cross_slot_multi_is_stable_resp_error() {
        let mut p = protocol();
        assert_eq!(run(&mut p, MULTI), vec![b"+OK\r\n".to_vec()]);
        assert_eq!(run(&mut p, SET_FOO), vec![b"+QUEUED\r\n".to_vec()]);
        assert_eq!(run(&mut p, SET_BAR), vec![b"+QUEUED\r\n".to_vec()]);
        assert_eq!(
            run(&mut p, EXEC),
            vec![b"-CROSSSLOT Keys in request don't hash to the same slot\r\n".to_vec()]
        );
    }

    #[test]
    fn same_slot_hash_tags_succeed_over_resp() {
        let mut p = protocol();
        assert_eq!(run(&mut p, MULTI), vec![b"+OK\r\n".to_vec()]);
        assert_eq!(run(&mut p, SET_USER_A), vec![b"+QUEUED\r\n".to_vec()]);
        assert_eq!(run(&mut p, SET_USER_B), vec![b"+QUEUED\r\n".to_vec()]);
        assert_eq!(
            run(&mut p, EXEC),
            vec![b"*2\r\n+OK\r\n+OK\r\n".to_vec()]
        );
    }

    const ASKING: &[u8] = b"*1\r\n$6\r\nASKING\r\n";

    #[test]
    fn migrating_missing_key_becomes_ask() {
        let foo_slot = janus_kernel::slot(b"foo");
        let mut source_kernel = Kernel::new(MemoryStorageEngine::new())
            .with_owned_slots(SlotOwnership::from_slots([foo_slot]));
        source_kernel.begin_migrating(foo_slot);
        let map = Arc::new(Mutex::new(SlotEndpointMap::from_ranges([(
            0u16,
            16383,
            "127.0.0.1:7000",
        )])));
        let ask = Arc::new(Mutex::new(AskEndpointMap::from_slots([(
            foo_slot,
            "127.0.0.1:7001",
        )])));
        let mut p = RespProtocol::new(source_kernel, RespSerializer)
            .with_slot_map(map)
            .with_ask_map(ask);
        let out = run(&mut p, GET_FOO);
        assert_eq!(
            out,
            vec![format!("-ASK {foo_slot} 127.0.0.1:7001\r\n").into_bytes()]
        );
    }

    #[test]
    fn asking_allows_one_importing_query_then_moved_without() {
        use crate::AskEndpointMap;

        let foo_slot = janus_kernel::slot(b"foo");
        let mut source_kernel = Kernel::new(MemoryStorageEngine::new())
            .with_owned_slots(SlotOwnership::from_slots([foo_slot]));
        assert_eq!(
            source_kernel.execute(&janus_types::Command::Set {
                key: b"foo",
                value: b"v",
            }),
            janus_types::Response::Empty
        );
        source_kernel.begin_migrating(foo_slot);

        let mut dest_kernel =
            Kernel::new(MemoryStorageEngine::new()).with_owned_slots(SlotOwnership::from_slots([]));
        dest_kernel.begin_importing(foo_slot);
        let entries = source_kernel.export_slot(foo_slot);
        dest_kernel.import_slot_entries(&entries);

        let map = Arc::new(Mutex::new(SlotEndpointMap::from_ranges([(
            0u16,
            16383,
            "127.0.0.1:7000",
        )])));
        let ask = Arc::new(Mutex::new(AskEndpointMap::from_slots([(
            foo_slot,
            "127.0.0.1:7001",
        )])));

        let source_shared = Arc::new(Mutex::new(source_kernel));
        let dest_shared = Arc::new(Mutex::new(dest_kernel));
        let mut source = RespProtocol::shared(Arc::clone(&source_shared), RespSerializer)
            .with_slot_map(Arc::clone(&map))
            .with_ask_map(Arc::clone(&ask));
        let mut dest = RespProtocol::shared(Arc::clone(&dest_shared), RespSerializer)
            .with_slot_map(Arc::clone(&map))
            .with_ask_map(ask);

        assert_eq!(run(&mut dest, ASKING), vec![b"+OK\r\n".to_vec()]);
        assert_eq!(run(&mut dest, GET_FOO), vec![b"$1\r\nv\r\n".to_vec()]);
        // Without a fresh ASKING, IMPORTING slot stays a stable MOVED to the map owner.
        assert_eq!(
            run(&mut dest, GET_FOO),
            vec![format!("-MOVED {foo_slot} 127.0.0.1:7000\r\n").into_bytes()]
        );

        {
            let mut sk = lock_kernel(&source_shared);
            let mut dk = lock_kernel(&dest_shared);
            sk.finish_migrate_source(foo_slot);
            dk.finish_migrate_destination(foo_slot);
            map.lock().unwrap().assign_slot(foo_slot, "127.0.0.1:7001");
        }
        assert_eq!(run(&mut dest, GET_FOO), vec![b"$1\r\nv\r\n".to_vec()]);
        assert_eq!(
            run(&mut source, GET_FOO),
            vec![format!("-MOVED {foo_slot} 127.0.0.1:7001\r\n").into_bytes()]
        );
    }
}
