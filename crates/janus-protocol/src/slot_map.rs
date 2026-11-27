//! Slot → endpoint map for Redis Cluster client redirects (composition-owned).
//!
//! Lives at the protocol / control-plane edge. The Kernel never stores host:port.

use janus_kernel::SLOT_COUNT;
use janus_types::Response;

/// One contiguous slot range mapped to a RESP endpoint (`host:port`).
#[derive(Debug, Clone, PartialEq, Eq)]
struct SlotRange {
    start: u16,
    end: u16,
    endpoint: String,
}

/// Composition-owned map from hash slots to client endpoints.
///
/// Feeds `-MOVED` rewriting and the `CLUSTER SLOTS` discovery subset.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SlotEndpointMap {
    ranges: Vec<SlotRange>,
}

impl SlotEndpointMap {
    /// Build a map from inclusive `(start, end, endpoint)` ranges.
    ///
    /// Empty or inverted ranges are skipped. Slots at or above [`SLOT_COUNT`]
    /// are clamped out. Later ranges win on overlap when looking up a slot.
    pub fn from_ranges(
        ranges: impl IntoIterator<Item = (u16, u16, impl Into<String>)>,
    ) -> Self {
        let mut out = Vec::new();
        for (start, end, endpoint) in ranges {
            if start > end || start >= SLOT_COUNT {
                continue;
            }
            let end = end.min(SLOT_COUNT - 1);
            out.push(SlotRange {
                start,
                end,
                endpoint: endpoint.into(),
            });
        }
        Self { ranges: out }
    }

    /// All 16384 slots point at a single endpoint (single-shard advertisement).
    pub fn single_endpoint(endpoint: impl Into<String>) -> Self {
        Self::from_ranges([(0, SLOT_COUNT - 1, endpoint)])
    }

    /// Endpoint that owns `slot`, if configured.
    pub fn endpoint_for_slot(&self, slot: u16) -> Option<&str> {
        self.ranges
            .iter()
            .rev()
            .find(|r| slot >= r.start && slot <= r.end)
            .map(|r| r.endpoint.as_str())
    }

    /// Redis-compatible `CLUSTER SLOTS` nested array (subset: master only, no node id).
    ///
    /// Each entry is `[start, end, [host, port]]`.
    pub fn cluster_slots_response(&self) -> Response {
        let mut entries = Vec::with_capacity(self.ranges.len());
        for range in &self.ranges {
            let Some((host, port)) = split_endpoint(&range.endpoint) else {
                continue;
            };
            entries.push(Response::Array(vec![
                Response::Integer(i64::from(range.start)),
                Response::Integer(i64::from(range.end)),
                Response::Array(vec![
                    Response::Value(Some(host.as_bytes().to_vec())),
                    Response::Integer(port),
                ]),
            ]));
        }
        Response::Array(entries)
    }
}

/// Split `host:port` on the last `:`. Hostnames and IPv4 are supported; bare
/// IPv6 without brackets is not (use `hostname:port` or `[ipv6]:port` later).
fn split_endpoint(endpoint: &str) -> Option<(String, i64)> {
    let (host, port) = endpoint.rsplit_once(':')?;
    if host.is_empty() {
        return None;
    }
    let port: i64 = port.parse().ok()?;
    if port < 0 || port > 65535 {
        return None;
    }
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host)
        .to_string();
    Some((host, port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_and_cluster_slots() {
        let map = SlotEndpointMap::from_ranges([
            (0u16, 8191, "127.0.0.1:7000"),
            (8192, 16383, "127.0.0.1:7001"),
        ]);
        assert_eq!(map.endpoint_for_slot(0), Some("127.0.0.1:7000"));
        assert_eq!(map.endpoint_for_slot(12182), Some("127.0.0.1:7001"));
        assert_eq!(map.endpoint_for_slot(16384), None);

        match map.cluster_slots_response() {
            Response::Array(entries) => {
                assert_eq!(entries.len(), 2);
                match &entries[0] {
                    Response::Array(parts) => {
                        assert_eq!(parts[0], Response::Integer(0));
                        assert_eq!(parts[1], Response::Integer(8191));
                        assert_eq!(
                            parts[2],
                            Response::Array(vec![
                                Response::Value(Some(b"127.0.0.1".to_vec())),
                                Response::Integer(7000),
                            ])
                        );
                    }
                    other => panic!("unexpected entry: {other:?}"),
                }
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    #[test]
    fn single_endpoint_covers_all_slots() {
        let map = SlotEndpointMap::single_endpoint("10.0.0.1:6380");
        assert_eq!(map.endpoint_for_slot(0), Some("10.0.0.1:6380"));
        assert_eq!(
            map.endpoint_for_slot(SLOT_COUNT - 1),
            Some("10.0.0.1:6380")
        );
    }
}
