// START_AI_HEADER
// MODULE: couplingd/src/queue.rs
// PURPOSE: Ordered persistent queue primitive for couplingd — Ярус 2, SPEC_coupling_v1 §3.
//          Implements QPUSH/QPOP/QPEEK: named queues with monotonic sequence numbers.
// INTENT: Stub for next agent. Signatures are final; bodies use in-memory VecDeque.
//         Next agent replaces with raft-backed durable queue.
// DEPENDENCIES: std, thiserror
// PUBLIC_API: QEntry, QueueError, QueueStore, push, pop, peek
// END_AI_HEADER

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};
use thiserror::Error;

/// A single enqueued item — mirrors capnp QEntry.
#[derive(Debug, Clone)]
pub struct QEntry {
    /// Queue name.
    pub queue:   String,
    /// Message payload (raw bytes).
    pub payload: Vec<u8>,
    /// Sequence number assigned by couplingd on enqueue (monotonic per queue).
    pub seq:     u64,
}

/// Errors produced by queue operations.
#[derive(Debug, Error)]
pub enum QueueError {
    #[error("queue '{0}' is empty")]
    Empty(String),
    #[error("store lock poisoned")]
    Poisoned,
}

/// Per-queue state.
struct QueueInner {
    items:      VecDeque<QEntry>,
    next_seq:   u64,
}

/// Thread-safe queue store (stub for raft-backed).
#[derive(Clone, Default)]
pub struct QueueStore {
    inner: Arc<Mutex<HashMap<String, QueueInner>>>,
}

impl QueueStore {
    // new:start
    //   purpose: Construct an empty QueueStore.
    //   input:  none
    //   output: QueueStore
    //   sideEffects: allocates Arc<Mutex<HashMap>>
    // new:end
    pub fn new() -> Self {
        Self::default()
    }
}

// push:start
//   purpose: Enqueue a payload on the named queue, assigning the next monotonic seq.
//            Creates the queue if it does not exist.
//            STUB — next agent replaces with raft-propose.
//   input:  store — shared QueueStore; queue — queue name; payload — message bytes
//   output: Result<u64, QueueError> — assigned sequence number
//   sideEffects: inserts entry into queue under Mutex write lock
// push:end
pub fn push(
    store:   &QueueStore,
    queue:   &str,
    payload: Vec<u8>,
) -> Result<u64, QueueError> {
    let mut guard = store.inner.lock().map_err(|_| QueueError::Poisoned)?;
    let q = guard.entry(queue.to_string()).or_insert_with(|| QueueInner {
        items:    VecDeque::new(),
        next_seq: 1,
    });

    let seq = q.next_seq;
    q.next_seq += 1;
    q.items.push_back(QEntry {
        queue:   queue.to_string(),
        payload,
        seq,
    });

    Ok(seq)
}

// pop:start
//   purpose: Dequeue and return the oldest entry from the named queue.
//            Returns QueueError::Empty if the queue has no messages.
//            STUB — next agent adds raft-committed dequeue + acknowledgement.
//   input:  store — shared QueueStore; queue — queue name
//   output: Result<QEntry, QueueError>
//   sideEffects: removes front entry under Mutex write lock
// pop:end
pub fn pop(store: &QueueStore, queue: &str) -> Result<QEntry, QueueError> {
    let mut guard = store.inner.lock().map_err(|_| QueueError::Poisoned)?;
    guard.get_mut(queue)
        .and_then(|q| q.items.pop_front())
        .ok_or_else(|| QueueError::Empty(queue.to_string()))
}

// peek:start
//   purpose: Return the oldest entry from the named queue without removing it.
//            Returns QueueError::Empty if the queue has no messages.
//            STUB — next agent may add stale-read vs. quorum-read flag.
//   input:  store — shared QueueStore; queue — queue name
//   output: Result<QEntry, QueueError>
//   sideEffects: Mutex read lock on store inner
// peek:end
pub fn peek(store: &QueueStore, queue: &str) -> Result<QEntry, QueueError> {
    let guard = store.inner.lock().map_err(|_| QueueError::Poisoned)?;
    guard.get(queue)
        .and_then(|q| q.items.front())
        .cloned()
        .ok_or_else(|| QueueError::Empty(queue.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: push a single ASCII string payload and return seq.
    fn push_str(store: &QueueStore, queue: &str, s: &str) -> u64 {
        push(store, queue, s.as_bytes().to_vec()).unwrap()
    }

    // --- seq monotonicity ---

    #[test]
    fn push_increments_seq_per_queue() {
        // Sequence numbers must be 1, 2, 3 for successive pushes on the same queue.
        let store = QueueStore::new();
        let s1 = push_str(&store, "q", "a");
        let s2 = push_str(&store, "q", "b");
        let s3 = push_str(&store, "q", "c");
        assert_eq!(s1, 1);
        assert_eq!(s2, 2);
        assert_eq!(s3, 3);
    }

    #[test]
    fn seq_is_independent_between_queues() {
        // Each queue maintains its own counter; pushing on "q2" must not
        // affect "q1"'s next_seq and vice-versa.
        let store = QueueStore::new();
        let a1 = push_str(&store, "q1", "x");
        let b1 = push_str(&store, "q2", "y");
        let b2 = push_str(&store, "q2", "z");
        let a2 = push_str(&store, "q1", "w");
        // q1: 1, 2
        assert_eq!(a1, 1);
        assert_eq!(a2, 2);
        // q2: 1, 2 — independent
        assert_eq!(b1, 1);
        assert_eq!(b2, 2);
    }

    // --- FIFO ordering ---

    #[test]
    fn pop_is_fifo() {
        // Items must arrive in the same order they were pushed.
        let store = QueueStore::new();
        push_str(&store, "fifo", "first");
        push_str(&store, "fifo", "second");
        push_str(&store, "fifo", "third");

        let e1 = pop(&store, "fifo").unwrap();
        let e2 = pop(&store, "fifo").unwrap();
        let e3 = pop(&store, "fifo").unwrap();

        assert_eq!(e1.payload, b"first");
        assert_eq!(e2.payload, b"second");
        assert_eq!(e3.payload, b"third");
        // Seq numbers must also be in ascending order.
        assert!(e1.seq < e2.seq && e2.seq < e3.seq);
    }

    // --- Empty queue errors ---

    #[test]
    fn pop_empty_returns_empty_error() {
        let store = QueueStore::new();
        let err = pop(&store, "nosuch").unwrap_err();
        assert!(matches!(err, QueueError::Empty(_)));
    }

    #[test]
    fn pop_after_drain_returns_empty_error() {
        let store = QueueStore::new();
        push_str(&store, "drain", "only");
        pop(&store, "drain").unwrap(); // consume the single entry
        let err = pop(&store, "drain").unwrap_err();
        assert!(matches!(err, QueueError::Empty(_)));
    }

    // --- peek semantics ---

    #[test]
    fn peek_does_not_remove_entry() {
        // peek must return the head, and the subsequent pop must return
        // the exact same entry (same seq + payload).
        let store = QueueStore::new();
        push_str(&store, "pk", "head");
        push_str(&store, "pk", "tail");

        let peeked = peek(&store, "pk").unwrap();
        assert_eq!(peeked.payload, b"head");

        // pop should still yield the head, not the tail.
        let popped = pop(&store, "pk").unwrap();
        assert_eq!(popped.seq, peeked.seq);
        assert_eq!(popped.payload, peeked.payload);
    }

    #[test]
    fn peek_empty_returns_empty_error() {
        let store = QueueStore::new();
        let err = peek(&store, "empty").unwrap_err();
        assert!(matches!(err, QueueError::Empty(_)));
    }

    // --- seq continuity after drain ---
    //
    // Design decision: next_seq is NOT reset when a queue is drained.
    // After push(1), pop(), push(2) the second push gets seq = 2, not 1.
    // This guarantees that no two entries on the same queue ever share a seq,
    // which is important for deduplication and raft log alignment.
    #[test]
    fn seq_continues_after_drain() {
        let store = QueueStore::new();
        let s1 = push_str(&store, "cont", "alpha");
        assert_eq!(s1, 1);

        pop(&store, "cont").unwrap(); // drain

        let s2 = push_str(&store, "cont", "beta");
        // Must be 2, not 1 — counter is never reset.
        assert_eq!(s2, 2);

        let s3 = push_str(&store, "cont", "gamma");
        assert_eq!(s3, 3);
    }
}
