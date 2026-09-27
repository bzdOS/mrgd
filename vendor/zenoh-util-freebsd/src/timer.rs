// START_AI_HEADER
// MODULE: zenoh-util-freebsd/src/timer.rs
// PURPOSE: Async timer for scheduling one-shot and periodic events with defuse support.
// INTENT: Provides a tokio-based timer using a BinaryHeap for event ordering and flume channels for communication.
// DEPENDENCIES: std (collections, sync, time), async_trait, flume, tokio, zenoh_core
// PUBLIC_API: Timer, TimedEvent, TimedHandle, Timed trait
// END_AI_HEADER

//
// Copyright (c) 2023 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
use std::{
    cmp::Ordering as ComparisonOrdering,
    collections::BinaryHeap,
    sync::{
        atomic::{AtomicBool, Ordering as AtomicOrdering},
        Arc, Weak,
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use flume::{bounded, Receiver, RecvError, Sender};
use tokio::{runtime::Handle, select, sync::Mutex, task, time};
use zenoh_core::zconfigurable;

zconfigurable! {
    static ref TIMER_EVENTS_CHANNEL_SIZE: usize = 1;
}

#[async_trait]
pub trait Timed {
    // run:start
//   purpose: Execute the timed action when the scheduled instant is reached.
//   input:  &mut self - mutable reference to the implementor's state.
//   output: ()
//   sideEffects: depends on implementation; typically modifies implementor state.
    async fn run(&mut self);
}

type TimedFuture = Arc<dyn Timed + Send + Sync>;

#[derive(Clone)]
pub struct TimedHandle(Weak<AtomicBool>);

impl TimedHandle {
    // defuse:start
//   purpose: Disarm a scheduled event so it will not fire.
//   input:  self - the handle (consumed).
//   output: ()
//   sideEffects: atomically sets the fused flag to false (Release ordering).
    pub fn defuse(self) {
        if let Some(arc) = self.0.upgrade() {
            arc.store(false, AtomicOrdering::Release);
        }
    }
    // defuse:end
}
    // run:end

#[derive(Clone)]
pub struct TimedEvent {
    when: Instant,
    period: Option<Duration>,
    future: TimedFuture,
    fused: Arc<AtomicBool>,
}

impl TimedEvent {
    // once:start
//   purpose: Create a one-shot TimedEvent that fires once at the specified Instant.
//   input:  when - Instant to fire at; event - the Timed implementation to execute.
//   output: TimedEvent with period=None and fused=true.
//   sideEffects: none
    pub fn once(when: Instant, event: impl Timed + Send + Sync + 'static) -> TimedEvent {
        TimedEvent {
            when,
            period: None,
            future: Arc::new(event),
            fused: Arc::new(AtomicBool::new(true)),
        }
    }
    // once:end

    // periodic:start
//   purpose: Create a repeating TimedEvent that fires at fixed intervals starting after the first interval.
//   input:  interval - Duration between repeated firings; event - the Timed implementation to execute.
//   output: TimedEvent with period=Some(interval), when=now+interval, and fused=true.
//   sideEffects: none
    pub fn periodic(interval: Duration, event: impl Timed + Send + Sync + 'static) -> TimedEvent {
        TimedEvent {
            when: Instant::now() + interval,
            period: Some(interval),
            future: Arc::new(event),
            fused: Arc::new(AtomicBool::new(true)),
        }
    }
    // periodic:end

    // is_fused:start
//   purpose: Check whether the event's fuse is still active (not defused).
//   input:  &self.
//   output: bool - true if the event is still armed and will fire.
//   sideEffects: none (Acquire atomic load)
    pub fn is_fused(&self) -> bool {
        self.fused.load(AtomicOrdering::Acquire)
    }
    // is_fused:end

    // get_handle:start
//   purpose: Get a TimedHandle that can defuse this event from another location.
//   input:  &self.
//   output: TimedHandle wrapping a Weak reference to the fused AtomicBool.
//   sideEffects: none (creates Weak from Arc)
    pub fn get_handle(&self) -> TimedHandle {
        TimedHandle(Arc::downgrade(&self.fused))
    }
    // get_handle:end
}

impl Eq for TimedEvent {}

impl Ord for TimedEvent {
    // cmp:start
//   purpose: Compare TimedEvents by their when field, reversed to convert BinaryHeap into a min-heap.
//   input:  self, other - TimedEvents to compare.
//   output: ComparisonOrdering - reversed comparison (other.when vs self.when) so earliest events are at heap top.
//   sideEffects: none
    fn cmp(&self, other: &Self) -> ComparisonOrdering {
        // The usual cmp is defined as: self.when.cmp(&other.when)
        // This would make the events ordered from largest to the smallest in the heap.
        // However, we want the events to be ordered from the smallest to the largest.
        // As a consequence of this, we swap the comparison terms, converting the heap
        // from a max-heap into a min-heap.
        other.when.cmp(&self.when)
    }
    // cmp:end
}

impl PartialOrd for TimedEvent {
    // partial_cmp:start
//   purpose: Delegate to cmp for PartialOrd implementation.
//   input:  self, other - TimedEvents.
//   output: Some(ComparisonOrdering) from self.cmp(other).
//   sideEffects: none
    fn partial_cmp(&self, other: &Self) -> Option<ComparisonOrdering> {
        Some(self.cmp(other))
    }
    // partial_cmp:end
}

impl PartialEq for TimedEvent {
    // eq:start
//   purpose: Compare TimedEvents for equality based on their when field.
//   input:  self, other - TimedEvents.
//   output: bool - true if when fields are equal.
//   sideEffects: none
    fn eq(&self, other: &Self) -> bool {
        self.when == other.when
    }
    // eq:end
}

// timer_task:start
//   purpose: Background loop that waits for events to become due, executes them, and re-queues periodic events.
//   input:  events - shared BinaryHeap of scheduled events; new_event - channel receiver for new/submitted events.
//   output: Result<(), RecvError> - Ok when channel closes normally; Err on channel error.
//   sideEffects: acquires mutex lock, spawns tokio sleep, executes Timed::run on due events
async fn timer_task(
    events: Arc<Mutex<BinaryHeap<TimedEvent>>>,
    new_event: Receiver<(bool, TimedEvent)>,
) -> Result<(), RecvError> {
    // Error message
    let e = "Timer has been dropped. Unable to run timed events.";

    // Acquire the lock
    let mut events = events.lock().await;

    loop {
        // Future for adding new events
        let new = new_event.recv_async();

        match events.peek() {
            Some(next) => {
                // Future for waiting an event timing
                let wait = async {
                    let next = next.clone();
                    let now = Instant::now();
                    if next.when > now {
                        time::sleep(next.when - now).await;
                    }
                    Ok((false, next))
                };

                let result = select! {
                    result = wait => { result },
                    result = new => { result },
                };

                match result {
                    Ok((is_new, mut ev)) => {
                        if is_new {
                            // A new event has just been added: push it onto the heap
                            events.push(ev);
                            continue;
                        }

                        // We are ready to serve the event, remove it from the heap
                        let _ = events.pop();

                        // Execute the future if the event is fused
                        if ev.is_fused() {
                            // Now there is only one Arc pointing to the event future
                            // It is safe to access and execute to the inner future as mutable
                            if let Some(fut) = Arc::get_mut(&mut ev.future) {
                                fut.run().await;
                            } else {
                                tracing::warn!("[timer] Arc not unique, skipping run");
                            }

                            // Check if the event is periodic
                            if let Some(interval) = ev.period {
                                ev.when = Instant::now() + interval;
                                events.push(ev);
                            }
                        }
                    }
                    Err(_) => {
                        // Channel error
                        tracing::trace!("{}", e);
                        return Ok(());
                    }
                }
            }
            None => match new.await {
                Ok((_, ev)) => {
                    events.push(ev);
                    continue;
                }
                Err(_) => {
                    // Channel error
                    tracing::trace!("{}", e);
                    return Ok(());
                }
            },
        }
    }
}
// timer_task:end

#[derive(Clone)]
pub struct Timer {
    events: Arc<Mutex<BinaryHeap<TimedEvent>>>,
    sl_sender: Option<Sender<()>>,
    ev_sender: Option<Sender<(bool, TimedEvent)>>,
}

impl Timer {
    // new:start
//   purpose: Create a new Timer and spawn its background timer_task.
//   input:  spawn_blocking - if true, spawn the task on a blocking thread via spawn_blocking.
//   output: Timer with event and stop channels, and a running background task.
//   sideEffects: creates flume channels, spawns tokio task (or spawn_blocking)
    pub fn new(spawn_blocking: bool) -> Timer {
        // Create the channels
        let (ev_sender, ev_receiver) = bounded::<(bool, TimedEvent)>(*TIMER_EVENTS_CHANNEL_SIZE);
        let (sl_sender, sl_receiver) = bounded::<()>(1);

        // Create the timer object
        let timer = Timer {
            events: Arc::new(Mutex::new(BinaryHeap::new())),
            sl_sender: Some(sl_sender),
            ev_sender: Some(ev_sender),
        };

        // Start the timer task
        let c_e = timer.events.clone();
        let fut = async move {
            select! {
                _ = sl_receiver.recv_async() => {},
                _ = timer_task(c_e, ev_receiver) => {},
            };
            tracing::trace!("A - Timer task no longer running...");
        };
        if spawn_blocking {
            task::spawn_blocking(|| Handle::current().block_on(fut));
        } else {
            task::spawn(fut);
        }

        // Return the timer object
        timer
    }
    // new:end

    // start:start
//   purpose: Start or restart the timer's background task after a stop; no-op if already running.
//   input:  &mut self; spawn_blocking - if true, spawn the task on a blocking thread.
//   output: ()
//   sideEffects: creates flume channels if stopped; spawns tokio task
    pub fn start(&mut self, spawn_blocking: bool) {
        if self.sl_sender.is_none() {
            // Create the channels
            let (ev_sender, ev_receiver) =
                bounded::<(bool, TimedEvent)>(*TIMER_EVENTS_CHANNEL_SIZE);
            let (sl_sender, sl_receiver) = bounded::<()>(1);

            // Store the channels handlers
            self.sl_sender = Some(sl_sender);
            self.ev_sender = Some(ev_sender);

            // Start the timer task
            let c_e = self.events.clone();
            let fut = async move {
                select! {
                    _ = sl_receiver.recv_async() => {},
                    _ = timer_task(c_e, ev_receiver) => {},
                };
                tracing::trace!("A - Timer task no longer running...");
            };
            if spawn_blocking {
                task::spawn_blocking(|| Handle::current().block_on(fut));
            } else {
                task::spawn(fut);
            }
        }
    }
    // start:end

    #[inline]
    // start_async:start
//   purpose: Async wrapper around start for use from async contexts.
//   input:  &mut self; spawn_blocking - if true, spawn the task on a blocking thread.
//   output: ()
//   sideEffects: same as start (creates channels, spawns task)
    pub async fn start_async(&mut self, spawn_blocking: bool) {
        self.start(spawn_blocking)
    }
    // start_async:end

    // stop:start
//   purpose: Stop the timer's background task by sending a stop signal and clearing channels.
//   input:  &mut self.
//   output: ()
//   sideEffects: sends () on stop channel, drops channel senders (causing timer_task to exit)
    pub fn stop(&mut self) {
        if let Some(sl_sender) = &self.sl_sender {
            // Stop the timer task
            let _ = sl_sender.send(());

            tracing::trace!("Stopping timer...");
            // Remove the channels handlers
            self.sl_sender = None;
            self.ev_sender = None;
        }
    }
    // stop:end

    // stop_async:start
//   purpose: Async version of stop that sends the stop signal asynchronously.
//   input:  &mut self.
//   output: ()
//   sideEffects: same as stop, uses send_async on stop channel
    pub async fn stop_async(&mut self) {
        if let Some(sl_sender) = &self.sl_sender {
            // Stop the timer task
            let _ = sl_sender.send_async(()).await;

            tracing::trace!("Stopping timer...");
            // Remove the channels handlers
            self.sl_sender = None;
            self.ev_sender = None;
        }
    }
    // stop_async:end

    // add:start
//   purpose: Submit a TimedEvent to the timer for scheduling.
//   input:  &self; event - the TimedEvent to schedule.
//   output: ()
//   sideEffects: sends (true, event) on the event channel; no-op if timer is stopped
    pub fn add(&self, event: TimedEvent) {
        if let Some(ev_sender) = &self.ev_sender {
            let _ = ev_sender.send((true, event));
        }
    }
    // add:end

    // add_async:start
//   purpose: Async version of add that sends the event asynchronously.
//   input:  &self; event - the TimedEvent to schedule.
//   output: ()
//   sideEffects: sends (true, event) on the event channel via send_async
    pub async fn add_async(&self, event: TimedEvent) {
        if let Some(ev_sender) = &self.ev_sender {
            let _ = ev_sender.send_async((true, event)).await;
        }
    }
    // add_async:end
}

impl Default for Timer {
    // default:start
//   purpose: Create a default Timer with spawn_blocking=false.
//   input:  none.
//   output: Timer.
//   sideEffects: same as Timer::new(false)
    fn default() -> Self {
        Self::new(false)
    }
    // default:end
}

mod tests {
    #[test]
    // timer:start
//   purpose: Test one-shot, periodic, defuse, and stop/start timer functionality.
//   input:  none (test function).
//   output: passes or panics on assertion failure.
//   sideEffects: creates tokio runtime; spawns async tasks
    fn timer() {
        use std::{
            sync::{
                atomic::{AtomicUsize, Ordering},
                Arc,
            },
            time::{Duration, Instant},
        };

        use async_trait::async_trait;
        use tokio::{runtime::Runtime, time};

        use super::{Timed, TimedEvent, Timer};

        #[derive(Clone)]
        struct MyEvent {
            counter: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl Timed for MyEvent {
            // run:start
//   purpose: Increment the shared atomic counter by 1 (test event action).
//   input:  &mut self.
//   output: ()
//   sideEffects: atomically increments counter (SeqCst ordering)
            async fn run(&mut self) {
                self.counter.fetch_add(1, Ordering::SeqCst);
            }
            // run:end
        }

        // run:start
//   purpose: Execute the timer test scenarios: once, defuse, periodic, stop/start.
//   input:  none (test helper).
//   output: ()
//   sideEffects: creates Timer, adds events, uses time::sleep
        async fn run() {
            // Create the timer
            let mut timer = Timer::new(false);

            // Counter for testing
            let counter = Arc::new(AtomicUsize::new(0));

            // Create my custom event
            let myev = MyEvent {
                counter: counter.clone(),
            };

            // Default testing interval: 1 s
            let interval = Duration::from_secs(1);

            /* [1] */
            println!("Timer [1]: Once event and run");
            // Fire a once timed event
            let now = Instant::now();
            let event = TimedEvent::once(now + (2 * interval), myev.clone());

            // Add the event to the timer
            timer.add_async(event).await;

            // Wait for the event to occur
            time::sleep(3 * interval).await;

            // Load and reset the counter value
            let value = counter.swap(0, Ordering::SeqCst);
            assert_eq!(value, 1);

            /* [2] */
            println!("Timer [2]: Once event and defuse");
            // Fire a once timed event and defuse it before it is executed
            let now = Instant::now();
            let event = TimedEvent::once(now + (2 * interval), myev.clone());
            let handle = event.get_handle();

            // Add the event to the timer
            timer.add_async(event).await;
            //
            handle.defuse();

            // Wait for the event to occur
            time::sleep(3 * interval).await;

            // Load and reset the counter value
            let value = counter.swap(0, Ordering::SeqCst);
            assert_eq!(value, 0);

            /* [3] */
            println!("Timer [3]: Periodic event run and defuse");
            // Number of events to occur
            let amount: usize = 3;

            // Half the waiting interval for granularity reasons
            let to_elapse = (2 * amount as u32) * interval;

            // Fire a periodic event
            let event = TimedEvent::periodic(2 * interval, myev.clone());
            let handle = event.get_handle();

            // Add the event to the timer
            timer.add_async(event).await;

            // Wait for the events to occur
            time::sleep(to_elapse + interval).await;

            // Load and reset the counter value
            let value = counter.swap(0, Ordering::SeqCst);
            assert_eq!(value, amount);

            // Defuse the event (check if twice defusing don't cause troubles)
            handle.clone().defuse();
            handle.defuse();

            // Wait a bit more to verify that not more events have been fired
            time::sleep(to_elapse).await;

            // Load and reset the counter value
            let value = counter.swap(0, Ordering::SeqCst);
            assert_eq!(value, 0);

            /* [4] */
            println!("Timer [4]: Periodic event and stop/start timer");
            // Fire a periodic event
            let event = TimedEvent::periodic(2 * interval, myev);

            // Add the event to the timer
            timer.add_async(event).await;

            // Wait for the events to occur
            time::sleep(to_elapse + interval).await;

            // Load and reset the counter value
            let value = counter.swap(0, Ordering::SeqCst);
            assert_eq!(value, amount);

            // Stop the timer
            timer.stop_async().await;

            // Wait some time
            time::sleep(to_elapse).await;

            // Load and reset the counter value
            let value = counter.swap(0, Ordering::SeqCst);
            assert_eq!(value, 0);

            // Restart the timer
            timer.start_async(false).await;

            // Wait for the events to occur
            time::sleep(to_elapse).await;

            // Load and reset the counter value
            let value = counter.swap(0, Ordering::SeqCst);
            assert_eq!(value, amount);
        }
        // run:end

        let rt = Runtime::new().unwrap();
        rt.block_on(run());
    }
    // timer:end
}
