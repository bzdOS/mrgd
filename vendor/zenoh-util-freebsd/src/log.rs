// START_AI_HEADER
// MODULE: zenoh-util-freebsd/src/log.rs
// PURPOSE: Tracing log initialization helpers and a custom Layer for structured log callbacks.
// INTENT: Provides convenience functions to initialize tracing_subscriber from RUST_LOG env, and a Layer that captures log records with span context into a callback.
// DEPENDENCIES: std (fmt, thread), tracing, tracing_subscriber
// PUBLIC_API: try_init_log_from_env, init_log_from_env_or, init_log_with_callback, init_log_test, LogRecord
// END_AI_HEADER

//
// Copyright (c) 2024 ZettaScale Technology
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
use std::{fmt, thread, thread::ThreadId};

use tracing::{field::Field, span, Event, Metadata, Subscriber};
use tracing_subscriber::{
    layer::{Context, SubscriberExt},
    registry::LookupSpan,
    EnvFilter,
};

/// A utility function to enable the tracing formatting subscriber.
///
/// The [`tracing_subscriber`]` is initialized from the `RUST_LOG` environment variable.
/// If `RUST_LOG` is not set, then logging is not enabled.
///
/// # Safety
///
/// Calling this function initializes a `lazy_static` in the [`tracing`] crate.
/// Such static is not deallocated prior to process exiting, thus tools such as `valgrind`
/// will report a memory leak.
/// Refer to this issue: <https://github.com/tokio-rs/tracing/issues/2069>
// try_init_log_from_env:start
//   purpose: Initialize tracing subscriber from RUST_LOG env var if set; no-op if unset.
//   input:  none.
//   output: ()
//   sideEffects: sets global tracing subscriber if RUST_LOG is set; allocates lazy_static in tracing crate (valgrind leak)
pub fn try_init_log_from_env() {
    if let Ok(env_filter) = EnvFilter::try_from_default_env() {
        init_env_filter(env_filter);
    }
}
// try_init_log_from_env:end

/// A utility function to enable the tracing formatting subscriber.
///
/// The [`tracing_subscriber`] is initialized from the `RUST_LOG` environment variable.
/// If `RUST_LOG` is not set, then fallback directives are used.
///
/// # Safety
/// Calling this function initializes a `lazy_static` in the [`tracing`] crate.
/// Such static is not deallocated prior to process existing, thus tools such as `valgrind`
/// will report a memory leak.
/// Refer to this issue: <https://github.com/tokio-rs/tracing/issues/2069>
pub fn init_log_from_env_or<S>(fallback: S)
where
    S: AsRef<str>,
{
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(fallback));
    init_env_filter(env_filter);
}

// init_env_filter:start
//   purpose: Initialize tracing subscriber with a given EnvFilter and formatted output.
//   input:  env_filter - the EnvFilter to control log level per target.
//   output: ()
//   sideEffects: sets global tracing subscriber; allocates lazy_static (valgrind leak)
fn init_env_filter(env_filter: EnvFilter) {
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_level(true)
        .with_target(true);

    let subscriber = subscriber.finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
}
// init_env_filter:end

pub struct LogRecord {
    pub target: String,
    pub level: tracing::Level,
    pub file: Option<&'static str>,
    pub line: Option<u32>,
    pub thread_id: ThreadId,
    pub thread_name: Option<String>,
    pub message: Option<String>,
    pub attributes: Vec<(&'static str, String)>,
}

#[derive(Clone)]
struct SpanFields(Vec<(&'static str, String)>);

struct Layer<Enabled, Callback> {
    enabled: Enabled,
    callback: Callback,
}

impl<S, E, C> tracing_subscriber::Layer<S> for Layer<E, C>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    E: Fn(&Metadata) -> bool + 'static,
    C: Fn(LogRecord) + 'static,
{
    // enabled:start
//   purpose: Filter whether a given metadata/event should be processed by this layer.
//   input:  self; metadata - event metadata; _ctx - span context (unused).
//   output: bool - true if the event passes the user-supplied enabled filter.
//   sideEffects: none
    fn enabled(&self, metadata: &Metadata<'_>, _: Context<'_, S>) -> bool {
        (self.enabled)(metadata)
    }
    // enabled:end

    // on_new_span:start
//   purpose: Record span fields when a new span is created and store them in span extensions.
//   input:  self; attrs - span attributes; id - span id; ctx - span context.
//   output: ()
//   sideEffects: mutates span extensions to store SpanFields
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).unwrap();
        let mut extensions = span.extensions_mut();
        let mut fields = vec![];
        attrs.record(&mut |field: &Field, value: &dyn fmt::Debug| {
            fields.push((field.name(), format!("{value:?}")))
        });
        extensions.insert(SpanFields(fields));
    }
    // on_new_span:end

    // on_record:start
//   purpose: Append additional fields to an existing span when values are recorded after creation.
//   input:  self; id - span id; values - new field values; ctx - span context.
//   output: ()
//   sideEffects: mutates span extensions to append to SpanFields
    fn on_record(&self, id: &span::Id, values: &span::Record<'_>, ctx: Context<'_, S>) {
        let span = ctx.span(id).unwrap();
        let mut extensions = span.extensions_mut();
        let fields = extensions.get_mut::<SpanFields>().unwrap();
        values.record(&mut |field: &Field, value: &dyn fmt::Debug| {
            fields.0.push((field.name(), format!("{value:?}")))
        });
    }
    // on_record:end

    // on_event:start
//   purpose: Capture a tracing event into a LogRecord with span context and invoke the user callback.
//   input:  self; event - the tracing Event; ctx - span context for scope enrichment.
//   output: ()
//   sideEffects: invokes the user-supplied callback with the constructed LogRecord
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let thread = thread::current();
        let mut record = LogRecord {
            target: event.metadata().target().into(),
            level: *event.metadata().level(),
            file: event.metadata().file(),
            line: event.metadata().line(),
            thread_id: thread.id(),
            thread_name: thread.name().map(Into::into),
            message: None,
            attributes: vec![],
        };
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope.from_root() {
                let extensions = span.extensions();
                let fields = extensions.get::<SpanFields>().unwrap();
                record.attributes.extend(fields.0.iter().cloned());
            }
        }
        event.record(&mut |field: &Field, value: &dyn fmt::Debug| {
            if field.name() == "message" {
                record.message = Some(format!("{value:?}"));
            } else {
                record.attributes.push((field.name(), format!("{value:?}")))
            }
        });
        (self.callback)(record);
    }
    // on_event:end
}

// init_log_with_callback:start
//   purpose: Initialize tracing with a custom Layer that filters and captures events into a user-supplied callback.
//   input:  enabled - filter fn determining which metadata to process; callback - fn receiving LogRecord for each event.
//   output: ()
//   sideEffects: sets global tracing subscriber with the custom layer
pub fn init_log_with_callback(
    enabled: impl Fn(&Metadata) -> bool + Send + Sync + 'static,
    callback: impl Fn(LogRecord) + Send + Sync + 'static,
) {
    let subscriber = tracing_subscriber::registry().with(Layer { enabled, callback });
    let _ = tracing::subscriber::set_global_default(subscriber);
}
// init_log_with_callback:end

#[cfg(feature = "test")]
// Used to verify memory leaks for valgrind CI.
// `EnvFilter` internally uses a static reference that is not cleaned up yielding to false positive in valgrind.
// This function enables logging without calling `EnvFilter` for env configuration.
// init_log_test:start
//   purpose: Initialize tracing at INFO level for testing, without EnvFilter (avoids valgrind false positives).
//   input:  none.
//   output: ()
//   sideEffects: sets global tracing subscriber with max_level INFO, thread ids/names disabled to avoid memory leaks
pub fn init_log_test() {
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_thread_ids(false) // Disable thread ids and name because
        .with_thread_names(false) // there is a memory leak in tracing_subscriber crate with these enabled.
        .with_level(true)
        .with_target(true);

    let subscriber = subscriber.finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
}
// init_log_test:end
