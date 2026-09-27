// START_AI_HEADER
// MODULE: zenoh-util-freebsd/src/time_range.rs
// PURPOSE: Provides parsing, representation, display, and resolution of Zenoh Time DSL expressions and ranges.
// INTENT: Encapsulates the Zenoh Time DSL syntax (instant, offset, range, duration) into typed Rust structs with safe conversion and resolution logic.
// DEPENDENCIES: std (time, convert, fmt, ops, str), humantime, zenoh_result
// PUBLIC_API: TimeRange<T>, TimeBound<T>, TimeExpr, parse_duration
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
    convert::{TryFrom, TryInto},
    fmt::Display,
    ops::Add,
    str::FromStr,
    time::{Duration, SystemTime},
};

use humantime::{format_rfc3339, parse_rfc3339_weak};
use zenoh_result::{bail, zerror, ZError};

const U_TO_SECS: f64 = 0.000001;
const MS_TO_SECS: f64 = 0.001;
const M_TO_SECS: f64 = 60.0;
const H_TO_SECS: f64 = M_TO_SECS * 60.0;
const D_TO_SECS: f64 = H_TO_SECS * 24.0;
const W_TO_SECS: f64 = D_TO_SECS * 7.0;

/// The structural representation of the Zenoh Time DSL, which may adopt one of two syntax:
/// - the "range" syntax: `<ldel: '[' | ']'><start: TimeExpr?>..<end: TimeExpr?><rdel: '[' | ']'>`
/// - the "duration" syntax: `<ldel: '[' | ']'><start: TimeExpr>;<duration: Duration><rdel: '[' | ']'>`, which is
///   equivalent to `<ldel><start>..<start+duration><rdel>`
///
/// Durations follow the `<duration: float><unit: "u", "ms", "s", "m", "h", "d", "w">` syntax.
///
/// Where [`TimeExpr`] itself may adopt one of two syntaxes:
/// - the "instant" syntax, which must be a UTC [RFC3339](https://datatracker.ietf.org/doc/html/rfc3339) formatted timestamp.
/// - the "offset" syntax, which is written `now(<sign: '-'?><offset: Duration?>)`, and allows to specify a target instant as
///   an offset applied to an instant of evaluation. These offset are resolved at the evaluation site.
///
/// In range syntax, omitting `<start>` and/or `<end>` implies that the range is unbounded in that direction.
///
/// Exclusive bounds are represented by their respective delimiters pointing towards the exterior.
/// Interior bounds are represented by the opposite.
///
/// The comparison step for instants is the nanosecond, which makes exclusive and inclusive bounds extremely close.
/// The `[<start>..<end>[` pattern may however be useful to guarantee that a same timestamp never appears twice when
/// iteratively getting values for `[t0..t1[`, `[t1..t2[`, `[t2..t3[`...
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct TimeRange<T = TimeExpr> {
    pub start: TimeBound<T>,
    pub end: TimeBound<T>,
}

impl TimeRange<TimeExpr> {
    /// Resolves the offset bounds in the range using `now` as reference.
    // resolve_at:start
//   purpose: Resolve offset-relative bounds using a given reference SystemTime.
//   input:  self - time range with possibly-unresolved TimeExpr bounds; now - reference SystemTime for offset resolution.
//   output: TimeRange<SystemTime> with all offset bounds resolved to absolute instants.
//   sideEffects: none
    pub fn resolve_at(self, now: SystemTime) -> TimeRange<SystemTime> {
        TimeRange {
            start: self.start.resolve_at(now),
            end: self.end.resolve_at(now),
        }
    }
    // resolve_at:end

    /// Resolves the offset bounds in the range using [`SystemTime::now`] as reference.
    // resolve:start
//   purpose: Resolve offset-relative bounds using the current system time as reference.
//   input:  self - time range with possibly-unresolved TimeExpr bounds.
//   output: TimeRange<SystemTime> with all offset bounds resolved to absolute instants.
//   sideEffects: reads system clock via SystemTime::now
    pub fn resolve(self) -> TimeRange<SystemTime> {
        self.resolve_at(SystemTime::now())
    }
    // resolve:end

    /// Returns `true` if the provided `instant` belongs to `self`.
    ///
    /// This method performs resolution with [`SystemTime::now`] if the bounds contain an "offset" time expression.
    /// If you intend on performing this check multiple times, it may be wiser to resolve `self` first, and use
    /// [`TimeRange::<SystemTime>::contains`] instead.
    // contains:start
//   purpose: Check if an instant falls within the range, resolving offsets against the current time.
//   input:  self - time range with possibly-unresolved bounds; instant - the SystemTime to check.
//   output: bool - true if instant is within the bounds of the range.
//   sideEffects: reads system clock via SystemTime::now
    pub fn contains(&self, instant: SystemTime) -> bool {
        let now = SystemTime::now();
        match &self.start.resolve_at(now) {
            TimeBound::Inclusive(t) if t > &instant => return false,
            TimeBound::Exclusive(t) if t >= &instant => return false,
            _ => {}
        }
        match &self.end.resolve_at(now) {
            TimeBound::Inclusive(t) => t >= &instant,
            TimeBound::Exclusive(t) => t > &instant,
            _ => true,
        }
    }
    // contains:end
}

impl TimeRange<SystemTime> {
    /// Returns `true` if the provided `instant` belongs to `self`.
    // contains:start
//   purpose: Check if an instant falls within a pre-resolved time range.
//   input:  self - resolved TimeRange<SystemTime>; instant - the SystemTime to check.
//   output: bool - true if instant is within the bounds of the range.
//   sideEffects: none
    pub fn contains(&self, instant: SystemTime) -> bool {
        match &self.start {
            TimeBound::Inclusive(t) if *t > instant => return false,
            TimeBound::Exclusive(t) if *t >= instant => return false,
            _ => {}
        }
        match &self.end {
            TimeBound::Inclusive(t) => *t >= instant,
            TimeBound::Exclusive(t) => *t > instant,
            _ => true,
        }
    }
    // contains:end
}

impl From<TimeRange<SystemTime>> for TimeRange<TimeExpr> {
    // from:start
//   purpose: Convert a resolved SystemTime range into a TimeExpr range by wrapping each bound as Fixed.
//   input:  value - TimeRange<SystemTime> to convert.
//   output: TimeRange<TimeExpr> with each bound converted via From<SystemTime>.
//   sideEffects: none
    fn from(value: TimeRange<SystemTime>) -> Self {
        TimeRange {
            start: value.start.into(),
            end: value.end.into(),
        }
    }
    // from:end
}

impl TryFrom<TimeRange<TimeExpr>> for TimeRange<SystemTime> {
    type Error = ();
    // try_from:start
//   purpose: Try to convert a TimeExpr range into a SystemTime range, failing if any bound contains a Now expression.
//   input:  value - TimeRange<TimeExpr> to convert.
//   output: Result<TimeRange<SystemTime>, ()> - Err if any bound cannot be resolved to a fixed instant.
//   sideEffects: none
    fn try_from(value: TimeRange<TimeExpr>) -> Result<Self, Self::Error> {
        Ok(TimeRange {
            start: value.start.try_into()?,
            end: value.end.try_into()?,
        })
    }
    // try_from:end
}

impl Display for TimeRange<TimeExpr> {
    // fmt:start
//   purpose: Format a TimeExpr range as a string using Zenoh Time DSL syntax.
//   input:  self - the TimeRange; f - Formatter to write into.
//   output: std::fmt::Result indicating success or formatting error.
//   sideEffects: none
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.start {
            TimeBound::Inclusive(t) => write!(f, "[{t}..")?,
            TimeBound::Exclusive(t) => write!(f, "]{t}..")?,
            TimeBound::Unbounded => f.write_str("[..")?,
        }
        match &self.end {
            TimeBound::Inclusive(t) => write!(f, "{t}]"),
            TimeBound::Exclusive(t) => write!(f, "{t}["),
            TimeBound::Unbounded => f.write_str("]"),
        }
    }
    // fmt:end
}

impl Display for TimeRange<SystemTime> {
    // fmt:start
//   purpose: Format a resolved SystemTime range as a string using Zenoh Time DSL syntax with RFC3339 timestamps.
//   input:  self - the TimeRange<SystemTime>; f - Formatter to write into.
//   output: std::fmt::Result indicating success or formatting error.
//   sideEffects: none
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.start {
            TimeBound::Inclusive(t) => write!(f, "[{}..", TimeExpr::Fixed(*t))?,
            TimeBound::Exclusive(t) => write!(f, "]{}..", TimeExpr::Fixed(*t))?,
            TimeBound::Unbounded => f.write_str("[..")?,
        }
        match &self.end {
            TimeBound::Inclusive(t) => write!(f, "{}]", TimeExpr::Fixed(*t)),
            TimeBound::Exclusive(t) => write!(f, "{}[", TimeExpr::Fixed(*t)),
            TimeBound::Unbounded => f.write_str("]"),
        }
    }
    // fmt:end
}

impl FromStr for TimeRange<TimeExpr> {
    type Err = ZError;
    // from_str:start
//   purpose: Parse a string into a TimeRange supporting range syntax (<ldel><start>..<end><rdel>) and duration syntax (<ldel><start>;<duration><rdel>).
//   input:  s - string slice in Zenoh Time DSL format.
//   output: Result<TimeRange<TimeExpr>, ZError> - Err on malformed input.
//   sideEffects: none
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // minimum str size is 4: "[..]"
        let len = s.len();
        if len < 4 {
            bail!("Invalid TimeRange: {}", s);
        }

        let mut chars = s.chars();
        let inclusive_start = match chars.next() {
            Some('[') => true,
            Some(']') => false,
            _ => bail!("Invalid TimeRange (must start with '[' or ']'): {}", s),
        };
        let inclusive_end = match chars.last() {
            Some(']') => true,
            Some('[') => false,
            _ => bail!("Invalid TimeRange (must end with '[' or ']'): {}", s),
        };

        let s = &s[1..len - 1];
        if let Some((start, end)) = s.split_once("..") {
            Ok(TimeRange {
                start: parse_time_bound(start, inclusive_start)?,
                end: parse_time_bound(end, inclusive_end)?,
            })
        } else if let Some((start, duration)) = s.split_once(';') {
            let start_bound = parse_time_bound(start, inclusive_start)?;
            let duration = parse_duration(duration)?;
            let end_bound = match &start_bound {
                TimeBound::Inclusive(time) | TimeBound::Exclusive(time) => {
                    if inclusive_end {
                        TimeBound::Inclusive(time + duration)
                    } else {
                        TimeBound::Exclusive(time + duration)
                    }
                }
                TimeBound::Unbounded => bail!(
                    r#"Invalid TimeRange (';' must contain a time and a duration)"): {}"#,
                    s
                ),
            };
            Ok(TimeRange {
                start: start_bound,
                end: end_bound,
            })
        } else {
            bail!(
                r#"Invalid TimeRange (must contain ".." or ";" as separator)"): {}"#,
                s
            )
        }
    }
    // from_str:end
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TimeBound<T> {
    Inclusive(T),
    Exclusive(T),
    Unbounded,
}

impl From<TimeBound<SystemTime>> for TimeBound<TimeExpr> {
    // from:start
//   purpose: Convert a resolved SystemTime bound into a TimeExpr bound by wrapping each instant as Fixed.
//   input:  value - TimeBound<SystemTime> to convert.
//   output: TimeBound<TimeExpr> preserving inclusiveness/exclusiveness/unboundedness.
//   sideEffects: none
    fn from(value: TimeBound<SystemTime>) -> Self {
        match value {
            TimeBound::Inclusive(t) => TimeBound::Inclusive(t.into()),
            TimeBound::Exclusive(t) => TimeBound::Exclusive(t.into()),
            TimeBound::Unbounded => TimeBound::Unbounded,
        }
    }
    // from:end
}

impl TryFrom<TimeBound<TimeExpr>> for TimeBound<SystemTime> {
    type Error = ();
    // try_from:start
//   purpose: Try to convert a TimeExpr bound into a SystemTime bound, failing if the inner expression is a Now variant.
//   input:  value - TimeBound<TimeExpr> to convert.
//   output: Result<TimeBound<SystemTime>, ()> - Err if the bound contains a non-fixed TimeExpr.
//   sideEffects: none
    fn try_from(value: TimeBound<TimeExpr>) -> Result<Self, Self::Error> {
        Ok(match value {
            TimeBound::Inclusive(t) => TimeBound::Inclusive(t.try_into()?),
            TimeBound::Exclusive(t) => TimeBound::Exclusive(t.try_into()?),
            TimeBound::Unbounded => TimeBound::Unbounded,
        })
    }
    // try_from:end
}

impl TimeBound<TimeExpr> {
    /// Resolves `self` into a [`TimeBound<SystemTime>`], using `now` as a reference for offset expressions.
    /// If `self` is a time boundary that cannot be represented as `SystemTime` (which means it's not inside
    /// the bounds of the underlying data structure), then `TimeBound::Unbounded` is returned.
    // resolve_at:start
//   purpose: Resolve a TimeExpr bound to a SystemTime bound using a reference time, returning Unbounded on overflow.
//   input:  self - the TimeBound to resolve; now - reference SystemTime for offset resolution.
//   output: TimeBound<SystemTime> - Unbounded if the inner TimeExpr cannot be represented as SystemTime.
//   sideEffects: none
    pub fn resolve_at(self, now: SystemTime) -> TimeBound<SystemTime> {
        match self {
            TimeBound::Inclusive(t) => match t.checked_resolve_at(now) {
                Some(ts) => TimeBound::Inclusive(ts),
                None => TimeBound::Unbounded,
            },
            TimeBound::Exclusive(t) => match t.checked_resolve_at(now) {
                Some(ts) => TimeBound::Exclusive(ts),
                None => TimeBound::Unbounded,
            },
            TimeBound::Unbounded => TimeBound::Unbounded,
        }
    }
    // resolve_at:end
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum TimeExpr {
    Fixed(SystemTime),
    Now { offset_secs: f64 },
}

impl From<SystemTime> for TimeExpr {
    // from:start
//   purpose: Wrap a SystemTime into a Fixed TimeExpr variant.
//   input:  t - SystemTime to wrap.
//   output: TimeExpr::Fixed(t).
//   sideEffects: none
    fn from(t: SystemTime) -> Self {
        Self::Fixed(t)
    }
    // from:end
}

impl TryFrom<TimeExpr> for SystemTime {
    type Error = ();
    // try_from:start
//   purpose: Unwrap a Fixed TimeExpr into SystemTime, failing for Now variants.
//   input:  value - TimeExpr to unwrap.
//   output: Result<SystemTime, ()> - Err if the expression is a Now variant.
//   sideEffects: none
    fn try_from(value: TimeExpr) -> Result<Self, Self::Error> {
        match value {
            TimeExpr::Fixed(t) => Ok(t),
            TimeExpr::Now { .. } => Err(()),
        }
    }
    // try_from:end
}

impl TimeExpr {
    /// Resolves `self` into a [`SystemTime`], using `now` as a reference for offset expressions.
    ///
    ///# Panics
    ///
    /// This function may panic if the resulting point in time cannot be represented by the
    /// underlying data structure. See [`TimeExpr::checked_resolve_at`] for a version without panic.
    // resolve_at:start
//   purpose: Resolve a TimeExpr to SystemTime, panicking on overflow for Now offsets.
//   input:  self - the TimeExpr to resolve; now - reference SystemTime for offset computation.
//   output: SystemTime - panics if the offset causes overflow.
//   sideEffects: none (panics on overflow)
    pub fn resolve_at(&self, now: SystemTime) -> SystemTime {
        self.checked_resolve_at(now).unwrap()
    }
    // resolve_at:end

    /// Resolves `self` into a [`SystemTime`], using `now` as a reference for offset expressions.
    /// If `self` is a `TimeExpr::Now{offset_secs}` and adding `offset_secs` to `now` results in a time
    /// that would be outside the bounds of the underlying data structure, `None` is returned.
    // checked_resolve_at:start
//   purpose: Resolve a TimeExpr to SystemTime, returning None on arithmetic overflow.
//   input:  self - the TimeExpr to resolve; now - reference SystemTime for offset computation.
//   output: Option<SystemTime> - None if the offset causes the instant to fall outside representable range.
//   sideEffects: none
    pub fn checked_resolve_at(&self, now: SystemTime) -> Option<SystemTime> {
        match self {
            TimeExpr::Fixed(t) => Some(*t),
            TimeExpr::Now { offset_secs } => checked_duration_add(now, *offset_secs),
        }
    }
    // checked_resolve_at:end
    /// Adds `duration` to `self`, returning `None` if `self` is a `Fixed(SystemTime)` and adding the duration is not possible
    /// because the result would be outside the bounds of the underlying data structure (see [`SystemTime::checked_add`]).
    /// Otherwise returns `Some(time_expr)`.
    // checked_add:start
//   purpose: Add a duration in seconds to a TimeExpr, returning None on overflow for Fixed variants.
//   input:  self - the TimeExpr; duration - f64 seconds to add (may be negative).
//   output: Option<TimeExpr> - None if the resulting Fixed instant is outside representable range.
//   sideEffects: none
    pub fn checked_add(&self, duration: f64) -> Option<Self> {
        match self {
            Self::Fixed(time) => checked_duration_add(*time, duration).map(Self::Fixed),
            Self::Now { offset_secs } => Some(Self::Now {
                offset_secs: offset_secs + duration,
            }),
        }
    }
    // checked_add:end
    /// Subtracts `duration` from `self`, returning `None` if `self` is a `Fixed(SystemTime)` and subtracting the duration is not possible
    /// because the result would be outside the bounds of the underlying data structure (see [`SystemTime::checked_sub`]).
    /// Otherwise returns `Some(time_expr)`.
    // checked_sub:start
//   purpose: Subtract a duration in seconds from a TimeExpr, returning None on underflow for Fixed variants.
//   input:  self - the TimeExpr; duration - f64 seconds to subtract (may be negative).
//   output: Option<TimeExpr> - None if the resulting Fixed instant is outside representable range.
//   sideEffects: none
    pub fn checked_sub(&self, duration: f64) -> Option<Self> {
        match self {
            Self::Fixed(time) => checked_duration_add(*time, -duration).map(Self::Fixed),
            Self::Now { offset_secs } => Some(Self::Now {
                offset_secs: offset_secs - duration,
            }),
        }
    }
    // checked_sub:end
}

impl Display for TimeExpr {
    // fmt:start
//   purpose: Format a TimeExpr as RFC3339 for Fixed or now(offset) for Now variant.
//   input:  self - the TimeExpr; f - Formatter to write into.
//   output: std::fmt::Result indicating success or formatting error.
//   sideEffects: none
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeExpr::Fixed(time) => {
                write!(f, "{}", format_rfc3339(*time))
            }
            TimeExpr::Now { offset_secs } => {
                if *offset_secs == 0.0 {
                    f.write_str("now()")
                } else {
                    write!(f, "now({offset_secs}s)")
                }
            }
        }
    }
    // fmt:end
}

impl FromStr for TimeExpr {
    type Err = ZError;
    // from_str:start
//   purpose: Parse a string into a TimeExpr, supporting RFC3339 timestamps and now() expressions.
//   input:  s - string slice (RFC3339 instant or now(±offset) syntax).
//   output: Result<TimeExpr, ZError> - Err on malformed input.
//   sideEffects: none
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("now(") && s.ends_with(')') {
            let s = &s[4..s.len() - 1];
            if s.is_empty() {
                Ok(TimeExpr::Now { offset_secs: 0.0 })
            } else {
                match s.chars().next() {
                    Some('-') => parse_duration(&s[1..]).map(|f| TimeExpr::Now { offset_secs: -f }),
                    Some(_) => parse_duration(s).map(|f| TimeExpr::Now { offset_secs: f }),
                    None => Err(zerror!("Empty time expression after 'now('")),
                }
            }
        } else {
            parse_rfc3339_weak(s)
                .map_err(|e| zerror!(e))
                .map(TimeExpr::Fixed)
        }
        .map_err(|e| zerror!(r#"Invalid time "{}" ({})"#, s, e))
    }
    // from_str:end
}

impl Add<f64> for TimeExpr {
    type Output = Self;
    // add:start
//   purpose: Add seconds to a TimeExpr, panicking on overflow for Fixed variants.
//   input:  self - the TimeExpr; duration - f64 seconds to add.
//   output: TimeExpr with the duration applied (panics on overflow for Fixed).
//   sideEffects: none (panics on overflow)
    fn add(self, duration: f64) -> Self {
        match self {
            Self::Fixed(time) => Self::Fixed(time + Duration::from_secs_f64(duration)),
            Self::Now { offset_secs } => Self::Now {
                offset_secs: offset_secs + duration,
            },
        }
    }
    // add:end
}

impl Add<f64> for &TimeExpr {
    type Output = TimeExpr;
    // add:start
//   purpose: Add seconds to a borrowed TimeExpr, panicking on overflow for Fixed variants.
//   input:  self - borrowed TimeExpr; duration - f64 seconds to add.
//   output: TimeExpr (owned) with the duration applied (panics on overflow for Fixed).
//   sideEffects: none (panics on overflow)
    fn add(self, duration: f64) -> TimeExpr {
        match self {
            TimeExpr::Fixed(time) => TimeExpr::Fixed((*time) + Duration::from_secs_f64(duration)),
            TimeExpr::Now { offset_secs } => TimeExpr::Now {
                offset_secs: offset_secs + duration,
            },
        }
    }
    // add:end
}

// checked_duration_add:start
//   purpose: Safely add a f64 seconds offset to a SystemTime, returning None on overflow or underflow.
//   input:  t - base SystemTime; duration - f64 seconds (positive adds, negative subtracts).
//   output: Option<SystemTime> - None if the arithmetic overflows the underlying representation.
//   sideEffects: none
fn checked_duration_add(t: SystemTime, duration: f64) -> Option<SystemTime> {
    if duration >= 0.0 {
        Duration::try_from_secs_f64(duration)
            .ok()
            .and_then(|d| t.checked_add(d))
    } else {
        Duration::try_from_secs_f64(-duration)
            .ok()
            .and_then(|d| t.checked_sub(d))
    }
}
// checked_duration_add:end

// parse_time_bound:start
//   purpose: Parse a string into a TimeBound, treating empty strings as Unbounded.
//   input:  s - string slice of a TimeExpr or empty for unbounded; inclusive - whether the bound delimiter is inclusive.
//   output: Result<TimeBound<TimeExpr>, ZError> - Err if the string is non-empty and cannot be parsed as TimeExpr.
//   sideEffects: none
fn parse_time_bound(s: &str, inclusive: bool) -> Result<TimeBound<TimeExpr>, ZError> {
    if s.is_empty() {
        Ok(TimeBound::Unbounded)
    } else if inclusive {
        Ok(TimeBound::Inclusive(s.parse()?))
    } else {
        Ok(TimeBound::Exclusive(s.parse()?))
    }
}
// parse_time_bound:end

/// Parses a &str as a Duration.
/// Expected format is a f64 in seconds, or "<f64><unit>" where <unit> is:
///  - 'u'  => microseconds
///  - "ms" => milliseconds
///  - 's' => seconds
///  - 'm' => minutes
///  - 'h' => hours
///  - 'd' => days
///  - 'w' => weeks
// parse_duration:start
//   purpose: Parse a duration string (plain f64 or f64 with unit suffix) into seconds as f64.
//   input:  s - string slice, e.g. "1.5", "100ms", "1h", "2d".
//   output: Result<f64, ZError> - total seconds; Err on malformed input or unknown unit.
//   sideEffects: none
fn parse_duration(s: &str) -> Result<f64, ZError> {
    if s.is_empty() {
        bail!(
            r#"Invalid duration: "" (expected format: <f64> (in seconds) or <f64><unit>. Accepted units: u, ms, s, m, h, d or w.)"#
        );
    }
    let mut it = s.bytes().enumerate().rev();
    match it.next() {
        Some((i, b'u')) => s[..i].parse::<f64>().map(|u| U_TO_SECS * u),
        Some((_, b's')) => match it.next() {
            Some((i, b'm')) => s[..i].parse::<f64>().map(|ms| MS_TO_SECS * ms),
            Some((i, _)) => s[..i + 1].parse::<f64>(),
            None => s.parse::<f64>(),
        },
        Some((i, b'm')) => s[..i].parse::<f64>().map(|m| M_TO_SECS * m),
        Some((i, b'h')) => s[..i].parse::<f64>().map(|h| H_TO_SECS * h),
        Some((i, b'd')) => s[..i].parse::<f64>().map(|d| D_TO_SECS * d),
        Some((i, b'w')) => s[..i].parse::<f64>().map(|w| W_TO_SECS * w),
        Some((_, _)) => s.parse::<f64>(),
        None => bail!("Duration string is empty"),
    }
    .map_err(|e| zerror!(r#"Invalid duration "{}" ({})"#, s, e))
}
// parse_duration:end

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // test_time_range_contains:start
//   purpose: Test that TimeRange::contains correctly evaluates instants against ranges with inclusive/exclusive/unbounded bounds and now offsets.
//   input:  none (test function).
//   output: passes or panics on assertion failure.
//   sideEffects: none
    fn test_time_range_contains() {
        assert!("[now(-1s)..now(1s)]"
            .parse::<TimeRange>()
            .unwrap()
            .contains(SystemTime::now()));
        assert!(!"[now(-2s)..now(-1s)]"
            .parse::<TimeRange>()
            .unwrap()
            .contains(SystemTime::now()));
        assert!(!"[now(1s)..now(2s)]"
            .parse::<TimeRange>()
            .unwrap()
            .contains(SystemTime::now()));

        assert!("[now(-1m)..]"
            .parse::<TimeRange>()
            .unwrap()
            .contains(SystemTime::now()));
        assert!("[..now(1m)]"
            .parse::<TimeRange>()
            .unwrap()
            .contains(SystemTime::now()));

        assert!("[..]"
            .parse::<TimeRange>()
            .unwrap()
            .contains(SystemTime::UNIX_EPOCH));
        assert!("[..]"
            .parse::<TimeRange>()
            .unwrap()
            .contains(SystemTime::now()));

        assert!("[1970-01-01T00:00:00Z..]"
            .parse::<TimeRange>()
            .unwrap()
            .contains(SystemTime::UNIX_EPOCH));
        assert!("[..1970-01-01T00:00:00Z]"
            .parse::<TimeRange>()
            .unwrap()
            .contains(SystemTime::UNIX_EPOCH));
        assert!(!"]1970-01-01T00:00:00Z..]"
            .parse::<TimeRange>()
            .unwrap()
            .contains(SystemTime::UNIX_EPOCH));
        assert!(!"[..1970-01-01T00:00:00Z["
            .parse::<TimeRange>()
            .unwrap()
            .contains(SystemTime::UNIX_EPOCH));
    }
    // test_time_range_contains:end

    #[test]
    // test_parse_time_range:start
//   purpose: Test parsing of various TimeRange string representations including unbounded and exclusive bounds.
//   input:  none (test function).
//   output: passes or panics on assertion failure.
//   sideEffects: none
    fn test_parse_time_range() {
        use TimeBound::*;
        assert_eq!(
            "[..]".parse::<TimeRange>().unwrap(),
            TimeRange {
                start: Unbounded,
                end: Unbounded
            }
        );
        assert_eq!(
            "[now(-1h)..now(1h)]".parse::<TimeRange>().unwrap(),
            TimeRange {
                start: Inclusive(TimeExpr::Now {
                    offset_secs: -3600.0
                }),
                end: Inclusive(TimeExpr::Now {
                    offset_secs: 3600.0
                })
            }
        );
        assert_eq!(
            "]now(-1h)..now(1h)[".parse::<TimeRange>().unwrap(),
            TimeRange {
                start: Exclusive(TimeExpr::Now {
                    offset_secs: -3600.0
                }),
                end: Exclusive(TimeExpr::Now {
                    offset_secs: 3600.0
                })
            }
        );

        assert!("".parse::<TimeExpr>().is_err());
        assert!("[;]".parse::<TimeExpr>().is_err());
        assert!("[;1h]".parse::<TimeExpr>().is_err());
    }
    // test_parse_time_range:end

    #[test]
    // test_parse_time_expr:start
//   purpose: Test parsing of RFC3339 timestamps and now() expressions into TimeExpr.
//   input:  none (test function).
//   output: passes or panics on assertion failure.
//   sideEffects: none
    fn test_parse_time_expr() {
        assert_eq!(
            "2022-06-30T01:02:03.226942997Z"
                .parse::<TimeExpr>()
                .unwrap(),
            TimeExpr::Fixed(humantime::parse_rfc3339("2022-06-30T01:02:03.226942997Z").unwrap())
        );
        assert_eq!(
            "2022-06-30T01:02:03Z".parse::<TimeExpr>().unwrap(),
            TimeExpr::Fixed(humantime::parse_rfc3339("2022-06-30T01:02:03Z").unwrap())
        );
        assert_eq!(
            "2022-06-30T01:02:03".parse::<TimeExpr>().unwrap(),
            TimeExpr::Fixed(humantime::parse_rfc3339("2022-06-30T01:02:03Z").unwrap())
        );
        assert_eq!(
            "2022-06-30 01:02:03Z".parse::<TimeExpr>().unwrap(),
            TimeExpr::Fixed(humantime::parse_rfc3339("2022-06-30T01:02:03Z").unwrap())
        );
        assert_eq!(
            "now()".parse::<TimeExpr>().unwrap(),
            TimeExpr::Now { offset_secs: 0.0 }
        );
        assert_eq!(
            "now(0)".parse::<TimeExpr>().unwrap(),
            TimeExpr::Now { offset_secs: 0.0 }
        );
        assert_eq!(
            "now(123.45)".parse::<TimeExpr>().unwrap(),
            TimeExpr::Now {
                offset_secs: 123.45
            }
        );
        assert_eq!(
            "now(1h)".parse::<TimeExpr>().unwrap(),
            TimeExpr::Now {
                offset_secs: 3600.0
            }
        );
        assert_eq!(
            "now(-1h)".parse::<TimeExpr>().unwrap(),
            TimeExpr::Now {
                offset_secs: -3600.0
            }
        );

        assert!("".parse::<TimeExpr>().is_err());
        assert!("1h".parse::<TimeExpr>().is_err());
        assert!("2020-11-05".parse::<TimeExpr>().is_err());
    }
    // test_parse_time_expr:end

    #[test]
    // test_add_time_expr:start
//   purpose: Test checked_add and checked_sub on both Now and Fixed TimeExpr variants, including overflow cases.
//   input:  none (test function).
//   output: passes or panics on assertion failure.
//   sideEffects: none
    fn test_add_time_expr() {
        let t = TimeExpr::Now { offset_secs: 0.0 };
        assert_eq!(
            t.checked_add(3600.0),
            Some(TimeExpr::Now {
                offset_secs: 3600.0
            })
        );
        assert_eq!(
            t.checked_add(-3600.0),
            Some(TimeExpr::Now {
                offset_secs: -3600.0
            })
        );
        assert_eq!(
            t.checked_sub(3600.0),
            Some(TimeExpr::Now {
                offset_secs: -3600.0
            })
        );
        assert_eq!(
            t.checked_sub(-3600.0),
            Some(TimeExpr::Now {
                offset_secs: 3600.0
            })
        );

        let t = TimeExpr::Fixed(SystemTime::UNIX_EPOCH);
        assert_eq!(
            t.checked_add(3600.0),
            Some(TimeExpr::Fixed(
                SystemTime::UNIX_EPOCH + Duration::from_secs_f64(3600.0)
            ))
        );
        assert_eq!(
            t.checked_add(-3600.0),
            Some(TimeExpr::Fixed(
                SystemTime::UNIX_EPOCH - Duration::from_secs_f64(3600.0)
            ))
        );
        assert_eq!(
            t.checked_sub(3600.0),
            Some(TimeExpr::Fixed(
                SystemTime::UNIX_EPOCH - Duration::from_secs_f64(3600.0)
            ))
        );
        assert_eq!(
            t.checked_sub(-3600.0),
            Some(TimeExpr::Fixed(
                SystemTime::UNIX_EPOCH + Duration::from_secs_f64(3600.0)
            ))
        );

        assert_eq!(t.checked_add(f64::MAX), None);
        assert_eq!(t.checked_sub(f64::MAX), None);
    }
    // test_add_time_expr:end

    #[test]
    // test_resolve_time_expr:start
//   purpose: Test checked_resolve_at for Now offsets and Fixed TimeExpr variants, including overflow edge case.
//   input:  none (test function).
//   output: passes or panics on assertion failure.
//   sideEffects: none
    fn test_resolve_time_expr() {
        let now = SystemTime::now();

        assert_eq!(
            TimeExpr::Now { offset_secs: 0.0 }.checked_resolve_at(now),
            Some(now)
        );
        assert_eq!(
            TimeExpr::Now {
                offset_secs: f64::MAX
            }
            .checked_resolve_at(now),
            None
        );

        let t = TimeExpr::Fixed(SystemTime::UNIX_EPOCH);
        assert_eq!(t.checked_resolve_at(now), Some(SystemTime::UNIX_EPOCH));
    }
    // test_resolve_time_expr:end

    #[test]
    // test_parse_duration:start
//   purpose: Test parse_duration with various unit suffixes (u, ms, s, m, h, d, w) and error cases.
//   input:  none (test function).
//   output: passes or panics on assertion failure.
//   sideEffects: none
    fn test_parse_duration() {
        assert_eq!(parse_duration("0").unwrap(), 0.0);
        assert_eq!(parse_duration("1.2").unwrap(), 1.2);
        assert_eq!(parse_duration("1u").unwrap(), 0.000001);
        assert_eq!(parse_duration("2u").unwrap(), 0.000002);
        assert_eq!(parse_duration("1.5ms").unwrap(), 0.0015);
        assert_eq!(parse_duration("100ms").unwrap(), 0.1);
        assert_eq!(parse_duration("10s").unwrap(), 10.0);
        assert_eq!(parse_duration("0.5s").unwrap(), 0.5);
        assert_eq!(parse_duration("1.1m").unwrap(), 66.0);
        assert_eq!(parse_duration("1.5h").unwrap(), 5400.0);
        assert_eq!(parse_duration("1d").unwrap(), 86400.0);
        assert_eq!(parse_duration("1w").unwrap(), 604800.0);

        assert!(parse_duration("").is_err());
        assert!(parse_duration("1x").is_err());
        assert!(parse_duration("abcd").is_err());
        assert!(parse_duration("4mm").is_err());
        assert!(parse_duration("1h4m").is_err());
    }
    // test_parse_duration:end
}
