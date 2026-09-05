use chrono::{DateTime, Local, NaiveDate, NaiveDateTime, TimeDelta};
use std::{
    fmt::Display,
    ops::{AddAssign, SubAssign},
    str::FromStr,
};

pub fn parse_datetime_or_date(value: &str) -> Result<DateTime<Local>, String> {
    let mut errors = Vec::new();
    // Parse without timezone
    match value.parse::<NaiveDateTime>() {
        Ok(datetime) => return Ok(datetime.and_local_timezone(Local).unwrap()),
        Err(err) => errors.push(err),
    }
    // Parse *with* timezone
    match value.parse::<DateTime<Local>>() {
        Ok(datetime) => return Ok(datetime),
        Err(err) => errors.push(err),
    }
    // Parse as date
    match value.parse::<NaiveDate>() {
        Ok(date) => {
            return Ok(date
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_local_timezone(Local)
                .unwrap());
        }
        Err(err) => errors.push(err),
    }
    return Err(format!(
        "Valid RFC3339-formatted (YYYY-MM-DDTHH:MM:SS[.ssssss][±hh:mm|Z]) datetime or date while parsing '{}':\n{}",
        value,
        errors
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<String>>()
            .join("\n")
    ));
}

#[derive(Clone, Debug, PartialEq)]
pub enum RelativeDateTime {
    /// Fixed point in time
    Absolute(DateTime<Local>),
    /// Subtracted from the current time at evaluation
    Offset(TimeDelta),
    /// The current time at evaluation
    Now,
}

impl RelativeDateTime {
    pub fn get_date_time(&self) -> Option<DateTime<Local>> {
        match self {
            Self::Absolute(date_time) => Some(*date_time),
            _ => None,
        }
    }

    /// The point in time this denotes when evaluated at `now`.
    pub fn resolve(&self, now: DateTime<Local>) -> DateTime<Local> {
        match self {
            Self::Absolute(date_time) => *date_time,
            Self::Offset(delta) => now - *delta,
            Self::Now => now,
        }
    }

    pub fn to_editable_string(&self) -> String {
        match self {
            Self::Absolute(date_time) => date_time.format("%Y-%m-%dT%H:%M:%S").to_string(),
            Self::Offset(offset) => {
                humantime::format_duration(offset.to_std().unwrap_or_default()).to_string()
            }
            Self::Now => String::new(),
        }
    }

    pub fn to_sql_datetime_64(&self) -> Option<String> {
        match self {
            Self::Absolute(date_time) => Some(format!(
                "fromUnixTimestamp64Nano({})",
                date_time.timestamp_nanos_opt()?
            )),
            Self::Offset(offset) => Some(format!(
                "now64(9) - INTERVAL {} NANOSECOND",
                offset.num_nanoseconds()?
            )),
            Self::Now => Some("now64(9)".to_string()),
        }
    }
}

impl From<DateTime<Local>> for RelativeDateTime {
    fn from(value: DateTime<Local>) -> Self {
        RelativeDateTime::Absolute(value)
    }
}

impl From<Option<DateTime<Local>>> for RelativeDateTime {
    fn from(value: Option<DateTime<Local>>) -> Self {
        match value {
            Some(date_time) => RelativeDateTime::Absolute(date_time),
            None => RelativeDateTime::Now,
        }
    }
}

impl FromStr for RelativeDateTime {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        // Empty string is a special case for relative "now"
        // (i.e. it will be always calculated from current time)
        if s.is_empty() {
            Ok(RelativeDateTime::Now)
        } else if let Ok(datetime) = parse_datetime_or_date(s) {
            Ok(RelativeDateTime::Absolute(datetime))
        } else {
            Ok(RelativeDateTime::Offset(TimeDelta::from_std(
                s.parse::<humantime::Duration>()?.into(),
            )?))
        }
    }
}

impl<'de> serde::Deserialize<'de> for RelativeDateTime {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

impl From<RelativeDateTime> for DateTime<Local> {
    fn from(value: RelativeDateTime) -> Self {
        match value {
            RelativeDateTime::Absolute(date_time) => date_time,
            RelativeDateTime::Offset(offset) => Local::now() - offset,
            RelativeDateTime::Now => Local::now(),
        }
    }
}

impl Display for RelativeDateTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self))
    }
}

impl AddAssign<TimeDelta> for RelativeDateTime {
    fn add_assign(&mut self, rhs: TimeDelta) {
        *self -= -rhs;
    }
}

impl SubAssign<TimeDelta> for RelativeDateTime {
    fn sub_assign(&mut self, rhs: TimeDelta) {
        *self = match *self {
            Self::Absolute(date_time) => Self::Absolute(date_time - rhs),
            Self::Offset(offset) => Self::Offset(offset + rhs),
            Self::Now => Self::Offset(rhs),
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_relative_seek_accumulates_and_respects_direction() {
        let minutes = |m| TimeDelta::try_minutes(m).unwrap();

        let mut dt = RelativeDateTime::Offset(minutes(60));
        dt -= minutes(10);
        dt -= minutes(10);
        assert_eq!(dt, RelativeDateTime::Offset(minutes(80)));
        dt += minutes(10);
        assert_eq!(dt, RelativeDateTime::Offset(minutes(70)));

        // Relative "now" gets an offset on first seek
        let mut now = RelativeDateTime::Now;
        now -= minutes(10);
        assert_eq!(now, RelativeDateTime::Offset(minutes(10)));
    }

    #[test]
    fn test_absolute_seek_shifts_date_time() {
        let minutes = |m| TimeDelta::try_minutes(m).unwrap();
        let anchor = parse_datetime_or_date("2026-08-24T12:00:00").unwrap();

        let mut dt = RelativeDateTime::from(anchor);
        dt -= minutes(10);
        dt += minutes(30);
        assert_eq!(dt, RelativeDateTime::Absolute(anchor + minutes(20)));
        assert_eq!(dt.to_editable_string(), "2026-08-24T12:20:00");
    }
}
