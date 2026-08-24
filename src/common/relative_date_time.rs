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

#[derive(Clone, Debug)]
pub struct RelativeDateTime {
    date_time: Option<DateTime<Local>>,
    // Always subtracted
    offset: Option<TimeDelta>,
}

impl RelativeDateTime {
    pub fn new(offset: Option<TimeDelta>) -> Self {
        Self {
            date_time: None,
            offset,
        }
    }

    pub fn get_date_time(&self) -> Option<DateTime<Local>> {
        self.date_time
    }

    pub fn to_editable_string(&self) -> String {
        match (&self.date_time, &self.offset) {
            (None, Some(offset)) => {
                humantime::format_duration(offset.to_std().unwrap_or_default()).to_string()
            }
            (Some(dt), _) => dt.format("%Y-%m-%dT%H:%M:%S").to_string(),
            (None, None) => String::new(),
        }
    }

    pub fn to_sql_datetime_64(&self) -> Option<String> {
        match (self.date_time, self.offset) {
            (Some(date_time), Some(offset)) => Some(format!(
                "fromUnixTimestamp64Nano({}) - INTERVAL {} NANOSECOND",
                date_time.timestamp_nanos_opt()?,
                offset.num_nanoseconds()?
            )),
            (None, Some(offset)) => Some(format!(
                "now64(9) - INTERVAL {} NANOSECOND",
                offset.num_nanoseconds()?
            )),
            (Some(date_time), None) => Some(format!(
                "fromUnixTimestamp64Nano({})",
                date_time.timestamp_nanos_opt()?
            )),
            (None, None) => Some("now64(9)".to_string()),
        }
    }
}

impl From<DateTime<Local>> for RelativeDateTime {
    fn from(value: DateTime<Local>) -> Self {
        RelativeDateTime {
            date_time: Some(value),
            offset: None,
        }
    }
}

impl From<Option<DateTime<Local>>> for RelativeDateTime {
    fn from(value: Option<DateTime<Local>>) -> Self {
        RelativeDateTime {
            date_time: value,
            offset: None,
        }
    }
}

impl FromStr for RelativeDateTime {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        // Empty string is a special case for relative "now"
        // (i.e. it will be always calculated from current time)
        if s.is_empty() {
            Ok(RelativeDateTime {
                date_time: None,
                offset: None,
            })
        } else if let Ok(datetime) = parse_datetime_or_date(s) {
            Ok(RelativeDateTime {
                date_time: Some(datetime),
                offset: None,
            })
        } else {
            Ok(RelativeDateTime {
                date_time: None,
                offset: Some(TimeDelta::from_std(
                    s.parse::<humantime::Duration>()?.into(),
                )?),
            })
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
        let mut date_time = value.date_time.unwrap_or(Local::now());
        if let Some(offset) = value.offset {
            date_time -= offset;
        }
        return date_time;
    }
}

impl Display for RelativeDateTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "{:?} (offset={:?})",
            self.date_time, self.offset
        ))
    }
}

impl AddAssign<TimeDelta> for RelativeDateTime {
    fn add_assign(&mut self, rhs: TimeDelta) {
        *self -= -rhs;
    }
}

impl SubAssign<TimeDelta> for RelativeDateTime {
    fn sub_assign(&mut self, rhs: TimeDelta) {
        if let Some(date_time) = &mut self.date_time {
            *date_time -= rhs;
        } else {
            self.offset = Some(self.offset.unwrap_or_else(TimeDelta::zero) + rhs);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_relative_seek_accumulates_and_respects_direction() {
        let minutes = |m| TimeDelta::try_minutes(m).unwrap();

        let mut dt = RelativeDateTime::new(Some(minutes(60)));
        dt -= minutes(10);
        dt -= minutes(10);
        assert_eq!(dt.offset, Some(minutes(80)));
        dt += minutes(10);
        assert_eq!(dt.offset, Some(minutes(70)));

        // Relative "now" gets an offset on first seek
        let mut now = RelativeDateTime::new(None);
        now -= minutes(10);
        assert_eq!(now.offset, Some(minutes(10)));
        assert!(now.date_time.is_none());
    }

    #[test]
    fn test_absolute_seek_shifts_date_time() {
        let minutes = |m| TimeDelta::try_minutes(m).unwrap();
        let anchor = parse_datetime_or_date("2026-08-24T12:00:00").unwrap();

        let mut dt = RelativeDateTime::from(anchor);
        dt -= minutes(10);
        dt += minutes(30);
        assert_eq!(dt.date_time, Some(anchor + minutes(20)));
        assert_eq!(dt.offset, None);
        assert_eq!(dt.to_editable_string(), "2026-08-24T12:20:00");
    }
}
