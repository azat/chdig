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

    pub fn to_sql_datetime_64(&self) -> Option<String> {
        match (self.date_time, self.offset) {
            (Some(date_time), Some(offset)) => Some(format!(
                "fromUnixTimestamp64Nano({}) - INTERVAL {} NANOSECOND",
                date_time.timestamp_nanos_opt()?,
                offset.num_nanoseconds()?
            )),
            (None, Some(offset)) => Some(format!(
                "now() - INTERVAL {} NANOSECOND",
                offset.num_nanoseconds()?
            )),
            (Some(date_time), None) => Some(format!(
                "fromUnixTimestamp64Nano({})",
                date_time.timestamp_nanos_opt()?
            )),
            (None, None) => Some("now()".to_string()),
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
        self.offset = Some(rhs);
    }
}

impl SubAssign<TimeDelta> for RelativeDateTime {
    fn sub_assign(&mut self, rhs: TimeDelta) {
        self.offset = Some(rhs);
    }
}
