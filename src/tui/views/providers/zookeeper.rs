use crate::{
    interpreter::{ClickHouseAvailableQuirks, ContextArc, options::ChDigViews},
    tui::{
        App, Dialog, Nameable, Navigation, OnEventView, Resizable, Scrollable, TextView,
        ViewProvider,
        event::{Event, EventResult, Key},
        views::sql_query_view::{Row as QueryResultRow, SQLQueryView},
    },
};
use std::sync::{Arc, Mutex};

pub struct ZooKeeperViewProvider;

impl ViewProvider for ZooKeeperViewProvider {
    fn name(&self) -> &'static str {
        "ZooKeeper"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::Zookeeper
    }

    fn show(&self, app: &mut App, context: ContextArc, _instance: Option<&str>) {
        // Keep the browsing position of an already opened browser
        if app.focus_name(VIEW_NAME) {
            return;
        }
        show_zookeeper(app, context, DEFAULT_ZOOKEEPER, "/");
    }
}

const VIEW_NAME: &str = "zookeeper";
/// zkutil::DEFAULT_ZOOKEEPER_NAME (system.replicas has it for tables on the
/// default ZooKeeper, auxiliary ones are named in <auxiliary_zookeepers>)
pub const DEFAULT_ZOOKEEPER: &str = "default";

/// Where the browser is: (ZooKeeper name, node path)
struct Location {
    zookeeper: String,
    path: String,
}

fn normalize_path(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{}", trimmed)
    }
}

fn child_path(path: &str, name: &str) -> String {
    if path == "/" {
        format!("/{}", name)
    } else {
        format!("{}/{}", path, name)
    }
}

fn parent_path(path: &str) -> String {
    match path.rfind('/') {
        Some(0) | None => "/".to_string(),
        Some(pos) => path[..pos].to_string(),
    }
}

fn quote(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

/// The displayed columns come first ("_"-prefixed ones are fetched but hidden,
/// the raw multi-line value and the details of the node dialog).
///
/// Node data (and in theory names) can be binary, and the driver rejects
/// invalid UTF-8 strings, hence toValidUTF8() (a node with an invalid name
/// is listed but cannot be entered, since its path is rebuilt from the name).
/// Control characters (newlines, tabs, CR) are collapsed for the one-line
/// cell: written verbatim into a terminal cell they garble the row.
const COLUMNS: &[&str] = &[
    "toValidUTF8(name) name",
    "numChildren children",
    "dataLength size",
    "version",
    "mtime",
    // Not aliased "value": an alias is substituted into the other SELECT
    // expressions, and _value would get the collapsed one (the header is
    // renamed to "value" below).
    "replaceRegexpAll(toValidUTF8(value), '[[:cntrl:]]+', ' ') value_",
    "toValidUTF8(value) _value",
    "ctime _ctime",
    "ephemeralOwner _ephemeral",
    "czxid _czxid",
    "mzxid _mzxid",
];

fn build_query(location: &Location) -> String {
    // The same ZooKeeper for every replica, so no clusterAllReplicas().
    // The zookeeperName filter (25.6+) is only emitted for auxiliary
    // ZooKeepers, so the default one works on older servers as well.
    let zookeeper_filter = if location.zookeeper == DEFAULT_ZOOKEEPER {
        String::new()
    } else {
        format!("zookeeperName = '{}' AND ", quote(&location.zookeeper))
    };
    format!(
        "SELECT {} FROM system.zookeeper WHERE {}path = '{}' ORDER BY name",
        COLUMNS.join(", "),
        zookeeper_filter,
        quote(&location.path),
    )
}

fn title(location: &Location) -> String {
    let zookeeper = if location.zookeeper == DEFAULT_ZOOKEEPER {
        String::new()
    } else {
        format!("[{}]", location.zookeeper)
    };
    format!(
        "ZooKeeper{}: {} (Enter: open node, Backspace/u: up)",
        zookeeper, location.path
    )
}

fn navigate(view: &mut SQLQueryView, current: &Mutex<Location>, path: String) {
    let mut location = current.lock().unwrap();
    location.path = path;
    view.set_title(title(&location));
    view.set_query(build_query(&location));
}

fn go_up(view: &mut SQLQueryView, current: &Mutex<Location>) -> Option<EventResult> {
    let (parent, child) = {
        let location = current.lock().unwrap();
        if location.path == "/" {
            return None;
        }
        let parent = parent_path(&location.path);
        let child = location.path[parent.len()..]
            .trim_start_matches('/')
            .to_string();
        (parent, child)
    };
    navigate(view, current, parent);
    // Land on the node we came from
    view.select_on_update("name", child);
    Some(EventResult::consumed())
}

/// Node data as a terminal can show it: CRLF/CR become newlines, tabs
/// become spaces (see utils::get_query), other control characters U+FFFD.
fn printable(value: &str) -> String {
    value
        .replace("\r\n", "\n")
        .replace('\r', "\n")
        .replace('\t', "    ")
        .chars()
        .map(|c| {
            if c.is_control() && c != '\n' {
                '\u{FFFD}'
            } else {
                c
            }
        })
        .collect()
}

fn show_node(app: &mut App, path: &str, columns: &[&'static str], row: &QueryResultRow) {
    let field = |name: &str| {
        columns
            .iter()
            .zip(row.0.iter())
            .find_map(|(c, r)| (*c == name).then(|| r.to_string()))
            .unwrap_or_default()
    };
    let info = format!(
        "path: {}\nchildren: {}\nsize: {}\nversion: {}\nctime: {}\nmtime: {}\nczxid: {}\nmzxid: {}\nephemeralOwner: {}\n\n{}",
        path,
        field("children"),
        field("size"),
        field("version"),
        field("_ctime"),
        field("mtime"),
        field("_czxid"),
        field("_mzxid"),
        field("_ephemeral"),
        printable(&field("_value")),
    );
    app.add_layer(
        Dialog::around(TextView::new(info).scrollable())
            .title("ZooKeeper node")
            .button("Ok", |app| {
                app.pop_layer();
            }),
    );
}

/// Opens (or replaces) the ZooKeeper browser at `path` of the `zookeeper`
/// (DEFAULT_ZOOKEEPER or an auxiliary one).
pub fn show_zookeeper(app: &mut App, context: ContextArc, zookeeper: &str, path: &str) {
    let zookeeper = if zookeeper.is_empty() {
        DEFAULT_ZOOKEEPER
    } else {
        zookeeper
    };
    if zookeeper != DEFAULT_ZOOKEEPER
        && !context
            .lock()
            .unwrap()
            .clickhouse
            .quirks
            .has(ClickHouseAvailableQuirks::SystemZooKeeperName)
    {
        app.add_layer(Dialog::info(format!(
            "Browsing the auxiliary ZooKeeper '{}' requires ClickHouse 25.6+ (system.zookeeper zookeeperName filter)",
            zookeeper
        )));
        return;
    }
    let location = Location {
        zookeeper: zookeeper.to_string(),
        path: normalize_path(path),
    };
    let query = build_query(&location);
    let view_title = title(&location);
    let current = Arc::new(Mutex::new(location));

    let mut view = SQLQueryView::new(
        context,
        VIEW_NAME,
        "children",
        COLUMNS.to_vec(),
        vec!["name"],
        vec!["value_"],
        query,
    )
    .unwrap_or_else(|_| panic!("Cannot create {}", VIEW_NAME));
    view.get_inner_mut().set_title(view_title);
    view.get_inner_mut().set_column_title("value_", "value");

    let submit_current = current.clone();
    view.get_inner_mut().set_on_submit(
        move |app: &mut App, columns: Vec<&'static str>, row: QueryResultRow| {
            let field = |name: &str| {
                columns
                    .iter()
                    .zip(row.0.iter())
                    .find_map(|(c, r)| (*c == name).then(|| r.to_string()))
            };
            let (Some(name), Some(children)) = (field("name"), field("children")) else {
                return;
            };
            let path = child_path(&submit_current.lock().unwrap().path, &name);
            if children.parse::<i64>().unwrap_or(0) > 0 {
                let current = submit_current.clone();
                app.call_on_name(VIEW_NAME, |view: &mut OnEventView<SQLQueryView>| {
                    navigate(view.get_inner_mut(), &current, path);
                });
            } else {
                show_node(app, &path, &columns, &row);
            }
        },
    );

    let up_current = current.clone();
    view.set_on_event_inner(Event::Key(Key::Backspace), move |view, _| {
        go_up(view, &up_current)
    });
    view.set_on_event_inner(Event::Char('u'), move |view, _| go_up(view, &current));

    app.present_view(VIEW_NAME, view.with_name(VIEW_NAME).full_screen());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_printable() {
        assert_eq!(printable("a\r\nb\rc\td\x01"), "a\nb\nc    d\u{FFFD}");
    }

    #[test]
    fn test_paths() {
        assert_eq!(normalize_path(""), "/");
        assert_eq!(normalize_path("/"), "/");
        assert_eq!(normalize_path("/a/b/"), "/a/b");
        assert_eq!(normalize_path("a"), "/a");
        assert_eq!(child_path("/", "a"), "/a");
        assert_eq!(child_path("/a", "b"), "/a/b");
        assert_eq!(parent_path("/a/b"), "/a");
        assert_eq!(parent_path("/a"), "/");
        assert_eq!(parent_path("/"), "/");
    }
}
