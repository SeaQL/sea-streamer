use crate::FileErr;
use flume::Sender;
use notify::{
    event::ModifyKind, Config, EventKind, RecommendedWatcher, RecursiveMode,
    Watcher as WatcherTrait,
};
use std::path::Path;

pub(crate) enum FileEvent {
    Modify,
    Remove,
    Error(notify::Error),
}

pub(crate) type Watcher = RecommendedWatcher;

pub(crate) fn new_watcher<P: AsRef<Path>>(
    path: P,
    sender: Sender<FileEvent>,
) -> Result<Watcher, FileErr> {
    let mut watcher = RecommendedWatcher::new(
        move |event: Result<notify::Event, notify::Error>| {
            if let Err(e) = event {
                sender.send(FileEvent::Error(e)).ok();
                return;
            }
            log::trace!("{event:?}");
            match event.unwrap().kind {
                EventKind::Modify(modify) => {
                    match modify {
                        ModifyKind::Data(_) => {
                            // only if the file grows
                            sender.send(FileEvent::Modify).ok();
                        }
                        ModifyKind::Metadata(_) => {
                            // it only shows `Any` on my machine
                            sender.send(FileEvent::Remove).ok();
                        }
                        _ => (),
                    }
                }
                EventKind::Any | EventKind::Access(_) | EventKind::Create(_) | EventKind::Other => {
                }
                EventKind::Remove(_) => {
                    sender.send(FileEvent::Remove).ok();
                }
            }
        },
        Config::default(),
    )
    .map_err(|e| FileErr::WatchError(e))?;

    watcher
        .watch(path.as_ref(), RecursiveMode::Recursive)
        .map_err(|e| FileErr::WatchError(e))?;

    Ok(watcher)
}
