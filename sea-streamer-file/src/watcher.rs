use crate::{FileErr, FileId};
use flume::{Sender, unbounded};
use notify::{
    Config, EventKind, RecommendedWatcher, RecursiveMode, Watcher as WatcherTrait,
    event::ModifyKind,
};
use sea_streamer_runtime::spawn_task;
use std::{
    collections::{BTreeSet, HashMap},
    sync::Mutex,
};

#[derive(Debug, Clone)]
pub(crate) enum FileEvent {
    Modify,
    Remove,
    Error(String),
    Rewatch,
}

static WATCHERS: std::sync::LazyLock<Mutex<Watchers>> =
    std::sync::LazyLock::new(|| Mutex::new(Watchers::new()));

type Wid = u32;

/// This is a process-wide singleton Watcher pool. No matter how many File handle
/// we have in the process, each file only has one Watcher registered with the OS.
///
/// The file system events are shared, which gives consistent behaviour.
struct Watchers {
    max_wid: Wid,
    watchers: HashMap<FileId, RecommendedWatcher>,
    listeners: BTreeSet<(FileId, Wid)>, // we want consistent iteration order
    senders: HashMap<Wid, Sender<FileEvent>>,
    sender: Sender<(FileId, FileEvent)>,
}

#[derive(Debug)]
/// A handle of a Watcher. The actual watchers are pooled are shared among the process.
pub struct Watcher {
    wid: Wid,
}

impl Watchers {
    fn new() -> Self {
        let (sender, receiver) = unbounded();
        let watchers = Self {
            max_wid: 0,
            watchers: Default::default(),
            listeners: Default::default(),
            senders: Default::default(),
            sender,
        };

        let _handle = spawn_task(async move {
            while let Ok((file_id, event)) = receiver.recv_async().await {
                let mut watchers = WATCHERS.lock().expect("Global Watchers error");
                watchers.dispatch(file_id, event);
            }
            log::error!("Global Watchers Task Dead");
        });

        watchers
    }

    fn dispatch(&mut self, file_id: FileId, event: FileEvent) {
        for (fid, wid) in self.listeners.iter() {
            if fid == &file_id {
                let sender = self.senders.get(wid).unwrap();
                sender.send(event.clone()).ok();
            }
        }
    }

    /// `Sender` should be unbounded, and never blocks.
    fn add(&mut self, file_id: FileId, sender: Sender<FileEvent>) -> Result<Watcher, FileErr> {
        assert!(sender.capacity().is_none());
        if !self.watchers.contains_key(&file_id) {
            let watcher = Self::new_watcher(file_id.clone(), self.sender.clone())?;
            self.watchers.insert(file_id.clone(), watcher);
        }

        self.max_wid += 1;
        let wid = self.max_wid;
        self.listeners.insert((file_id, wid));
        self.senders.insert(wid, sender);

        Ok(Watcher { wid })
    }

    fn remove(&mut self, wid: Wid) {
        if self.senders.remove(&wid).is_some() {
            let to_remove: Vec<_> = self
                .listeners
                .iter()
                .filter(|(_, w)| w == &wid)
                .cloned()
                .collect();
            for target in to_remove.iter() {
                self.listeners.remove(target);
            }
            assert_eq!(to_remove.len(), 1);
            let file_id = to_remove.into_iter().next().unwrap().0;
            let count = self.listeners.iter().filter(|(f, _)| f == &file_id).count();
            if count == 0 {
                // no one is watching this file anymore
                self.watchers.remove(&file_id);
                log::debug!("Stopped watching {file_id}");
            }
        }
    }

    fn new_watcher(
        file_id: FileId,
        sender: Sender<(FileId, FileEvent)>,
    ) -> Result<RecommendedWatcher, FileErr> {
        let fid = file_id.clone();
        let mut watcher = RecommendedWatcher::new(
            move |event: Result<notify::Event, notify::Error>| {
                if let Err(e) = event {
                    sender
                        .send((fid.clone(), FileEvent::Error(e.to_string())))
                        .ok();
                    return;
                }
                // log::trace!("{event:?}");
                match event.unwrap().kind {
                    EventKind::Modify(modify) => {
                        match modify {
                            ModifyKind::Data(_) => {
                                // only if the file grows
                                sender.send((fid.clone(), FileEvent::Modify)).ok();
                            }
                            ModifyKind::Metadata(_) => {
                                // we are in a different thread, but blocking here is still undesirable
                                if std::fs::metadata(fid.path()).is_err() {
                                    sender.send((fid.clone(), FileEvent::Remove)).ok();
                                }
                            }
                            _ => (),
                        }
                    }
                    EventKind::Any
                    | EventKind::Access(_)
                    | EventKind::Create(_)
                    | EventKind::Other => {}
                    EventKind::Remove(_) => {
                        sender.send((fid.clone(), FileEvent::Remove)).ok();
                    }
                }
            },
            Config::default(),
        )
        .map_err(|e| FileErr::WatchError(e.to_string()))?;

        watcher
            .watch(file_id.path().as_ref(), RecursiveMode::NonRecursive)
            .map_err(|e| FileErr::WatchError(e.to_string()))?;

        Ok(watcher)
    }
}

pub(crate) fn new_watcher(file_id: FileId, sender: Sender<FileEvent>) -> Result<Watcher, FileErr> {
    let mut watchers = WATCHERS.lock().expect("Global Watchers error");
    watchers.add(file_id, sender)
}

impl Drop for Watcher {
    fn drop(&mut self) {
        let mut watchers = WATCHERS.lock().expect("Global Watchers error");
        watchers.remove(self.wid)
    }
}
