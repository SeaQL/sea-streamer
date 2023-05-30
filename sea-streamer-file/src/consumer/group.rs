use flume::{bounded, unbounded, Receiver, Sender};
use sea_streamer_runtime::{spawn_task, AsyncMutex};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use super::FileConsumer;
use crate::{FileErr, FileId, MessageSource, StreamMode};
use sea_streamer_types::{ConsumerGroup, Message, SharedMessage, StreamKey};

lazy_static::lazy_static! {
    static ref STREAMERS: AsyncMutex<Streamers> = AsyncMutex::new(Streamers::new());
    static ref CONTROL: (Sender<CtrlMsg>, Receiver<CtrlMsg>) = unbounded();
}

struct Streamers {
    max_sid: Sid,
    streamers: HashMap<FileId, Vec<(StreamMode, StreamerHandle)>>,
}

pub(crate) type Sid = u32;

pub(crate) struct Pulse;

#[derive(Debug, Clone, Copy)]
enum CtrlMsg {
    Drop(Sid),
}

struct StreamerHandle {
    subscribers: Subscribers,
    pulse: Sender<Pulse>,
}

/// This is not the Streamer of the public API
struct Streamer {}

#[derive(Clone)]
struct Subscribers {
    subscribers: Arc<Mutex<SubscriberMap>>,
}

struct SubscriberMap {
    senders: HashMap<Sid, Sender<Result<SharedMessage, FileErr>>>,
    groups: Vec<((ConsumerGroup, StreamKey), Vec<Sid>)>,
    ungrouped: Vec<(StreamKey, Sid)>,
}

impl Streamers {
    fn new() -> Self {
        let _handle = spawn_task(async move {
            while let Ok(ctrl) = CONTROL.1.recv_async().await {
                match ctrl {
                    CtrlMsg::Drop(sid) => {
                        let mut streamers = STREAMERS.lock().await;
                        streamers.remove(sid);
                    }
                }
            }
        });

        Self {
            max_sid: 0,
            streamers: Default::default(),
        }
    }

    async fn add(
        &mut self,
        file_id: FileId,
        mode: StreamMode,
        group: Option<ConsumerGroup>,
        keys: Vec<StreamKey>,
    ) -> Result<FileConsumer, FileErr> {
        let (sender, receiver) = unbounded();
        self.max_sid += 1;
        let sid = self.max_sid;
        if self.streamers.get(&file_id).is_none() {
            self.streamers.insert(file_id.clone(), Vec::new());
        }
        let handles = self.streamers.get_mut(&file_id).unwrap();
        let handle = match mode {
            StreamMode::Live => {
                if let Some((_, handle)) = handles
                    .iter_mut()
                    .find(|(p, _)| matches!(p, StreamMode::Live))
                {
                    handle
                } else {
                    handles.push((
                        mode,
                        Streamer::new(MessageSource::new(file_id.clone()).await?),
                    ));
                    &mut handles.last_mut().unwrap().1
                }
            }
            StreamMode::Replay => {
                handles.push((
                    mode,
                    Streamer::new(MessageSource::new(file_id.clone()).await?),
                ));
                &mut handles.last_mut().unwrap().1
            }
        };
        handle.subscribers.add(sid, sender, group, keys);
        Ok(FileConsumer::new(sid, receiver, handle.pulse.clone()))
    }

    fn remove(&mut self, sid: Sid) {
        for (_, handles) in self.streamers.iter_mut() {
            for (_, handle) in handles.iter_mut() {
                handle.subscribers.remove(sid);
            }
        }
    }
}

pub(crate) async fn new_consumer(
    file_id: FileId,
    mode: StreamMode,
    group: Option<ConsumerGroup>,
    keys: Vec<StreamKey>,
) -> Result<FileConsumer, FileErr> {
    let mut streamers = STREAMERS.lock().await;
    streamers.add(file_id, mode, group, keys).await
}

pub(crate) fn remove_consumer(sid: Sid) {
    CONTROL
        .0
        .send(CtrlMsg::Drop(sid))
        .expect("Should never die");
}

impl Streamer {
    fn new(mut source: MessageSource) -> StreamerHandle {
        let subscribers = Subscribers::new();
        let (sender, pulse) = bounded(0);
        let ret = subscribers.clone();

        let _handle = spawn_task(async move {
            loop {
                _ = pulse.recv_async().await;
                let res = source.next().await;
                let is_err = res.is_err();
                subscribers.dispatch(match res {
                    Ok(m) => Ok(m.message.to_shared()),
                    Err(e) => Err(e),
                });
                if is_err {
                    break;
                }
            }
        });

        StreamerHandle {
            subscribers: ret,
            pulse: sender,
        }
    }
}

impl Subscribers {
    fn new() -> Self {
        Self {
            subscribers: Arc::new(Mutex::new(SubscriberMap {
                senders: Default::default(),
                groups: Default::default(),
                ungrouped: Default::default(),
            })),
        }
    }

    fn add(
        &self,
        sid: Sid,
        sender: Sender<Result<SharedMessage, FileErr>>,
        my_group: Option<ConsumerGroup>,
        my_keys: Vec<StreamKey>,
    ) {
        let mut map = self.subscribers.lock().unwrap();
        if map.senders.insert(sid, sender).is_none() {
            for my_key in my_keys {
                match my_group.clone() {
                    Some(my_group) => {
                        for ((group, key), sids) in map.groups.iter_mut() {
                            if group == &my_group && key == &my_key {
                                sids.push(sid);
                                return;
                            }
                        }
                        map.groups.push(((my_group, my_key), vec![sid]));
                    }
                    None => map.ungrouped.push((my_key, sid)),
                }
            }
        } else {
            panic!("Duplicate Subscriber {sid}");
        }
    }

    fn remove(&self, sid: Sid) {
        let mut map = self.subscribers.lock().unwrap();
        if map.senders.remove(&sid).is_none() {
            panic!("Unknown Subscriber {sid}");
        }
        map.ungrouped.retain(|(_, s)| s != &sid);
        for (_, sids) in map.groups.iter_mut() {
            if sids.contains(&sid) {
                sids.retain(|s| s != &sid);
            }
        }
        map.groups.retain(|(_, sids)| !sids.is_empty());
    }

    fn dispatch(&self, message: Result<SharedMessage, FileErr>) {
        let map = self.subscribers.lock().unwrap();
        match message {
            Ok(message) => {
                // send to relevant subscribers
                for ((_, stream_key), sids) in map.groups.iter() {
                    if stream_key == message.header().stream_key() {
                        // This round-robin is deterministic
                        let sid = sids[message.sequence() as usize % sids.len()];
                        let sender = map.senders.get(&sid).unwrap();
                        sender.send(Ok(message.clone())).ok();
                    }
                }

                for (stream_key, sid) in map.ungrouped.iter() {
                    if stream_key == message.header().stream_key() {
                        let sender = map.senders.get(&sid).unwrap();
                        sender.send(Ok(message.clone())).ok();
                    }
                }
            }
            Err(mut err) => {
                // broadcast the error
                for (_, sids) in map.groups.iter() {
                    for sid in sids.iter() {
                        let sender = map.senders.get(&sid).unwrap();
                        sender.send(Err(err.take())).ok();
                    }
                }

                for (_, sid) in map.ungrouped.iter() {
                    let sender = map.senders.get(&sid).unwrap();
                    sender.send(Err(err.take())).ok();
                }
            }
        }
    }
}
