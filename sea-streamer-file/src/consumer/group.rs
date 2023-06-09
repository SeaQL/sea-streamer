use flume::{bounded, unbounded, Receiver, Sender};
use sea_streamer_runtime::{spawn_task, AsyncMutex};
use std::{
    collections::HashMap,
    ops::Deref,
    sync::{Arc, Mutex},
};

use super::{CtrlMsg, FileConsumer};
use crate::{
    is_end_of_stream, is_pulse, pulse_message, ConfigErr, FileErr, FileId, MessageSource,
    StreamMode,
};
use sea_streamer_types::{
    export::futures::{select, FutureExt},
    ConsumerGroup, Message, ShardId, SharedMessage, StreamKey,
};

lazy_static::lazy_static! {
    static ref STREAMERS: AsyncMutex<Streamers> = AsyncMutex::new(Streamers::new());
    static ref CONTROL: (Sender<BgTask>, Receiver<BgTask>) = unbounded();
}

/// This is a process-wide singleton Streamer manager. It allows multiple stream consumers
/// to share the same FileSource, and thus has the same pace.
///
/// Most importantly, it manages consumer groups and dispatches messages fairly.
/// This behaviour is very similar to stdio, but there is no broadcast channel.
/// In stdio there is only one global Streamer, where in file, each file is a Streamer.
///
/// Right now we have the constraint that each Consumer can only stream from one file source.
/// There is nothing other than implementation complexity prevents Consumer <> File being an
/// M to N relationship. One needs to be very careful about deadlock, memory leak and race condition.
/// Which sadly we currently don't have the theoretical tools to deal with.
struct Streamers {
    max_sid: Sid,
    streamers: HashMap<FileId, Vec<(StreamMode, Streamer)>>,
}

pub(crate) type Sid = u32;
const ZERO: ShardId = ShardId::new(0);

#[derive(Debug, Clone, Copy)]
enum BgTask {
    Drop(Sid),
}

/// This is not the Streamer of the public API
struct Streamer {
    subscribers: Subscribers,
    ctrl: Sender<CtrlMsg>,
    tick: Sender<()>,
}

pub struct StreamerInfo {
    pub mode: StreamMode,
    pub subscribers: Vec<SubscriberInfo>,
}

pub struct SubscriberInfo {
    pub sid: Sid,
    pub group: Option<ConsumerGroup>,
    pub stream_key: StreamKey,
}

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
                    BgTask::Drop(sid) => {
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
        let mut handle = if let Some(group) = &group {
            // consumers in the same group always share a stream
            if let Some((m, h)) = handles
                .iter_mut()
                .find(|(_, h)| h.subscribers.has_group(group))
            {
                if *m == mode {
                    // consumers in the same group must use the same mode
                    Some(h)
                } else {
                    // you are wrong
                    return Err(FileErr::ConfigErr(ConfigErr::SameGroupSameMode));
                }
            } else {
                // no existing members yet, so whatever is asked for
                None
            }
        } else {
            // no group
            match mode {
                StreamMode::Live => {
                    // live stream can be shared among consumers
                    handles.iter_mut().find(|(p, _)| p == &mode).map(|(_, h)| h)
                }
                StreamMode::LiveReplay | StreamMode::Replay => {
                    // otherwise each consumer 'owns' a streamer
                    None
                }
            }
        };
        if handle.is_none() {
            handles.push((
                mode,
                Streamer::create(MessageSource::new(file_id.clone(), mode).await?),
            ));
            handle = Some(&mut handles.last_mut().unwrap().1);
        }
        let handle = handle.unwrap();
        handle.subscribers.add(sid, sender, group, keys);
        Ok(FileConsumer::new(
            file_id,
            sid,
            receiver,
            handle.ctrl.clone(),
        ))
    }

    fn remove(&mut self, sid: Sid) {
        for (_, handles) in self.streamers.iter_mut() {
            for (_, handle) in handles.iter_mut() {
                handle.subscribers.remove(sid);
            }
            handles.retain(|(_, h)| !h.subscribers.is_empty());
        }
    }

    /// Check if the Streamer is 'solo', if not, detach it and make it solo.
    /// Also might 'tick' the streamer.
    async fn pre_seek(&mut self, file_id: &FileId, sid: Sid) -> Result<(), FileErr> {
        if let Some(handles) = self.streamers.get_mut(file_id) {
            for (mode, handle) in handles.iter_mut() {
                let new_mode = match mode {
                    StreamMode::Live | StreamMode::LiveReplay => StreamMode::LiveReplay,
                    StreamMode::Replay => StreamMode::Replay,
                };
                if handle.subscribers.has_sid(sid) {
                    if handle.subscribers.is_solo() {
                        // we can go on and reuse the same Streamer
                        *mode = new_mode;
                        // we abort the current message read
                        handle.tick.try_send(()).expect("send should never block");
                    } else {
                        let (sender, _group, keys) =
                            handle.subscribers.remove(sid).expect("Checked by has_sid");
                        // create a new source
                        handles.push((
                            new_mode,
                            Streamer::create(MessageSource::new(file_id.clone(), new_mode).await?),
                        ));
                        let handle = &mut handles.last_mut().unwrap().1;
                        // subscribe to the source
                        handle.subscribers.add(sid, sender, None, keys);
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    fn query(&self, file_id: &FileId) -> Option<Vec<StreamerInfo>> {
        self.streamers.get(file_id).map(|handles| {
            handles
                .iter()
                .map(|(m, h)| StreamerInfo {
                    mode: *m,
                    subscribers: h.subscribers.info(),
                })
                .collect()
        })
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
    CONTROL.0.send(BgTask::Drop(sid)).expect("Should never die");
}

pub(crate) async fn preseek_consumer(file_id: &FileId, sid: Sid) -> Result<(), FileErr> {
    let mut streamers = STREAMERS.lock().await;
    streamers.pre_seek(file_id, sid).await
}

/// Query info about global Streamer(s) topology
pub async fn query_streamer(file_id: &FileId) -> Option<Vec<StreamerInfo>> {
    let streamers = STREAMERS.lock().await;
    streamers.query(file_id)
}

impl Streamer {
    fn create(mut source: MessageSource) -> Self {
        let subscribers = Subscribers::new();
        let (sender, ctrl) = bounded(0);
        let (ticker, tick) = bounded(1);
        let ret = subscribers.clone();

        let _handle = spawn_task(async move {
            loop {
                match ctrl.recv_async().await {
                    Ok(CtrlMsg::Read) => {
                        tick.drain();
                        let res = select! {
                            m = source.next().fuse() => m,
                            // the above future is not cancel safe, but a subsequent seek
                            // will rectify the internal states
                            _ = tick.recv_async().fuse() => continue,
                        };
                        let res: Result<SharedMessage, FileErr> =
                            res.map(|m| m.message.to_shared());
                        let end = match &res {
                            Ok(m) => is_end_of_stream(m),
                            Err(_) => true,
                        };
                        subscribers.dispatch(res);
                        if end {
                            // when this ends, source will be dropped as well
                            break;
                        }
                    }
                    Ok(CtrlMsg::Seek(target)) => {
                        if let Some(keys) = subscribers.solo_keys() {
                            match source.seek(&keys[0], &ZERO, target).await {
                                Ok(()) => {
                                    subscribers.dispatch(Ok(pulse_message().to_shared()));
                                }
                                Err(e) => {
                                    subscribers.dispatch(Err(e));
                                    break;
                                }
                            }
                        } else {
                            log::error!("Cannot seek a shared MessageSource");
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        Self {
            subscribers: ret,
            ctrl: sender,
            tick: ticker,
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

    fn len(&self) -> usize {
        let map = self.subscribers.lock().unwrap();
        map.senders.len()
    }

    fn is_solo(&self) -> bool {
        let map = self.subscribers.lock().unwrap();
        Self::is_solo_inner(&map)
    }

    #[inline]
    fn is_solo_inner<M: Deref<Target = SubscriberMap>>(map: &M) -> bool {
        map.senders.len() == 1
    }

    fn solo_keys(&self) -> Option<Vec<StreamKey>> {
        let map = self.subscribers.lock().unwrap();
        if Self::is_solo_inner(&map) {
            let mut keys: Vec<_> = map.ungrouped.iter().map(|(k, _)| k).cloned().collect();
            for ((_, key), _) in map.groups.iter() {
                keys.push(key.clone());
            }
            Some(keys)
        } else {
            None
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn has_sid(&self, sid: Sid) -> bool {
        let map = self.subscribers.lock().unwrap();
        map.senders.contains_key(&sid)
    }

    fn has_group(&self, group: &ConsumerGroup) -> bool {
        let map = self.subscribers.lock().unwrap();
        map.groups.iter().any(|((g, _), _)| g == group)
    }

    fn info(&self) -> Vec<SubscriberInfo> {
        let map = self.subscribers.lock().unwrap();
        let mut subs = Vec::new();
        for ((group, stream_key), sids) in map.groups.iter() {
            for sid in sids.iter() {
                subs.push(SubscriberInfo {
                    sid: *sid,
                    group: Some(group.clone()),
                    stream_key: stream_key.clone(),
                });
            }
        }
        for (stream_key, sid) in map.ungrouped.iter() {
            subs.push(SubscriberInfo {
                sid: *sid,
                group: None,
                stream_key: stream_key.clone(),
            });
        }
        subs
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

    fn remove(
        &self,
        sid: Sid,
    ) -> Option<(
        Sender<Result<SharedMessage, FileErr>>,
        Option<ConsumerGroup>,
        Vec<StreamKey>,
    )> {
        let mut map = self.subscribers.lock().unwrap();
        if let Some(sender) = map.senders.remove(&sid) {
            let mut keys: Vec<_> = map
                .ungrouped
                .iter()
                .filter(|(_, s)| s == &sid)
                .map(|(k, _)| k)
                .cloned()
                .collect();
            map.ungrouped.retain(|(_, s)| s != &sid);
            let mut group = None;
            for ((gp, key), sids) in map.groups.iter_mut() {
                if sids.contains(&sid) {
                    assert!(group.is_none());
                    group = Some(gp.clone());
                    keys.push(key.clone());
                    sids.retain(|s| s != &sid);
                }
            }
            map.groups.retain(|(_, sids)| !sids.is_empty());
            Some((sender, group, keys))
        } else {
            None
        }
    }

    fn dispatch(&self, message: Result<SharedMessage, FileErr>) {
        let map = self.subscribers.lock().unwrap();
        match message {
            Ok(message) => {
                // send to relevant subscribers
                for ((_, stream_key), sids) in map.groups.iter() {
                    if is_pulse(&message) || stream_key == message.header().stream_key() {
                        // This round-robin is deterministic
                        let sid = sids[message.sequence() as usize % sids.len()];
                        let sender = map.senders.get(&sid).unwrap();
                        sender.send(Ok(message.clone())).ok();
                    }
                }

                for (stream_key, sid) in map.ungrouped.iter() {
                    if is_pulse(&message) || stream_key == message.header().stream_key() {
                        let sender = map.senders.get(sid).unwrap();
                        sender.send(Ok(message.clone())).ok();
                    }
                }
            }
            Err(mut err) => {
                // broadcast the error
                for (_, sids) in map.groups.iter() {
                    for sid in sids.iter() {
                        let sender = map.senders.get(sid).unwrap();
                        sender.send(Err(err.take())).ok();
                    }
                }

                for (_, sid) in map.ungrouped.iter() {
                    let sender = map.senders.get(sid).unwrap();
                    sender.send(Err(err.take())).ok();
                }
            }
        }
    }
}
