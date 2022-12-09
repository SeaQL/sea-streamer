use rdkafka::config::ClientConfig;
use sea_streamer::Timestamp;
use std::time::Duration;

#[derive(Debug, Default, Clone)]
pub struct ConsumerOption {
    /// https://kafka.apache.org/documentation/#connectconfigs_group.id
    group_id: Option<String>,
    /// https://kafka.apache.org/documentation/#connectconfigs_session.timeout.ms
    session_timeout: Option<Duration>,
    /// https://kafka.apache.org/documentation/#consumerconfigs_auto.offset.reset
    auto_offset_reset: Option<AutoOffsetReset>,
    /// https://kafka.apache.org/documentation/#consumerconfigs_enable.auto.commit
    enable_auto_commit: Option<bool>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum OptionKey {
    BootstrapServers,
    GroupId,
    SessionTimeout,
    AutoOffsetReset,
    EnableAutoCommit,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AutoOffsetReset {
    /// automatically reset the offset to the earliest offset
    Earliest,
    /// automatically reset the offset to the latest offset
    Latest,
    /// throw exception to the consumer if no previous offset is found for the consumer's group
    NoReset,
}

impl ConsumerOption {
    /// A unique string that identifies the consumer group this consumer belongs to.
    /// This property is required if the consumer uses either the group management functionality
    /// by using subscribe(topic) or the Kafka-based offset management strategy.
    pub fn group_id(&mut self, id: String) -> &mut Self {
        self.group_id = Some(id);
        self
    }

    /// The timeout used to detect worker failures. The worker sends periodic heartbeats
    /// to indicate its liveness to the broker. If no heartbeats are received by the broker
    /// before the expiration of this session timeout, then the broker will remove the worker
    /// from the group and initiate a rebalance.
    pub fn session_timeout(&mut self, v: Duration) -> &mut Self {
        self.session_timeout = Some(v);
        self
    }

    /// What to do when there is no initial offset in Kafka or if the current offset does
    /// not exist any more on the server.
    pub fn auto_offset_reset(&mut self, v: AutoOffsetReset) -> &mut Self {
        self.auto_offset_reset = Some(v);
        self
    }

    pub fn enable_auto_commit(&mut self, v: bool) -> &mut Self {
        self.enable_auto_commit = Some(v);
        self
    }

    fn set_client_config(&self, client_config: &mut ClientConfig) {
        if let Some(group_id) = &self.group_id {
            client_config.set(OptionKey::GroupId, group_id);
        } else {
            // https://github.com/edenhill/librdkafka/issues/3261
            // librdkafka always require a group_id even when not joining a consumer group
            // But this is purely a client side issue
            let random_id = format!(
                "{}",
                Timestamp::now_utc().unix_timestamp() * 1000 + fastrand::i64(0..1000),
            );
            client_config.set(OptionKey::GroupId, random_id);
        }
        if let Some(v) = self.session_timeout {
            client_config.set(OptionKey::SessionTimeout, format!("{}", v.as_millis()));
        }
        if let Some(v) = self.auto_offset_reset {
            client_config.set(OptionKey::AutoOffsetReset, v);
        }
        if let Some(v) = self.enable_auto_commit {
            client_config.set(OptionKey::EnableAutoCommit, v.to_string());
        }
    }
}

impl OptionKey {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::BootstrapServers => "bootstrap.servers",
            Self::GroupId => "group.id",
            Self::SessionTimeout => "session.timeout.ms",
            Self::AutoOffsetReset => "auto.offset.reset",
            Self::EnableAutoCommit => "enable.auto.commit",
        }
    }
}

impl AutoOffsetReset {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Earliest => "earliest",
            Self::Latest => "latest",
            Self::NoReset => "none",
        }
    }
}

macro_rules! impl_into_string {
    ($name:ident) => {
        impl From<$name> for String {
            fn from(o: $name) -> Self {
                o.as_str().to_owned()
            }
        }
    };
}

impl_into_string!(OptionKey);
impl_into_string!(AutoOffsetReset);
