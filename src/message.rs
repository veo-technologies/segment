//! Representations of the messages which may be sent to Segment's tracking API.
//!
//! All Segment messages support a few common fields:
//!
//! * Details related to user identification are captured by this library
//!   through the [`User`] enum.
//!
//! * Some user traits and event properties are specified through the Segment
//!   spec -- these are standardized members which, if followed, will be
//!   converted to the native equivalent of each tool.
//!
//!     * Standardized event names and properties are specified in [Segment's
//!       semantic events docs](https://segment.com/docs/spec/semantic/).
//!     * Standardized user traits are specified in [Segment's `identify` traits
//!       docs](https://segment.com/docs/spec/identify/#traits).
//!     * Standardized group traits are specified in [Segment's `group` traits
//!       docs](https://segment.com/docs/spec/group/#traits).
//!
//! * All Segment messages support a `context` field containing additional
//!   contextual details. This field is exposed in this library as `context`.
//!   The data in `context` is standardized, and is documented in [Segment's
//!   context docs](https://segment.com/docs/spec/common/#context).
//!
//! * All Segment messages support an `integrations` field that enables simple
//!   routing at the event collection layer. See [Segment's `integrations`
//!   docs](https://segment.com/docs/spec/common/#integrations) for how to use
//!   this field.

use std::fmt::Display;

use jiff::Timestamp;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use serde_json::{Map, Value, json};

use crate::{Result, ValidationError};

pub(crate) const MAX_BATCH_MESSAGES: usize = 2_500;

/// An enum containing all values which may be sent to Segment's tracking API.
#[derive(PartialEq, Eq, Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum Message {
    Identify(Identify),
    Track(Track),
    Page(Page),
    Screen(Screen),
    Group(Group),
    Alias(Alias),
    Batch(Batch),
}

impl<'de> Deserialize<'de> for Message {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut value = Value::deserialize(deserializer)?;
        let object = value
            .as_object_mut()
            .ok_or_else(|| D::Error::custom("a Segment message must be a JSON object"))?;

        if object.contains_key("batch") {
            return serde_json::from_value(value)
                .map(Self::Batch)
                .map_err(D::Error::custom);
        }

        let message_type = object
            .remove("type")
            .and_then(|value| value.as_str().map(str::to_owned));
        let inferred_type = message_type.as_deref().or_else(|| {
            if object.contains_key("event") {
                Some("track")
            } else if object.contains_key("groupId") {
                Some("group")
            } else if object.contains_key("previousId") {
                Some("alias")
            } else if object.contains_key("traits") {
                Some("identify")
            } else {
                None
            }
        });

        match inferred_type {
            Some("identify") => serde_json::from_value(value)
                .map(Self::Identify)
                .map_err(D::Error::custom),
            Some("track") => serde_json::from_value(value)
                .map(Self::Track)
                .map_err(D::Error::custom),
            Some("page") => serde_json::from_value(value)
                .map(Self::Page)
                .map_err(D::Error::custom),
            Some("screen") => serde_json::from_value(value)
                .map(Self::Screen)
                .map_err(D::Error::custom),
            Some("group") => serde_json::from_value(value)
                .map(Self::Group)
                .map_err(D::Error::custom),
            Some("alias") => serde_json::from_value(value)
                .map(Self::Alias)
                .map_err(D::Error::custom),
            Some(other) => Err(D::Error::custom(format!(
                "unknown Segment message type `{other}`"
            ))),
            None => Err(D::Error::custom(
                "message type is ambiguous; include a `type` field",
            )),
        }
    }
}

impl Message {
    /// Validate this message against the client-side portion of Segment's
    /// Tracking API contract.
    pub fn validate(&self) -> std::result::Result<(), ValidationError> {
        match self {
            Self::Identify(message) => validate_identify(message),
            Self::Track(message) => validate_track(message),
            Self::Page(message) => validate_page(message),
            Self::Screen(message) => validate_screen(message),
            Self::Group(message) => validate_group(message),
            Self::Alias(message) => validate_alias(message),
            Self::Batch(batch) => validate_batch(batch),
        }
    }

    /// Validate this message and populate any missing `messageId`, library
    /// context, or batch `sentAt` metadata.
    pub fn prepare(&mut self) -> Result<()> {
        self.validate()?;

        match self {
            Self::Identify(message) => {
                prepare_common(&mut message.message_id, &mut message.context)
            }
            Self::Track(message) => prepare_common(&mut message.message_id, &mut message.context),
            Self::Page(message) => prepare_common(&mut message.message_id, &mut message.context),
            Self::Screen(message) => prepare_common(&mut message.message_id, &mut message.context),
            Self::Group(message) => prepare_common(&mut message.message_id, &mut message.context),
            Self::Alias(message) => prepare_common(&mut message.message_id, &mut message.context),
            Self::Batch(batch) => {
                for message in &mut batch.batch {
                    message.prepare()?;
                }
                batch.sent_at = Some(Timestamp::now());
            }
        }

        Ok(())
    }
}

/// An identify event.
///
/// See [Segment's documentation](https://segment.com/docs/spec/identify/) for
/// how to use `identify` events.
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize, Default)]
pub struct Identify {
    /// The user associated with this message.
    #[serde(flatten)]
    pub user: User,

    /// The traits to assign to the user.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub traits: Value,

    /// Unique identifier used by Segment to deduplicate this message.
    #[serde(rename = "messageId", skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,

    /// The timestamp associated with this message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<Timestamp>,

    /// Context associated with this message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Value>,

    /// Integrations to route this message to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integrations: Option<Value>,

    /// Extra fields to put at the top level of this message.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl Identify {
    /// Construct a new `Identify` message with the given user and traits.
    pub fn new(user: User, traits: Value) -> Self {
        Self {
            user,
            traits,
            ..Default::default()
        }
    }
}

/// A track event.
///
/// See [Segment's documentation](https://segment.com/docs/spec/track/) for
/// how to use `track` events.
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize, Default)]
pub struct Track {
    /// The user associated with this message.
    #[serde(flatten)]
    pub user: User,

    /// The name of the event being tracked.
    pub event: String,

    /// The properties associated with the event.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub properties: Value,

    /// Unique identifier used by Segment to deduplicate this message.
    #[serde(rename = "messageId", skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,

    /// The timestamp associated with this message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<Timestamp>,

    /// Context associated with this message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Value>,

    /// Integrations to route this message to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integrations: Option<Value>,

    /// Extra fields to put at the top level of this message.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl Track {
    /// Construct a new `Track` message with the given user and event name.
    pub fn new(user: User, event: impl Into<String>) -> Self {
        Self {
            user,
            event: event.into(),
            properties: json!({}),
            ..Default::default()
        }
    }
}

/// A page event.
///
/// See [Segment's documentation](https://segment.com/docs/spec/page/) for how
/// to use `page` events.
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize, Default)]
pub struct Page {
    /// The user associated with this message.
    #[serde(flatten)]
    pub user: User,

    /// The name of the page being tracked.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,

    /// The properties associated with the event.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub properties: Value,

    /// Unique identifier used by Segment to deduplicate this message.
    #[serde(rename = "messageId", skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,

    /// The timestamp associated with this message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<Timestamp>,

    /// Context associated with this message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Value>,

    /// Integrations to route this message to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integrations: Option<Value>,

    /// Extra fields to put at the top level of this message.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl Page {
    /// Construct a new `Page` message with the given user and page name.
    pub fn new(user: User, name: impl Into<String>) -> Self {
        Self {
            user,
            name: name.into(),
            properties: json!({}),
            ..Default::default()
        }
    }
}

/// A screen event.
///
/// See [Segment's documentation](https://segment.com/docs/spec/screen/) for how
/// to use `screen` events.
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize, Default)]
pub struct Screen {
    /// The user associated with this message.
    #[serde(flatten)]
    pub user: User,

    /// The name of the screen being tracked.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,

    /// The properties associated with the event.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub properties: Value,

    /// Unique identifier used by Segment to deduplicate this message.
    #[serde(rename = "messageId", skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,

    /// The timestamp associated with this message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<Timestamp>,

    /// Context associated with this message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Value>,

    /// Integrations to route this message to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integrations: Option<Value>,

    /// Extra fields to put at the top level of this message.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl Screen {
    /// Construct a new `Screen` message with the given user and screen name.
    pub fn new(user: User, name: impl Into<String>) -> Self {
        Self {
            user,
            name: name.into(),
            properties: json!({}),
            ..Default::default()
        }
    }
}

/// A group event.
///
/// See [Segment's documentation](https://segment.com/docs/spec/group/) for how
/// to use `group` events.
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize, Default)]
pub struct Group {
    /// The user associated with this message.
    #[serde(flatten)]
    pub user: User,

    /// The group the user is being associated with.
    #[serde(rename = "groupId")]
    pub group_id: String,

    /// The traits to assign to the group.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub traits: Value,

    /// Unique identifier used by Segment to deduplicate this message.
    #[serde(rename = "messageId", skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,

    /// The timestamp associated with this message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<Timestamp>,

    /// Context associated with this message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Value>,

    /// Integrations to route this message to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integrations: Option<Value>,

    /// Extra fields to put at the top level of this message.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl Group {
    /// Construct a new `Group` message with the given user and group ID.
    pub fn new(user: User, group_id: impl Into<String>) -> Self {
        Self {
            user,
            group_id: group_id.into(),
            traits: json!({}),
            ..Default::default()
        }
    }
}

/// An alias event.
///
/// See [Segment's documentation](https://segment.com/docs/spec/alias/) for how
/// to use `alias` events.
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize, Default)]
pub struct Alias {
    /// The user associated with this message.
    #[serde(flatten)]
    pub user: User,

    /// The user's previous ID.
    #[serde(rename = "previousId")]
    pub previous_id: String,

    /// Unique identifier used by Segment to deduplicate this message.
    #[serde(rename = "messageId", skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,

    /// The timestamp associated with this message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<Timestamp>,

    /// Context associated with this message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Value>,

    /// Integrations to route this message to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integrations: Option<Value>,

    /// Extra fields to put at the top level of this message.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl Alias {
    /// Construct a new `Alias` message linking `previous_id` to `user_id`.
    pub fn new(user_id: impl Into<String>, previous_id: impl Into<String>) -> Self {
        Self {
            user: User::user_id(user_id),
            previous_id: previous_id.into(),
            ..Default::default()
        }
    }
}

/// A batch of events.
///
/// See [Segment's
/// documentation](https://segment.com/docs/sources/server/http/#batch) for how
/// to send batches of events.
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize, Default)]
pub struct Batch {
    /// The batch of messages to send.
    pub batch: Vec<BatchMessage>,

    /// Time at which this batch was sent to Segment.
    #[serde(
        rename = "sentAt",
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_sent_at"
    )]
    pub sent_at: Option<Timestamp>,

    /// Context associated with this message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Value>,

    /// Integrations to route this message to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integrations: Option<Value>,

    /// Extra fields to put at the top level of this message.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

fn serialize_sent_at<S>(
    timestamp: &Option<Timestamp>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match timestamp {
        Some(timestamp) => serializer.serialize_some(&fixed_precision_timestamp(timestamp)),
        None => serializer.serialize_none(),
    }
}

fn fixed_precision_timestamp(timestamp: &Timestamp) -> String {
    format!("{timestamp:.9}")
}

/// An enum containing all messages which may be placed inside a batch.
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum BatchMessage {
    #[serde(rename = "identify")]
    Identify(Identify),
    #[serde(rename = "track")]
    Track(Track),
    #[serde(rename = "page")]
    Page(Page),
    #[serde(rename = "screen")]
    Screen(Screen),
    #[serde(rename = "group")]
    Group(Group),
    #[serde(rename = "alias")]
    Alias(Alias),
}

impl BatchMessage {
    pub(crate) fn message_type(&self) -> &'static str {
        match self {
            Self::Identify(_) => "identify",
            Self::Track(_) => "track",
            Self::Page(_) => "page",
            Self::Screen(_) => "screen",
            Self::Group(_) => "group",
            Self::Alias(_) => "alias",
        }
    }

    pub(crate) fn message_id(&self) -> Option<&str> {
        match self {
            Self::Identify(message) => message.message_id.as_deref(),
            Self::Track(message) => message.message_id.as_deref(),
            Self::Page(message) => message.message_id.as_deref(),
            Self::Screen(message) => message.message_id.as_deref(),
            Self::Group(message) => message.message_id.as_deref(),
            Self::Alias(message) => message.message_id.as_deref(),
        }
    }

    /// Validate this message against the client-side portion of Segment's
    /// Tracking API contract.
    pub fn validate(&self) -> std::result::Result<(), ValidationError> {
        match self {
            Self::Identify(message) => validate_identify(message),
            Self::Track(message) => validate_track(message),
            Self::Page(message) => validate_page(message),
            Self::Screen(message) => validate_screen(message),
            Self::Group(message) => validate_group(message),
            Self::Alias(message) => validate_alias(message),
        }
    }

    /// Validate this message and populate any missing `messageId` and library
    /// context metadata.
    pub fn prepare(&mut self) -> Result<()> {
        self.validate()?;
        match self {
            Self::Identify(message) => {
                prepare_common(&mut message.message_id, &mut message.context)
            }
            Self::Track(message) => prepare_common(&mut message.message_id, &mut message.context),
            Self::Page(message) => prepare_common(&mut message.message_id, &mut message.context),
            Self::Screen(message) => prepare_common(&mut message.message_id, &mut message.context),
            Self::Group(message) => prepare_common(&mut message.message_id, &mut message.context),
            Self::Alias(message) => prepare_common(&mut message.message_id, &mut message.context),
        }
        Ok(())
    }

    pub(crate) fn is_prepared(&self) -> bool {
        let context = match self {
            Self::Identify(message) => message.context.as_ref(),
            Self::Track(message) => message.context.as_ref(),
            Self::Page(message) => message.context.as_ref(),
            Self::Screen(message) => message.context.as_ref(),
            Self::Group(message) => message.context.as_ref(),
            Self::Alias(message) => message.context.as_ref(),
        };
        self.message_id().is_some()
            && matches!(
                context,
                Some(Value::Object(map)) if map.get("library").is_some_and(Value::is_object)
            )
    }

    pub(crate) fn timestamp_mut(&mut self) -> &mut Option<Timestamp> {
        match self {
            Self::Identify(identify) => &mut identify.timestamp,
            Self::Track(track) => &mut track.timestamp,
            Self::Page(page) => &mut page.timestamp,
            Self::Screen(screen) => &mut screen.timestamp,
            Self::Group(group) => &mut group.timestamp,
            Self::Alias(alias) => &mut alias.timestamp,
        }
    }
}

fn validate_identify(message: &Identify) -> std::result::Result<(), ValidationError> {
    validate_user(&message.user, false)?;
    validate_object_or_null("traits", &message.traits)?;
    validate_common(
        message.message_id.as_deref(),
        message.context.as_ref(),
        message.integrations.as_ref(),
        &message.extra,
        &["traits"],
    )
}

fn validate_track(message: &Track) -> std::result::Result<(), ValidationError> {
    validate_user(&message.user, false)?;
    validate_non_empty("event", &message.event)?;
    validate_object_or_null("properties", &message.properties)?;
    validate_common(
        message.message_id.as_deref(),
        message.context.as_ref(),
        message.integrations.as_ref(),
        &message.extra,
        &["event", "properties"],
    )
}

fn validate_page(message: &Page) -> std::result::Result<(), ValidationError> {
    validate_user(&message.user, false)?;
    validate_object_or_null("properties", &message.properties)?;
    validate_common(
        message.message_id.as_deref(),
        message.context.as_ref(),
        message.integrations.as_ref(),
        &message.extra,
        &["name", "properties"],
    )
}

fn validate_screen(message: &Screen) -> std::result::Result<(), ValidationError> {
    validate_user(&message.user, false)?;
    validate_object_or_null("properties", &message.properties)?;
    validate_common(
        message.message_id.as_deref(),
        message.context.as_ref(),
        message.integrations.as_ref(),
        &message.extra,
        &["name", "properties"],
    )
}

fn validate_group(message: &Group) -> std::result::Result<(), ValidationError> {
    validate_user(&message.user, false)?;
    validate_non_empty("groupId", &message.group_id)?;
    validate_object_or_null("traits", &message.traits)?;
    validate_common(
        message.message_id.as_deref(),
        message.context.as_ref(),
        message.integrations.as_ref(),
        &message.extra,
        &["groupId", "traits"],
    )
}

fn validate_alias(message: &Alias) -> std::result::Result<(), ValidationError> {
    validate_user(&message.user, true)?;
    validate_non_empty("previousId", &message.previous_id)?;
    validate_common(
        message.message_id.as_deref(),
        message.context.as_ref(),
        message.integrations.as_ref(),
        &message.extra,
        &["previousId"],
    )
}

fn validate_batch(batch: &Batch) -> std::result::Result<(), ValidationError> {
    if batch.batch.is_empty() {
        return Err(ValidationError::new(
            "batch",
            "must contain at least one message",
        ));
    }
    if batch.batch.len() > MAX_BATCH_MESSAGES {
        return Err(ValidationError::new(
            "batch",
            format!("must contain no more than {MAX_BATCH_MESSAGES} messages"),
        ));
    }
    validate_optional_object("context", batch.context.as_ref())?;
    validate_optional_object("integrations", batch.integrations.as_ref())?;
    validate_extra_fields(
        &batch.extra,
        &["batch", "sentAt", "context", "integrations", "writeKey"],
    )?;
    for (index, message) in batch.batch.iter().enumerate() {
        message.validate().map_err(|error| {
            ValidationError::new(format!("batch[{index}].{}", error.field), error.message)
        })?;
    }
    Ok(())
}

fn validate_common(
    message_id: Option<&str>,
    context: Option<&Value>,
    integrations: Option<&Value>,
    extra: &Map<String, Value>,
    message_fields: &[&str],
) -> std::result::Result<(), ValidationError> {
    if let Some(message_id) = message_id {
        validate_non_empty("messageId", message_id)?;
        if message_id.chars().count() >= 100 {
            return Err(ValidationError::new(
                "messageId",
                "must contain fewer than 100 characters",
            ));
        }
    }
    validate_optional_object("context", context)?;
    validate_optional_object("integrations", integrations)?;

    let mut reserved = vec![
        "type",
        "userId",
        "anonymousId",
        "messageId",
        "timestamp",
        "context",
        "integrations",
        "writeKey",
    ];
    reserved.extend_from_slice(message_fields);
    validate_extra_fields(extra, &reserved)
}

fn validate_user(user: &User, require_user_id: bool) -> std::result::Result<(), ValidationError> {
    match user {
        User::UserId { user_id } => validate_non_empty("userId", user_id),
        User::AnonymousId { .. } if require_user_id => Err(ValidationError::new(
            "userId",
            "is required for alias messages",
        )),
        User::AnonymousId { anonymous_id } => validate_non_empty("anonymousId", anonymous_id),
        User::Both {
            user_id,
            anonymous_id,
        } => {
            validate_non_empty("userId", user_id)?;
            validate_non_empty("anonymousId", anonymous_id)
        }
    }
}

fn validate_non_empty(field: &str, value: &str) -> std::result::Result<(), ValidationError> {
    if value.trim().is_empty() {
        Err(ValidationError::new(field, "must not be empty"))
    } else {
        Ok(())
    }
}

fn validate_object_or_null(field: &str, value: &Value) -> std::result::Result<(), ValidationError> {
    if value.is_null() || value.is_object() {
        Ok(())
    } else {
        Err(ValidationError::new(field, "must be a JSON object"))
    }
}

fn validate_optional_object(
    field: &str,
    value: Option<&Value>,
) -> std::result::Result<(), ValidationError> {
    match value {
        None | Some(Value::Object(_)) => Ok(()),
        Some(_) => Err(ValidationError::new(field, "must be a JSON object")),
    }
}

fn validate_extra_fields(
    extra: &Map<String, Value>,
    reserved: &[&str],
) -> std::result::Result<(), ValidationError> {
    if let Some(field) = reserved.iter().find(|field| extra.contains_key(**field)) {
        Err(ValidationError::new(
            "extra",
            format!("must not override reserved field `{field}`"),
        ))
    } else {
        Ok(())
    }
}

fn prepare_common(message_id: &mut Option<String>, context: &mut Option<Value>) {
    message_id.get_or_insert_with(|| uuid::Uuid::new_v4().to_string());

    let library = json!({
        "name": "segment-rust",
        "version": env!("CARGO_PKG_VERSION"),
    });
    match context {
        Some(Value::Object(context)) => {
            context.insert("library".to_owned(), library);
        }
        None => {
            *context = Some(json!({ "library": library }));
        }
        Some(_) => unreachable!("context was validated before preparation"),
    }
}

/// User ID information.
///
/// All Segment tracking API calls require a user ID, an anonymous ID, or both.
/// See [Segment's
/// documentation](https://segment.com/docs/spec/identify/#identities) for how
/// user IDs and anonymous IDs should be used.
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum User {
    /// The user is identified only by a user ID.
    UserId {
        #[serde(rename = "userId")]
        user_id: String,
    },

    /// The user is identified only by an anonymous ID.
    AnonymousId {
        #[serde(rename = "anonymousId")]
        anonymous_id: String,
    },

    /// The user is identified by both a user ID and an anonymous ID.
    Both {
        #[serde(rename = "userId")]
        user_id: String,

        #[serde(rename = "anonymousId")]
        anonymous_id: String,
    },
}

impl User {
    /// Construct a `User` identified only by a `userId`.
    pub fn user_id(user_id: impl Into<String>) -> Self {
        Self::UserId {
            user_id: user_id.into(),
        }
    }

    /// Construct a `User` identified only by an `anonymousId`.
    pub fn anonymous_id(anonymous_id: impl Into<String>) -> Self {
        Self::AnonymousId {
            anonymous_id: anonymous_id.into(),
        }
    }

    /// Construct a `User` identified by both a `userId` and an `anonymousId`.
    pub fn both(user_id: impl Into<String>, anonymous_id: impl Into<String>) -> Self {
        Self::Both {
            user_id: user_id.into(),
            anonymous_id: anonymous_id.into(),
        }
    }
}

impl Display for User {
    /// Display a `UserId`. If he has both an `anonymous_id` and a `user_id` we display the `user_id`
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            User::UserId { user_id } => f.write_str(user_id),
            User::AnonymousId { anonymous_id } => f.write_str(anonymous_id),
            User::Both { user_id, .. } => f.write_str(user_id),
        }
    }
}

impl Default for User {
    fn default() -> Self {
        User::AnonymousId {
            anonymous_id: "".to_owned(),
        }
    }
}

macro_rules! into {
    (from $from:ident into $for:ident) => {
        impl From<$from> for $for {
            fn from(message: $from) -> Self {
                Self::$from(message)
            }
        }
    };
    ($(from $from:ident into $for:ident),+ $(,)?) => {
        $(
            into!{from $from into $for}
        )+
    };
}

into! {
    from Identify into Message,
    from Track into Message,
    from Page into Message,
    from Screen into Message,
    from Group into Message,
    from Alias into Message,
    from Batch into Message,

    from Identify into BatchMessage,
    from Track into BatchMessage,
    from Page into BatchMessage,
    from Screen into BatchMessage,
    from Group into BatchMessage,
    from Alias into BatchMessage,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn serialize() {
        assert_eq!(
            serde_json::to_string(&Message::Identify(Identify {
                user: User::UserId {
                    user_id: "foo".to_owned()
                },
                traits: json!({
                    "foo": "bar",
                    "baz": "quux",
                }),
                message_id: Some("123".to_owned()),
                ..Default::default()
            }))
            .unwrap(),
            r#"{"userId":"foo","traits":{"baz":"quux","foo":"bar"},"messageId":"123"}"#.to_owned(),
        );

        assert_eq!(
            serde_json::to_string(&Message::Track(Track {
                user: User::AnonymousId {
                    anonymous_id: "foo".to_owned()
                },
                event: "Foo".to_owned(),
                properties: json!({
                    "foo": "bar",
                    "baz": "quux",
                }),
                ..Default::default()
            }))
            .unwrap(),
            r#"{"anonymousId":"foo","event":"Foo","properties":{"baz":"quux","foo":"bar"}}"#
                .to_owned(),
        );

        assert_eq!(
            serde_json::to_string(&Message::Page(Page {
                user: User::Both {
                    user_id: "foo".to_owned(),
                    anonymous_id: "bar".to_owned()
                },
                name: "Foo".to_owned(),
                properties: json!({
                    "foo": "bar",
                    "baz": "quux",
                }),
                ..Default::default()
            }))
            .unwrap(),
            r#"{"userId":"foo","anonymousId":"bar","name":"Foo","properties":{"baz":"quux","foo":"bar"}}"#
                .to_owned(),
        );

        assert_eq!(
            serde_json::to_string(&Message::Screen(Screen {
                user: User::Both {
                    user_id: "foo".to_owned(),
                    anonymous_id: "bar".to_owned()
                },
                name: "Foo".to_owned(),
                properties: json!({
                    "foo": "bar",
                    "baz": "quux",
                }),
                ..Default::default()
            }))
            .unwrap(),
            r#"{"userId":"foo","anonymousId":"bar","name":"Foo","properties":{"baz":"quux","foo":"bar"}}"#
                .to_owned(),
        );

        assert_eq!(
            serde_json::to_string(&Message::Group(Group {
                user: User::UserId {
                    user_id: "foo".to_owned()
                },
                group_id: "bar".to_owned(),
                traits: json!({
                    "foo": "bar",
                    "baz": "quux",
                }),
                ..Default::default()
            }))
            .unwrap(),
            r#"{"userId":"foo","groupId":"bar","traits":{"baz":"quux","foo":"bar"}}"#.to_owned(),
        );

        assert_eq!(
            serde_json::to_string(&Message::Alias(Alias {
                user: User::UserId {
                    user_id: "foo".to_owned()
                },
                previous_id: "bar".to_owned(),
                ..Default::default()
            }))
            .unwrap(),
            r#"{"userId":"foo","previousId":"bar"}"#.to_owned(),
        );

        assert_eq!(
            serde_json::to_string(&Message::Batch(Batch {
                batch: vec![
                    BatchMessage::Track(Track {
                        user: User::UserId {
                            user_id: "foo".to_owned()
                        },
                        event: "Foo".to_owned(),
                        properties: json!({}),
                        ..Default::default()
                    }),
                    BatchMessage::Track(Track {
                        user: User::UserId {
                            user_id: "bar".to_owned()
                        },
                        event: "Bar".to_owned(),
                        properties: json!({}),
                        ..Default::default()
                    }),
                    BatchMessage::Track(Track {
                        user: User::UserId {
                            user_id: "baz".to_owned()
                        },
                        event: "Baz".to_owned(),
                        properties: json!({}),
                        ..Default::default()
                    })
                ],
                context: Some(json!({
                    "foo": "bar",
                })),
                ..Default::default()
            }))
            .unwrap(),
            r#"{"batch":[{"type":"track","userId":"foo","event":"Foo","properties":{}},{"type":"track","userId":"bar","event":"Bar","properties":{}},{"type":"track","userId":"baz","event":"Baz","properties":{}}],"context":{"foo":"bar"}}"#
            .to_owned(),
        );
    }

    #[test]
    fn deserialize_without_timestamp() {
        let track: Track = serde_json::from_value(json!({
            "anonymousId": "foo",
            "event": "Foo",
            "properties": {}
        }))
        .expect("timestamp should be optional");

        assert_eq!(track.timestamp, None);
    }

    #[test]
    fn timestamp_serializes_as_utc() {
        let track: Track = serde_json::from_value(json!({
            "anonymousId": "foo",
            "event": "Foo",
            "properties": {},
            "timestamp": "2024-01-01T00:00:00-05:00"
        }))
        .expect("timestamp with an offset should deserialize");

        let serialized = serde_json::to_value(track).expect("track should serialize");
        assert_eq!(serialized["timestamp"], "2024-01-01T05:00:00Z");
    }

    #[test]
    fn validation_rejects_invalid_required_fields_and_shapes() {
        let empty_identity = Track {
            event: "Example".to_owned(),
            properties: json!({}),
            ..Default::default()
        };
        assert_eq!(
            Message::from(empty_identity)
                .validate()
                .expect_err("empty identity should be rejected")
                .field,
            "anonymousId"
        );

        let invalid_properties = Track {
            user: User::UserId {
                user_id: "user".to_owned(),
            },
            event: "Example".to_owned(),
            properties: json!(["not", "an", "object"]),
            ..Default::default()
        };
        assert_eq!(
            Message::from(invalid_properties)
                .validate()
                .expect_err("array properties should be rejected")
                .field,
            "properties"
        );

        let alias = Alias {
            user: User::AnonymousId {
                anonymous_id: "anonymous".to_owned(),
            },
            previous_id: "previous".to_owned(),
            ..Default::default()
        };
        assert_eq!(
            Message::from(alias)
                .validate()
                .expect_err("alias requires a user ID")
                .field,
            "userId"
        );
    }

    #[test]
    fn validation_rejects_reserved_extra_fields() {
        let track = Track {
            user: User::UserId {
                user_id: "user".to_owned(),
            },
            event: "Example".to_owned(),
            properties: json!({}),
            extra: [("event".to_owned(), json!("Override"))]
                .into_iter()
                .collect(),
            ..Default::default()
        };

        let error = Message::from(track)
            .validate()
            .expect_err("reserved field collision should be rejected");
        assert_eq!(error.field, "extra");
    }

    #[test]
    fn preparation_adds_stable_id_and_library_context() {
        let mut message = Message::from(Track {
            user: User::UserId {
                user_id: "user".to_owned(),
            },
            event: "Example".to_owned(),
            properties: json!({}),
            context: Some(json!({ "requestId": "request" })),
            ..Default::default()
        });

        message.prepare().expect("valid message should prepare");
        let Message::Track(track) = &message else {
            panic!("expected track message");
        };
        let message_id = track.message_id.clone().expect("ID should be generated");
        assert!(uuid::Uuid::parse_str(&message_id).is_ok());
        assert_eq!(track.context.as_ref().unwrap()["requestId"], "request");
        assert_eq!(
            track.context.as_ref().unwrap()["library"]["name"],
            "segment-rust"
        );

        message.prepare().expect("preparation should be idempotent");
        let Message::Track(track) = message else {
            panic!("expected track message");
        };
        assert_eq!(track.message_id.as_deref(), Some(message_id.as_str()));
    }

    #[test]
    fn batch_preparation_adds_sent_at_and_nested_metadata() {
        let mut message = Message::from(Batch {
            batch: vec![BatchMessage::from(Track {
                user: User::UserId {
                    user_id: "user".to_owned(),
                },
                event: "Example".to_owned(),
                properties: json!({}),
                ..Default::default()
            })],
            ..Default::default()
        });

        message.prepare().expect("valid batch should prepare");
        let Message::Batch(batch) = message else {
            panic!("expected batch message");
        };
        assert!(batch.sent_at.is_some());
        let BatchMessage::Track(track) = &batch.batch[0] else {
            panic!("expected track message");
        };
        assert!(track.message_id.is_some());
    }

    #[test]
    fn message_deserialization_uses_type_to_distinguish_page_and_screen() {
        let screen: Message = serde_json::from_value(json!({
            "type": "screen",
            "anonymousId": "anonymous",
            "name": "Home",
            "properties": {}
        }))
        .expect("typed screen should deserialize");
        assert!(matches!(screen, Message::Screen(_)));

        let ambiguous = serde_json::from_value::<Message>(json!({
            "anonymousId": "anonymous",
            "name": "Home",
            "properties": {}
        }))
        .expect_err("untyped page/screen payload should be rejected");
        assert!(ambiguous.to_string().contains("ambiguous"));
    }

    #[test]
    fn fixed_precision_timestamp_formats_nine_nanosecond_digits() {
        let whole = Timestamp::from_second(1_700_000_000).unwrap();
        assert_eq!(
            fixed_precision_timestamp(&whole),
            "2023-11-14T22:13:20.000000000Z"
        );

        let fractional = Timestamp::new(1_700_000_000, 123_450_000).unwrap();
        assert_eq!(
            fixed_precision_timestamp(&fractional),
            "2023-11-14T22:13:20.123450000Z"
        );
    }

    #[test]
    fn ergonomic_constructors_produce_valid_messages() {
        let uid = User::user_id("u1");
        assert_eq!(
            uid,
            User::UserId {
                user_id: "u1".to_owned()
            }
        );
        let anon = User::anonymous_id("a1");
        assert_eq!(
            anon,
            User::AnonymousId {
                anonymous_id: "a1".to_owned()
            }
        );
        let both = User::both("u1", "a1");
        assert_eq!(
            both,
            User::Both {
                user_id: "u1".to_owned(),
                anonymous_id: "a1".to_owned()
            }
        );

        let track = Track::new(User::user_id("u1"), "Signed Up");
        assert_eq!(track.event, "Signed Up");
        assert_eq!(track.properties, json!({}));
        Message::from(track).validate().unwrap();

        let identify = Identify::new(User::user_id("u1"), json!({ "plan": "pro" }));
        assert_eq!(identify.traits, json!({ "plan": "pro" }));
        Message::from(identify).validate().unwrap();

        let page = Page::new(User::anonymous_id("a1"), "Pricing");
        assert_eq!(page.name, "Pricing");
        assert_eq!(page.properties, json!({}));
        Message::from(page).validate().unwrap();

        let screen = Screen::new(User::anonymous_id("a1"), "Home");
        assert_eq!(screen.name, "Home");
        assert_eq!(screen.properties, json!({}));
        Message::from(screen).validate().unwrap();

        let group = Group::new(User::user_id("u1"), "g1");
        assert_eq!(group.group_id, "g1");
        assert_eq!(group.traits, json!({}));
        Message::from(group).validate().unwrap();

        let alias = Alias::new("u1", "a1");
        assert_eq!(alias.user, User::user_id("u1"));
        assert_eq!(alias.previous_id, "a1");
        Message::from(alias).validate().unwrap();
    }
}
