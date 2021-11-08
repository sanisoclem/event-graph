use async_trait::async_trait;
use futures::{pin_mut, select, FutureExt};
use serde::{Deserialize, Serialize};

// TODO: make async!!

// TODO: how can I not repeat the *Ref code?
// maybe implementing Deref, Default and From should be enough?
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub struct EntityRef(String);

impl EntityRef {
  pub fn to_str(&self) -> &str {
    &self.0
  }

  pub fn from_str(s: &str) -> Self {
    EntityRef(s.to_string())
  }

  pub fn generate() -> Self {
    EntityRef(format!("ix{}", uuid::Uuid::new_v4().to_string()))
  }
}

impl std::fmt::Display for EntityRef {
  fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
    write!(f, "{}", self.0)
  }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub struct EventRef(String);

impl EventRef {
  pub fn to_str(&self) -> &str {
    &self.0
  }

  pub fn from_str(s: &str) -> Self {
    EventRef(s.to_string())
  }

  pub fn generate() -> Self {
    EventRef(format!("ex{}", uuid::Uuid::new_v4().to_string()))
  }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub struct StateRef(String);

impl StateRef {
  pub fn to_str(&self) -> &str {
    &self.0
  }

  pub fn from_str(s: &str) -> Self {
    StateRef(s.to_string())
  }

  pub fn generate() -> Self {
    StateRef(format!("sx{}", uuid::Uuid::new_v4().to_string()))
  }
}

#[derive(Debug)]
pub struct DomainEvent<T> {
  pub id: EventRef,
  pub entity_id: EntityRef,
  pub hydrated_by: Option<StateRef>,
  pub preceded_by: Option<EventRef>,
  pub timestamp: u64,
  pub event: T,
}
impl<T> DomainEvent<T> {
  pub fn map<F, U>(self: Self, f: F) -> DomainEvent<U>
  where
    F: FnOnce(T) -> U,
  {
    DomainEvent {
      id: self.id,
      hydrated_by: self.hydrated_by,
      preceded_by: self.preceded_by,
      entity_id: self.entity_id,
      timestamp: 0,
      event: f(self.event),
    }
  }
  pub fn generate_new(
    entity_id: &EntityRef,
    hydrated_by: &Option<StateRef>,
    preceded_by: &Option<EventRef>,
    event: T,
  ) -> DomainEvent<T> {
    DomainEvent {
      id: EventRef::generate(),
      hydrated_by: hydrated_by.clone(),
      preceded_by: preceded_by.clone(),
      entity_id: entity_id.clone(),
      timestamp: 0,
      event,
    }
  }
}

#[derive(Debug)]
pub enum DomainState<T> {
  Persisted(PersistedDomainState<T>),
  Initial(T),
}
impl<T> DomainState<T> {
  pub fn get_inner(&self) -> &T {
    match self {
      DomainState::Persisted(PersistedDomainState { data, .. }) => data,
      DomainState::Initial(data) => data,
    }
  }
  pub fn into_inner(self) -> T {
    match self {
      DomainState::Persisted(PersistedDomainState { data, .. }) => data,
      DomainState::Initial(data) => data,
    }
  }
}

#[derive(Debug)]
pub struct PersistedDomainState<T> {
  pub id: StateRef,
  pub entity_id: EntityRef,
  pub generated_by: EventRef,
  pub preceded_by: Option<StateRef>,
  pub data: T,
}
impl<T> PersistedDomainState<T> {
  pub fn generate_new(
    entity_id: &EntityRef,
    generated_by: &EventRef,
    preceded_by: &Option<StateRef>,
    data: T,
  ) -> PersistedDomainState<T> {
    PersistedDomainState {
      id: StateRef::generate(),
      generated_by: generated_by.clone(),
      preceded_by: preceded_by.clone(),
      entity_id: entity_id.clone(),
      data,
    }
  }
}

pub trait ProcessorEventStream: StatefulEventStream {
  type InputStream: EventStream;

  fn map_entity_id(src_id: &EntityRef) -> EntityRef;
  fn processor(
    state: &DomainState<Self::State>,
    event: DomainEvent<<Self::InputStream as EventStream>::OwnEvent>,
  ) -> Self::OwnEvent;
}
pub trait ProcessorEventStream2: StatefulEventStream {
  type InputStream1: EventStream;
  type InputStream2: EventStream;
  type InputEvent: From<<Self::InputStream1 as EventStream>::OwnEvent>
    + From<<Self::InputStream2 as EventStream>::OwnEvent>;

  fn map_entity_id1(src_id: &EntityRef) -> EntityRef;
  fn map_entity_id2(src_id: &EntityRef) -> EntityRef;
  fn processor(
    state: &DomainState<Self::State>,
    event: DomainEvent<Self::InputEvent>,
  ) -> Self::OwnEvent;
}

pub trait EventStream {
  type OwnEvent;
  fn get_name() -> &'static str;
}

pub trait StatefulEventStream: EventStream {
  type State;

  // NOTE: don't wrap state and event - decisions shouldn't be made based on the wrapper
  // because this will make optimisation difficult later since unfolding is not guaranteed to work
  // since we will only unfold to the inner type
  //
  // EXAMPLE: if we have a `created_at` field in our state and we use `DomainEvent.timestamp` to
  // populate it, we cannot create a function `unfold(TState) -> Vec<TEvent>` that returns a list of events that
  // will lead to the same state because we do not return the wrapper `DomainEvent<T>` type.
  fn reducer(state: Self::State, event: &Self::OwnEvent) -> Self::State;
}

pub trait StreamQueryProvider<TQuery, TState> {
  type Error;
  fn get_states(
    &self,
    stream_name: &str,
    query: TQuery,
  ) -> Result<Vec<PersistedDomainState<TState>>, Self::Error>;
}

#[async_trait]
pub trait StreamEventProvider<TEvent> {
  type Error;
  // TODO: define this, what is a checkpoint?
  fn get_events_since_last_checkpoint(
    &self,
    stream_name: &str,
    id: &EntityRef,
  ) -> Result<Vec<DomainEvent<TEvent>>, Self::Error>;
  async fn wait_for_event(&self, stream_name: &str) -> Result<DomainEvent<TEvent>, Self::Error>;
}

pub trait StreamProvider<TState, TEvent>: StreamEventProvider<TEvent> {
  // return the last state for the current version or None if no state, or state is old
  fn get_last_state(
    &self,
    stream_name: &str,
    id: &EntityRef,
  ) -> Result<Option<PersistedDomainState<TState>>, Self::Error>;

  // saves an event and state linked to event
  // fails if the state/event is not the latest state for this entity_id and state version
  fn sink_event(
    &self,
    stream_name: &str,
    id: &EntityRef,
    event: &DomainEvent<TEvent>,
    state: &PersistedDomainState<TState>,
  ) -> Result<(), Self::Error>;

  // saves a state
  // fails if this is not the latest state for this entity_id and state version
  fn sink_state(
    &self,
    stream_name: &str,
    id: &EntityRef,
    state: &PersistedDomainState<TState>,
  ) -> Result<(), Self::Error>;
}

pub struct StreamConnector<TProvider> {
  stream_provider: TProvider,
}

impl<TProvider> StreamConnector<TProvider> {
  pub fn new(stream_provider: TProvider) -> Self {
    StreamConnector { stream_provider }
  }

  pub fn get_states<TQuery, TStream>(
    &self,
    query: TQuery,
  ) -> Result<Vec<PersistedDomainState<TStream::State>>, TProvider::Error>
  where
    TStream: StatefulEventStream,
    TProvider: StreamQueryProvider<TQuery, TStream::State>,
  {
    let stream_name = TStream::get_name();
    self.stream_provider.get_states(stream_name, query)
  }

  pub fn get_state<TStream>(
    &self,
    entity_id: &EntityRef,
  ) -> Result<DomainState<TStream::State>, TProvider::Error>
  where
    TStream: StatefulEventStream,
    TProvider: StreamProvider<TStream::State, TStream::OwnEvent>,
    TStream::State: Default,
  {
    let stream_name = TStream::get_name();
    if let Some(state) = self
      .stream_provider
      .get_last_state(stream_name, entity_id)?
    {
      // TODO: check if further rehydration is required
      // this can be done by geting all events that succeeds
      // the event that generated this state
      Ok(DomainState::Persisted(state))
    } else {
      // NOTE: this should also be able to handle scenarios where get_last_state returns None
      // but there is outdated state (if the hydration fails part way) where we need to start from
      // instead of starting from the initial state
      let mut state = DomainState::Initial(TStream::State::default());
      for evt in self
        .stream_provider
        .get_events_since_last_checkpoint(stream_name, entity_id)?
      {
        let prev_state_id = match &state {
          DomainState::Persisted(s) => Some(s.id.clone()),
          _ => None,
        };

        let new_state = PersistedDomainState::generate_new(
          entity_id,
          &evt.id,
          &prev_state_id,
          TStream::reducer(state.into_inner(), &evt.event),
        );

        self
          .stream_provider
          .sink_state(stream_name, entity_id, &new_state)?;
        state = DomainState::Persisted(new_state);
      }
      Ok(state)
    }
  }

  pub fn try_accept_event<TStream, F, E>(&self, entity_id: &EntityRef, f: F) -> Result<(), E>
  where
    TStream: StatefulEventStream,
    TProvider: StreamProvider<TStream::State, TStream::OwnEvent>,
    TStream::State: Default,
    E: From<TProvider::Error>,
    F: FnOnce(&DomainState<TStream::State>) -> Result<TStream::OwnEvent, E>,
  {
    let stream_name = TStream::get_name();
    let state = self.get_state::<TStream>(entity_id)?;
    let new_event = f(&state)?;

    let persisted_state = match &state {
      DomainState::Persisted(s) => Some(s),
      DomainState::Initial(_) => None,
    };

    let prev_state_id = persisted_state.map(|s| s.id.clone());
    let prev_event_id = persisted_state.map(|s| s.generated_by.clone());

    let new_event = DomainEvent::generate_new(entity_id, &prev_state_id, &prev_event_id, new_event);
    let new_state = PersistedDomainState::generate_new(
      entity_id,
      &new_event.id,
      &prev_state_id,
      TStream::reducer(state.into_inner(), &new_event.event),
    );

    // write new event with state atomically
    // if not done atomically, the latest state will still be from
    // the previous event
    self
      .stream_provider
      .sink_event(stream_name, entity_id, &new_event, &new_state)?;
    Ok(())
  }

  pub async fn process_event<TStream>(
    &self,
  ) -> Result<(), <TProvider as StreamEventProvider<TStream::OwnEvent>>::Error>
  where
    TStream: ProcessorEventStream,
    TProvider: StreamProvider<TStream::State, TStream::OwnEvent>
      + StreamEventProvider<
        <TStream::InputStream as EventStream>::OwnEvent,
        Error = <TProvider as StreamEventProvider<TStream::OwnEvent>>::Error,
      >,
    TStream::State: Default,
  {
    let input_stream_name = TStream::InputStream::get_name();

    let evt = self
      .stream_provider
      .wait_for_event(input_stream_name)
      .await?;
    let entity_id = TStream::map_entity_id(&evt.entity_id);

    let stream_name = TStream::get_name();
    let state = self.get_state::<TStream>(&entity_id)?;

    let persisted_state = match &state {
      DomainState::Persisted(s) => Some(s),
      DomainState::Initial(_) => None,
    };
    let prev_state_id = persisted_state.map(|s| s.id.clone());
    let prev_event_id = persisted_state.map(|s| s.generated_by.clone());

    let new_event = DomainEvent::generate_new(
      &entity_id,
      &prev_state_id,
      &prev_event_id,
      TStream::processor(&state, evt),
    );
    let new_state = PersistedDomainState::generate_new(
      &entity_id,
      &new_event.id,
      &prev_state_id,
      TStream::reducer(state.into_inner(), &new_event.event),
    );

    // write new event with state atomically
    // if not done atomically, the latest state will still be from
    // the previous event
    self
      .stream_provider
      .sink_event(stream_name, &entity_id, &new_event, &new_state)?;

    Ok(())
  }

  pub async fn process_event2<TStream>(
    &self,
  ) -> Result<(), <TProvider as StreamEventProvider<TStream::OwnEvent>>::Error>
  where
    TStream: ProcessorEventStream2,
    TProvider: StreamProvider<TStream::State, TStream::OwnEvent>
      + StreamEventProvider<
        <TStream::InputStream1 as EventStream>::OwnEvent,
        Error = <TProvider as StreamEventProvider<TStream::OwnEvent>>::Error,
      > + StreamEventProvider<
        <TStream::InputStream2 as EventStream>::OwnEvent,
        Error = <TProvider as StreamEventProvider<TStream::OwnEvent>>::Error,
      >,
    TStream::State: Default,
  {
    let input_stream_name1 = TStream::InputStream1::get_name();
    let input_stream_name2 = TStream::InputStream2::get_name();

    let evt_wait_1 =
      StreamEventProvider::<<TStream::InputStream1 as EventStream>::OwnEvent>::wait_for_event(
        &self.stream_provider,
        input_stream_name1,
      )
      .fuse();
    let evt_wait_2 =
      StreamEventProvider::<<TStream::InputStream2 as EventStream>::OwnEvent>::wait_for_event(
        &self.stream_provider,
        input_stream_name2,
      )
      .fuse();

    pin_mut!(evt_wait_1, evt_wait_2);

    let evt: DomainEvent<TStream::InputEvent> = select! {
      evt1 = evt_wait_1 => evt1?.map(|e|e.into()),
      evt2 = evt_wait_2 => evt2?.map(|e|e.into()),
    };

    let entity_id = evt.entity_id.clone();
    let stream_name = TStream::get_name();
    let state = self.get_state::<TStream>(&entity_id)?;

    let persisted_state = match &state {
      DomainState::Persisted(s) => Some(s),
      DomainState::Initial(_) => None,
    };
    let prev_state_id = persisted_state.map(|s| s.id.clone());
    let prev_event_id = persisted_state.map(|s| s.generated_by.clone());

    let new_event = DomainEvent::generate_new(
      &entity_id,
      &prev_state_id,
      &prev_event_id,
      TStream::processor(&state, evt),
    );
    let new_state = PersistedDomainState::generate_new(
      &entity_id,
      &new_event.id,
      &prev_state_id,
      TStream::reducer(state.into_inner(), &new_event.event),
    );

    // write new event with state atomically
    // if not done atomically, the latest state will still be from
    // the previous event
    self
      .stream_provider
      .sink_event(stream_name, &entity_id, &new_event, &new_state)?;

    Ok(())
  }
}
