use std::time::Instant;

pub struct UserRef(String);
#[derive(Clone)]
pub struct EntityRef(String);
pub struct EventGeneration(u32);

pub struct DomainEvent<T> {
  entity_id: EntityRef,
  timestamp: Instant,
  event: T,
}
impl<T> DomainEvent<T> {
  pub fn wrap(entity_id: &EntityRef, event: T) -> DomainEvent<T> {
    DomainEvent {
      entity_id: entity_id.clone(),
      timestamp: Instant::now(),
      event,
    }
  }
  pub fn get_timestamp(&self) -> Instant {
    self.timestamp
  }
  pub fn get_event(&self) -> &T {
    &self.event
  }
  pub fn get_entity_id(&self) -> &EntityRef {
    &self.entity_id
  }
}

pub trait ProcessorEventStream: EventStream {
  type InputEvent;
  fn processor(state: &Self::State, event: &DomainEvent<Self::InputEvent>) -> Self::OwnEvent;
}

pub trait EventStream {
  type OwnEvent;
  type State;
  fn get_name() -> String;
  fn get_generation() -> u32;
  fn reducer(state: &Self::State, event: &DomainEvent<Self::OwnEvent>) -> Self::State;
}

pub trait StreamAcceptor<TState, TEvent> {
  fn try_accept_event(&self, entity_id: &EntityRef, f: dyn FnOnce(&TState) -> Option<TEvent>);
}

pub trait StreamProcessor<TState, TInputEvent, TOwnEvent> {
  fn process_event(&self, evt: &DomainEvent<TInputEvent>);
}

pub trait StreamProvider {
  fn get_last_state<TStream: EventStream>(&self, id: &EntityRef) -> Result<TStream::State, ()>;
  fn get_events_since_last_checkpoint<TStream: EventStream>(
    &self,
    id: &EntityRef,
  ) -> Box<dyn Iterator<Item = DomainEvent<TStream::OwnEvent>>>;
  fn sink_event<TStream: EventStream>(
    &self,
    id: &EntityRef,
    event: &DomainEvent<TStream::OwnEvent>,
    state: &TStream::State,
  );
}

pub struct StreamConnector<T>{
  provider: T,
}

impl<T> StreamConnector<T> where T: StreamProvider {
  pub fn new(provider: T) -> StreamConnector<T> {
    StreamConnector { provider }
  }
  fn get_provider(&self) -> &T {
    &self.provider
  }

  pub fn get_state<TStream>(&self, entity_id: &EntityRef) -> TStream::State
  where
    TStream::State: Default,
    TStream: EventStream,
  {
    self
      .get_provider()
      .get_last_state::<TStream>(entity_id)
      .unwrap_or_else(|_| {
        self
          .get_provider()
          .get_events_since_last_checkpoint::<TStream>(entity_id)
          .fold(TStream::State::default(), |b, e| TStream::reducer(&b, &e))
      })
  }

  pub fn try_accept_event<TStream, F>(&self, entity_id: &EntityRef, f: F)
  where
    TStream: EventStream,
    TStream::State: Default,
    F: FnOnce(&TStream::State) -> Option<TStream::OwnEvent>,
  {
    let state = self.get_state::<TStream>(entity_id);

    if let Some(new_event) = f(&state) {
      let new_event = DomainEvent::wrap(entity_id, new_event);
      let new_state = TStream::reducer(&state, &new_event);

      // write new event with state atomically
      // if not done atomically, the latest state will still be from
      // the previous event
      self
        .get_provider()
        .sink_event::<TStream>(entity_id, &new_event, &new_state);
    }
  }

  pub fn process_event<TStream>(&self, evt: &DomainEvent<TStream::InputEvent>)
  where
    TStream: ProcessorEventStream,
    TStream::State: Default,
  {
    let state = self.get_state::<TStream>(&evt.entity_id);

    let new_event = DomainEvent::wrap(&evt.entity_id, TStream::processor(&state, &evt));
    let new_state = TStream::reducer(&state, &new_event);

    // write new event with state atomically
    // if not done atomically, the latest state will still be from
    // the previous event
    self.get_provider().sink_event::<TStream>(&evt.entity_id, &new_event, &new_state);
  }
}
