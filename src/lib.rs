
mod event_streams;
pub mod neo4j;

pub use event_streams::*;

struct TestAcceptor;
struct TestAcceptorEvent;
#[derive(Clone)]
struct TestAcceptorState;

impl EventStream for TestAcceptor {
    type OwnEvent = TestAcceptorEvent;
    type State = TestAcceptorState;
    fn get_generation() -> u32 { 0 }
    fn reducer(state: &Self::State, event: &DomainEvent<Self::OwnEvent>) -> Self::State {
        state.clone()
    }
    fn get_name() -> std::string::String { todo!() }
}
struct TestProvider;
impl StreamProvider for TestProvider {
    fn get_last_state<TStream>(&self, _: &event_streams::EntityRef) -> std::result::Result<<TStream as event_streams::EventStream>::State, ()> where TStream: event_streams::EventStream { todo!() }
    fn get_events_since_last_checkpoint<TStream>(&self, _: &event_streams::EntityRef) -> std::boxed::Box<(dyn std::iter::Iterator<Item = event_streams::DomainEvent<<TStream as event_streams::EventStream>::OwnEvent>> + 'static)> where TStream: event_streams::EventStream { todo!() }
    fn sink_event<TStream>(&self, _: &event_streams::EntityRef, _: &event_streams::DomainEvent<<TStream as event_streams::EventStream>::OwnEvent>, _: &<TStream as event_streams::EventStream>::State) where TStream: event_streams::EventStream { todo!() }
}

fn createEventGraphConnector() -> StreamConnector<TestProvider> {
    StreamConnector::new(TestProvider)
}

fn example() {
    let connector = createEventGraphConnector();
    let evt = TestAcceptorEvent;
}