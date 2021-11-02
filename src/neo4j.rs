// eventing using neo4j backend

use crate::{DomainEvent, EntityRef, EventStream, StreamProvider};
use neo4rs::{query, Graph};
use rusted_cypher::*;
use std::sync::Arc;

pub struct EventGraphProvider;

impl StreamProvider for EventGraphProvider {
  fn get_last_state<TStream>(
    &self,
    _: &EntityRef,
  ) -> std::result::Result<<TStream as EventStream>::State, ()>
  where
    TStream: EventStream,
  {
    // get state that hasn't caused an event yet, this is the tip
    // MATCH (s: StreamState { stream: $stream, entity_id: $entity_id }) WHERE NOT (s)-[:CAUSES]->(:StreamEvent) RETURN s
    todo!()
  }
  fn get_events_since_last_checkpoint<TStream>(
    &self,
    _: &EntityRef,
  ) -> std::boxed::Box<
    (dyn std::iter::Iterator<Item = DomainEvent<<TStream as EventStream>::OwnEvent>> + 'static),
  >
  where
    TStream: EventStream,
  {
    // get all events since the last checkpoint, we might have to batch this (i.e., *1..n)
    // MATCH (s: Checkpoint { stream: $stream, entity_id: $entity_id })-[:PRECEDES *1..]->(e: StreamEvent) RETURN e
    todo!()
  }
  fn sink_event<TStream>(
    &self,
    _: &EntityRef,
    _: &DomainEvent<<TStream as EventStream>::OwnEvent>,
    _: &<TStream as EventStream>::State,
  ) where
    TStream: EventStream,
  {
    // no idea yet
    // this has to create the event and state
    // it has to ensure that no new state has been written since it was last checked
    // in other words, the tree should not branch out
    // just curious, if it does branch out, is there anyway to resolve it?
    todo!()
  }
}

pub async fn test() -> Result<(), neo4rs::Error> {
  // concurrent queries
  let uri = "c606bab2.databases.neo4j.io:7687";
  let user = "neo4j";
  let pass = "zQP6CnSIkBKuS_0xYE6a_xVx2m7tlPPjIrJOvXEx_5g";
  let graph = Arc::new(Graph::new(&uri, user, pass).await?);

  //Transactions
  let txn = graph.start_txn().await?;
  txn.run_queries(vec![
      query("CREATE (e1:TestEventStream { entity_id: '00001', offset: 0, timestamp: date(), start: true})-[:PRECEDES]->(e2:TestEventStream { entity_id: '00001', offset: 1, timestamp: date(), eventData: 'aaaaa'})-[:FOLDSTO]->(s1: TestEventStreamState { entity_id: '00001', state: 1}) RETURN e1, e2, s1"),
      query("MATCH (e1:TestEventStream {entity_id:'00001', offset: 1})-[:FOLDSTO]->(s1:TestEventStreamState { entity_id: '00001' }) CREATE (e1)-[:PRECEDES]->(e2:TestEventStream { entity_id: '00001', offset: 2, timestamp: date(), eventData: 'bbbbb'})<-[:FEED]-(s1), (e2)-[:FOLDSTO]->(s2:TestEventStreamState { entity_id: '00001', state: 1}) RETURN e1,e2,s1,s2"),
      query("MATCH (e2:TestEventStream {entity_id:'00001', offset: 2})-[:FOLDSTO]->(s2:TestEventStreamState { entity_id: '00001' }) CREATE (e2)-[:PRECEDES]->(e3:TestEventStream { entity_id: '00001', offset: 3, timestamp: date(), eventData: 'ccccc'})<-[:FEED]-(s2), (e3)-[:FOLDSTO]->(s3:TestEventStreamState { entity_id: '00001', state: 2}) RETURN e2,e3,s2,s3"),
  ])
  .await?;
  txn.commit().await
}

pub fn test2() -> Result<(), GraphError> {
  let graph = GraphClient::connect("http://neo4j:tae@localhost:7474/db/data")?;

  let mut query = graph.query();

  // Statement implements From<&str>
  query.add_statement("CREATE (n:LANG { name: 'Rust', level: 'low', safe: true })");

  let statement = Statement::new("CREATE (n:LANG { name: 'C++', level: 'low', safe: {safeness} })")
    .with_param("safeness", false)?;

  query.add_statement(statement);

  query.send()?;

  graph.exec("CREATE (n:LANG { name: 'Python', level: 'high', safe: true })")?;

  let result = graph.exec("MATCH (n:LANG) RETURN n.name, n.level, n.safe")?;

  assert_eq!(result.data.len(), 3);

  for row in result.rows() {
    let name: String = row.get("n.name")?;
    let level: String = row.get("n.level")?;
    let safeness: bool = row.get("n.safe")?;
    println!("name: {}, level: {}, safe: {}", name, level, safeness);
  }

  graph.exec("MATCH (n:LANG) DELETE n")?;
  Ok(())
}

pub mod tae {
  use cqlite::*;
  pub fn test3() -> Result<(), Error> {
    let graph = Graph::open_anon()?;

    let mut txn = graph.mut_txn()?;
    let edge: u64 = graph.prepare("CREATE (e1:TestEventStream { entity_id: '00001', offset: 0, timestamp: date(), start: 'true'})-[:PRECEDES]->(e2:TestEventStream { entity_id: '00001', offset: 1, timestamp: date(), eventData: 'aaaaa'})-[:FOLDSTO]->(s1: TestEventStreamState { entity_id: '00001', state: 1}) RETURN e1, e2, s1")?
        .query_map(&mut txn, (), |m| m.get(0))?
        .next()
        .unwrap()?;
    txn.commit()?;
    Ok(())
  }
}
