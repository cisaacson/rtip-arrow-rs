// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

mod common;

use crate::common::fixture::TestFixture;
use crate::common::utils::make_primitive_batch;

use arrow_array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionBeginTransactionRequest, ActionBeginTransactionResult, ActionEndTransactionRequest,
    CommandStatementIngest, EndTransaction, SqlInfo, TableDefinitionOptions, TableExistsOption,
    TableNotExistOption,
};
use arrow_flight::Action;
use futures::{StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Status};
use uuid::Uuid;

#[tokio::test]
pub async fn test_begin_end_transaction() {
    let test_server = FlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service()).await;
    let channel = fixture.channel().await;
    let mut flight_sql_client = FlightSqlServiceClient::new(channel);

    // begin commit
    let transaction_id = flight_sql_client.begin_transaction().await.unwrap();
    flight_sql_client
        .end_transaction(transaction_id, EndTransaction::Commit)
        .await
        .unwrap();

    // begin rollback
    let transaction_id = flight_sql_client.begin_transaction().await.unwrap();
    flight_sql_client
        .end_transaction(transaction_id, EndTransaction::Rollback)
        .await
        .unwrap();

    // unknown transaction id
    let transaction_id = "UnknownTransactionId".to_string().into();
    assert!(flight_sql_client
        .end_transaction(transaction_id, EndTransaction::Commit)
        .await
        .is_err());
}

#[tokio::test]
pub async fn test_execute_ingest() {
    let test_server = FlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service()).await;
    let channel = fixture.channel().await;
    let mut flight_sql_client = FlightSqlServiceClient::new(channel);
    let cmd = make_ingest_command();
    let expected_rows = 10;
    let batches = vec![
        make_primitive_batch(5),
        make_primitive_batch(3),
        make_primitive_batch(2),
    ];
    let actual_rows = flight_sql_client
        .execute_ingest(cmd, futures::stream::iter(batches.clone()).map(Ok))
        .await
        .expect("ingest should succeed");
    assert_eq!(actual_rows, expected_rows);
    // make sure the batches made it through to the server
    let ingested_batches = test_server.ingested_batches.lock().await.clone();
    assert_eq!(ingested_batches, batches);
}

#[tokio::test]
pub async fn test_execute_ingest_error() {
    let test_server = FlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service()).await;
    let channel = fixture.channel().await;
    let mut flight_sql_client = FlightSqlServiceClient::new(channel);
    let cmd = make_ingest_command();
    // send an error from the client
    let batches = vec![
        Ok(make_primitive_batch(5)),
        Err(FlightError::NotYetImplemented(
            "Client error message".to_string(),
        )),
    ];
    // make sure the client returns the error from the client
    let err = flight_sql_client
        .execute_ingest(cmd, futures::stream::iter(batches))
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "External error: Not yet implemented: Client error message"
    );
}

fn make_ingest_command() -> CommandStatementIngest {
    CommandStatementIngest {
        table_definition_options: Some(TableDefinitionOptions {
            if_not_exist: TableNotExistOption::Create.into(),
            if_exists: TableExistsOption::Fail.into(),
        }),
        table: String::from("test"),
        schema: None,
        catalog: None,
        temporary: true,
        transaction_id: None,
        options: HashMap::default(),
    }
}

#[derive(Clone)]
pub struct FlightSqlServiceImpl {
    transactions: Arc<Mutex<HashMap<String, ()>>>,
    ingested_batches: Arc<Mutex<Vec<RecordBatch>>>,
}

impl FlightSqlServiceImpl {
    pub fn new() -> Self {
        Self {
            transactions: Arc::new(Mutex::new(HashMap::new())),
            ingested_batches: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Return an [`FlightServiceServer`] that can be used with a
    /// [`Server`](tonic::transport::Server)
    pub fn service(&self) -> FlightServiceServer<Self> {
        // wrap up tonic goop
        FlightServiceServer::new(self.clone())
    }

    fn fake_empty_result() -> Result<RecordBatch, ArrowError> {
        let schema = Schema::new(vec![Field::new("salutation", DataType::Utf8, false)]);
        let mut builder = StringBuilder::new(); // Add an empty StringBuilder array.
        // builder.append_value("Hello, FlightSQL!");
        let cols = vec![Arc::new(builder.finish()) as ArrayRef];
        // RecordBatch::try_new(Arc::new(schema), cols)
        Ok(RecordBatch::new_empty(schema))
    }

}

impl Default for FlightSqlServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = FlightSqlServiceImpl;

    // Implement a return of an empty result.
    async fn do_get_statement(
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let batch = Self::fake_empty_result().map_err(|e| status!("Could not fake an empty result", e))?;
        let schema = batch.schema_ref();
        let batches = vec![batch.clone()];
        let flight_data = batches_to_flight_data(schema, batches)
            .map_err(|e| status!("Could not convert batches", e))?
            .into_iter()
            .map(Ok);

        let stream: Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>> =
            Box::pin(stream::iter(flight_data));
        let resp = Response::new(stream);
        Ok(resp)
    }

}
