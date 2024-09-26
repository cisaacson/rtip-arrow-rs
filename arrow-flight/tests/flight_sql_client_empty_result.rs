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

use arrow_array::{RecordBatch, builder::StringBuilder, ArrayRef};
use arrow_schema::{ArrowError, SchemaRef, Field, Schema, DataType};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::{flight_service_server::FlightServiceServer, FlightData, FlightDescriptor, FlightInfo, flight_descriptor::DescriptorType, FlightEndpoint, Ticket, flight_service_server::FlightService, utils::batches_to_flight_data};
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionBeginTransactionRequest, ActionBeginTransactionResult, ActionEndTransactionRequest,
    CommandStatementQuery, SqlInfo, TableDefinitionOptions, TableExistsOption,
    TableNotExistOption, TicketStatementQuery, ProstMessageExt,
};
use arrow_flight::Action;
use futures::{StreamExt, TryStreamExt, Stream};
use std::collections::HashMap;
use std::sync::Arc;
use std::pin::Pin;
use futures::stream;
use tokio::sync::Mutex;
use tonic::{Request, Status};
use uuid::Uuid;
use tonic::Response;
use prost::Message;

#[tokio::test]
pub async fn test_get_empty_result() {
    let test_server = FlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service()).await;
    let channel = fixture.channel().await;
    let mut flight_sql_client = FlightSqlServiceClient::new(channel);

    // Execute a dummy `CommandStatementQuery` to get a valid
    // flight_info response.
    let cmd = CommandStatementQuery {
        query: "SELECT salutation FROM dummy".to_string(),
        transaction_id: None,
    };
    let flight_info = flight_sql_client.execute(cmd.query, None).await.expect("Could not call client execute");

    if flight_info.endpoint.len() > 1 {
        panic!("Only a single endpoint is expected from ng_db_server. flight_info.endpoint.len(): {}", flight_info.endpoint.len());
    }

    let endpoint = &flight_info.endpoint[0];

    // Get the ticket, this must be a `TicketStatementQuery` which contains
    // the results handle.
    let ticket = if let Some(ticket) = &endpoint.ticket {
        ticket
    } else {
        panic!("Could not get ticket from the endpoint.");
    };

    let ticket_any: arrow_flight::sql::Any = prost::Message::decode(ticket.ticket.clone()).expect("Could not decode the ticket.ticket Bytes as a Message.");

    // Now `unpack` the ticket to a `TicketStatementQuery`.
    let ticket_statement_query: TicketStatementQuery = ticket_any.unpack().expect("Could not unpack ticket").unwrap();

    // Decode the `TicketStatementQuery.statement_handle` now contains the message `Bytes` for the
    // `results_handle` which is the `String` value we want. 
    let results_handle: String = prost::Message::decode(ticket_statement_query.clone().statement_handle).expect("Could not decode the TicketStatementQuery.handle");
    println!("results_handle: {} ticket_statement_query: {:?} ticket_statement_query.statement_handle: {}", results_handle, &ticket_statement_query, std::str::from_utf8(&ticket_statement_query.statement_handle).expect("Could not build str from utf8 bytes"));

    let mut flight_data = flight_sql_client.do_get(ticket.clone()).await.unwrap();

    // Get the first RecordBatch which should be an empty batch.
    let rb = flight_data.next().await;

    assert!(rb.is_some());
}

// Formatter for server status.
macro_rules! status {
    ($desc:expr, $err:expr) => {
        Status::internal(format!("{}: {} at {}:{}", $desc, $err, file!(), line!()))
    };
}

#[derive(Clone)]
pub struct FlightSqlServiceImpl {}

impl FlightSqlServiceImpl {
    pub fn new() -> Self {
        Self {}
    }

    /// Return an [`FlightServiceServer`] that can be used with a
    /// [`Server`](tonic::transport::Server)
    pub fn service(&self) -> FlightServiceServer<Self> {
        // wrap up tonic goop
        FlightServiceServer::new(self.clone())
    }

    fn fake_empty_result() -> Result<RecordBatch, ArrowError> {
        let schema = Schema::new(vec![Field::new("salutation", DataType::Utf8, false)]);

        Ok(RecordBatch::new_empty(schema.into()))
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

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}

    // Implement a dummy return for a query.
    async fn get_flight_info_statement(
        &self,
        command_statement_query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {

        // Create the `TicketStatementQuery` instance.
        let ticket_statement_query = TicketStatementQuery {
            statement_handle: "0102030405060708".to_string().into(),
        };

        let schema = Schema::new(vec![Field::new("salutation", DataType::Utf8, false)]);

        // Build and return the `FlightInfo` response.
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| status!("Unable to serialize schema", e))?
            .with_descriptor(FlightDescriptor {
                r#type: DescriptorType::Cmd.into(),
                cmd: Default::default(),
                path: vec![],
            })            
            .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(ticket_statement_query.as_any().encode_to_vec())))
            .with_total_records(-1 as i64) // We do not know the number of rows as we always read a stream.
            .with_total_bytes(-1 as i64)
            .with_ordered(false);

        Ok(Response::new(info))

    }

    // Implement a return of an empty result.
    async fn do_get_statement(
        &self,
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
