// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Production Kalshi data probe for KXCS2GAME-26NOV120700LEOG1-LEO.
//!
//! Uses only a data client and the built-in `DataTester` actor. No orders are placed.
//! Subscribes to quotes, trades, L2 books, status, and closes, and requests a book
//! snapshot plus the last hour of trades and one-minute candles.
//!
//! Run with:
//! `cargo run --locked -p nautilus-kalshi --example node_data_tester_probe`
//!
//! Credentials are read from `KALSHI_API_KEY_ID` and `KALSHI_PRIVATE_KEY`.

use nautilus_common::{enums::Environment, logging::logger::LoggerConfig};
use nautilus_kalshi::{
    common::{consts::KALSHI_CLIENT_ID, enums::KalshiEnvironment},
    config::KalshiDataClientConfig,
    factories::KalshiDataClientFactory,
};
use nautilus_live::node::LiveNode;
use nautilus_model::{
    data::bar::BarType,
    identifiers::{InstrumentId, TraderId},
};
use nautilus_testkit::testers::{DataTester, DataTesterConfig};

const TRADER_ID: &str = "TESTER-001";
const NODE_NAME: &str = "KALSHI-DATA-TESTER-001";
const INSTRUMENT_ID: &str = "KXCS2GAME-26NOV120700LEOG1-LEO.KALSHI";
const ENVIRONMENT: KalshiEnvironment = KalshiEnvironment::Production;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let trader_id = TraderId::from(TRADER_ID);
    let client_id = *KALSHI_CLIENT_ID;
    let instrument_id = InstrumentId::from(INSTRUMENT_ID);
    let instrument_ids = vec![instrument_id];
    let bar_type = BarType::from(format!("{INSTRUMENT_ID}-1-MINUTE-LAST-EXTERNAL").as_str());

    let data_config = KalshiDataClientConfig {
        environment: ENVIRONMENT,
        instrument_ids: instrument_ids.clone(),
        ..Default::default()
    };

    let log_config = LoggerConfig {
        stdout_level: log::LevelFilter::Info,
        ..Default::default()
    };

    let mut node = LiveNode::builder(trader_id, Environment::Live)?
        .with_name(NODE_NAME.to_string())
        .with_load_state(false)
        .with_save_state(false)
        .with_logging(log_config)
        .add_data_client(
            None,
            Box::new(KalshiDataClientFactory),
            Box::new(data_config),
        )?
        .build()?;

    let tester_config = DataTesterConfig::builder()
        .client_id(client_id)
        .instrument_ids(instrument_ids)
        .bar_types(vec![bar_type])
        .subscribe_book_deltas(true)
        .subscribe_quotes(true)
        .subscribe_trades(true)
        .subscribe_instrument_status(true)
        .subscribe_instrument_close(true)
        .request_trades(true)
        .request_bars(true)
        .request_book_snapshot(true)
        .manage_book(true)
        .log_data(true)
        .build()?;
    node.add_actor(DataTester::new(tester_config))?;
    node.run().await?;
    Ok(())
}
