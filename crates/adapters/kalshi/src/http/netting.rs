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

//! Account and saved event collateral-return admission.

use super::{
    client::KalshiHttpClient,
    models::{Event, Field, Market, PositionPages, SubaccountNettingResponse},
    parse::supported_market,
    query::PositionsQuery,
};
use crate::common::enums::PositionSettlementStatus;

/// Effective state for primary subaccount 0 and the requested event/shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NettingState {
    Disabled,
    Enabled,
    Unknown,
}

impl NettingState {
    pub(crate) fn from_responses(
        market: &Market,
        event: &Event,
        account: &SubaccountNettingResponse,
        positions: &PositionPages,
    ) -> Self {
        let Field::Value(shard) = market.exchange_index else {
            return Self::Unknown;
        };
        let Ok(shard) = u32::try_from(shard) else {
            return Self::Unknown;
        };

        if market.event_ticker != event.event_ticker
            || matches!(event.exchange_index, Field::Value(value) if i64::from(value) != i64::from(shard))
            || !positions.complete
            || positions.next_cursor.is_some()
            || positions
                .event_positions
                .iter()
                .any(|position| position.event_ticker != event.event_ticker)
        {
            return Self::Unknown;
        }
        let mut configs = account
            .netting_configs
            .iter()
            .filter(|config| config.subaccount_number == 0 && config.exchange_index == shard);
        let Some(config) = configs.next() else {
            return Self::Unknown;
        };

        if configs.next().is_some() {
            return Self::Unknown;
        }

        match config.enabled {
            Field::Value(false) => {}
            Field::Value(true) => return Self::Enabled,
            Field::Missing | Field::Null => return Self::Unknown,
        }
        // A complete empty response inherits the current account default. Existing zero-size
        // history records still carry a saved setting and must not be filtered out.
        let mut result = Self::Disabled;

        for position in &positions.market_positions {
            if i64::from(position.exchange_index) != i64::from(shard)
                || !(position.ticker == market.ticker
                    || position
                        .ticker
                        .strip_prefix(&event.event_ticker)
                        .is_some_and(|suffix| suffix.starts_with('-')))
            {
                return Self::Unknown;
            }

            match position.netting_enabled {
                Field::Value(true) => return Self::Enabled,
                Field::Value(false) => {}
                Field::Missing | Field::Null => result = Self::Unknown,
            }
        }
        result
    }
}

impl KalshiHttpClient {
    pub(crate) async fn require_supported_market(
        &self,
        market: &Market,
        event: &Event,
    ) -> anyhow::Result<()> {
        let shard = match market.exchange_index {
            Field::Value(value) => u32::try_from(value)?,
            _ => anyhow::bail!("netting admission requires an explicit market shard"),
        };
        let account = self.get_subaccount_netting().await?;
        let positions = self
            .get_all_positions(
                PositionsQuery {
                    event_ticker: Some(event.event_ticker.clone()),
                    settlement_status: Some(PositionSettlementStatus::All),
                    limit: Some(1000),
                    subaccount: 0,
                    exchange_index: shard,
                    ..Default::default()
                },
                100,
            )
            .await?;
        let state = NettingState::from_responses(market, event, &account, &positions);
        anyhow::ensure!(
            supported_market(market, event, state),
            "Kalshi market {} is unsupported: account/event netting must be disabled (state {state:?})",
            market.ticker
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::{Value, json};

    use super::*;
    use crate::{common::enums::CollateralReturnType, http::models::MarketPosition};

    fn definition() -> (Market, Event) {
        let value: Value =
            serde_json::from_str(include_str!("../../test_data/instruments.json")).unwrap();
        let value = &value["definitions"]["unsupported"];
        (
            serde_json::from_value(value["market"].clone()).unwrap(),
            serde_json::from_value(value["event"].clone()).unwrap(),
        )
    }

    fn evidence(
        account_flag: &str,
        saved_flag: &str,
    ) -> (SubaccountNettingResponse, PositionPages) {
        let (market, event) = definition();
        let mut config = json!({"subaccount_number": 0, "exchange_index": 0});
        if account_flag != "missing" {
            config["enabled"] = serde_json::from_str(account_flag).unwrap();
        }
        let account = serde_json::from_value(json!({"netting_configs": [config]})).unwrap();
        let mut positions = PositionPages {
            market_positions: Vec::new(),
            event_positions: Vec::new(),
            complete: true,
            next_cursor: None,
        };

        if saved_flag != "absent" {
            let fixtures: Value =
                serde_json::from_str(include_str!("../../test_data/rest_responses.json")).unwrap();
            let mut position = fixtures["positions"]["market_positions"][0].clone();
            position["ticker"] = market.ticker.into();
            position["exchange_index"] = 0.into();
            // An exited position must still retain its saved event setting.
            position["position_fp"] = "0.00".into();
            position.as_object_mut().unwrap().remove("netting_enabled");
            if saved_flag != "missing" {
                position["netting_enabled"] = serde_json::from_str(saved_flag).unwrap();
            }
            positions
                .market_positions
                .push(serde_json::from_value::<MarketPosition>(position).unwrap());
        }
        assert_eq!(event.collateral_return_type, CollateralReturnType::Mecnet);
        (account, positions)
    }

    #[rstest]
    #[case("false", "absent", true)]
    #[case("false", "false", true)]
    #[case("false", "true", false)]
    #[case("false", "missing", false)]
    #[case("false", "null", false)]
    #[case("true", "absent", false)]
    #[case("true", "false", false)]
    #[case("true", "true", false)]
    #[case("missing", "absent", false)]
    #[case("missing", "false", false)]
    #[case("null", "absent", false)]
    #[case("null", "false", false)]
    fn supported_market_requires_disabled_account_and_event(
        #[case] account_flag: &str,
        #[case] saved_flag: &str,
        #[case] expected: bool,
        #[values(false, true)] mecnet: bool,
    ) {
        let (market, mut event) = definition();
        if !mecnet {
            event.collateral_return_type = CollateralReturnType::None;
        }
        let (account, positions) = evidence(account_flag, saved_flag);
        let state = NettingState::from_responses(&market, &event, &account, &positions);
        assert_eq!(supported_market(&market, &event, state), expected);
    }

    #[rstest]
    #[case("wrong_subaccount")]
    #[case("wrong_shard")]
    #[case("duplicate_account")]
    #[case("missing_account")]
    #[case("incomplete_positions")]
    #[case("continuation")]
    #[case("wrong_position_shard")]
    #[case("wrong_position_market")]
    #[case("wrong_event")]
    fn invalid_or_incomplete_scope_cannot_authorize_market(#[case] failure: &str) {
        let (market, mut event) = definition();
        let (mut account, mut positions) = evidence("false", "false");
        match failure {
            "wrong_subaccount" => account.netting_configs[0].subaccount_number = 1,
            "wrong_shard" => account.netting_configs[0].exchange_index = 1,
            "duplicate_account" => account
                .netting_configs
                .push(account.netting_configs[0].clone()),
            "missing_account" => account.netting_configs.clear(),
            "incomplete_positions" => positions.complete = false,
            "continuation" => positions.next_cursor = Some("next".into()),
            "wrong_position_shard" => positions.market_positions[0].exchange_index = 1,
            "wrong_position_market" => {
                positions.market_positions[0].ticker = "UNRELATED-MARKET".into();
            }
            "wrong_event" => event.event_ticker = "UNRELATED-EVENT".into(),
            _ => unreachable!(),
        }
        let state = NettingState::from_responses(&market, &event, &account, &positions);
        assert!(!supported_market(&market, &event, state));
    }

    #[rstest]
    fn another_market_in_the_event_can_block_admission() {
        let (market, event) = definition();
        let (account, mut positions) = evidence("false", "false");
        let mut sibling = positions.market_positions[0].clone();
        sibling.ticker = format!("{}-OTHER", event.event_ticker);
        sibling.netting_enabled = Field::Value(true);
        positions.market_positions.push(sibling);
        let state = NettingState::from_responses(&market, &event, &account, &positions);
        assert!(!supported_market(&market, &event, state));
    }

    #[rstest]
    #[case("scalar")]
    #[case("notional")]
    #[case("bounds")]
    #[case("floor")]
    #[case("mve")]
    #[case("directional")]
    #[case("unknown")]
    fn disabled_netting_does_not_relax_other_product_boundaries(#[case] failure: &str) {
        let (mut market, mut event) = definition();
        match failure {
            "scalar" => market.market_type = "scalar".parse().unwrap(),
            "notional" => market.notional_value_dollars.0 = rust_decimal::Decimal::TWO,
            "bounds" => market.settlement_bounds_type = "floor".parse().unwrap(),
            "floor" => {
                market.settlement_floor_dollars =
                    Field::Value(super::super::models::FixedPoint(rust_decimal::Decimal::ONE));
            }
            "mve" => market.mve_collection_ticker = Field::Value("MVE".into()),
            "directional" => event.collateral_return_type = CollateralReturnType::Direcnet,
            "unknown" => {
                event.collateral_return_type = CollateralReturnType::Other("future".into());
            }
            _ => unreachable!(),
        }
        assert!(!supported_market(&market, &event, NettingState::Disabled));
    }
}
