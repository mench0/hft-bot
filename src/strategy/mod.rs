//! Стратегии: spread capture, стат-арбитраж, перп-арбитраж, lead-lag.

pub mod leadlag;
pub mod perparb;
pub mod spread;
pub mod statarb;

/// Биржа для межбиржевых стратегий: счёт, торговля включена, стаканы выбранных монет.
pub struct Venue<'a> {
    pub account: &'a mut crate::trading::account::Account,
    pub trading: bool,
    pub lim: crate::trading::paper::Limits,
    pub books: std::collections::HashMap<String, crate::market::book::Book>,
}
