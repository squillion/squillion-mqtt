use slog::*;

use slog::Drain;
use std::net::TcpStream;

use crate::config;

pub fn get_logger(uuid: &str) -> slog::Logger {
    let decorator = slog_term::PlainDecorator::new(std::io::stdout());
    let term_drain = slog_term::FullFormat::new(decorator).build();

    let drain = if config::get_bool("logging_remote").unwrap() {
        let mongo_drain = slog_retry::Retry::new(
            || -> Result<_> {
                let logging_enpoint = config::get_string("logging_endpoint").unwrap();
                let connection = TcpStream::connect(&logging_enpoint)?;
                Ok(slog_json::Json::default(connection))
            },
            None,
            true,
        )
        .unwrap()
        .ignore_res();

        slog_async::Async::new(slog::Duplicate::new(mongo_drain, term_drain).fuse())
            .build()
            .fuse()
    } else {
        slog_async::Async::new(term_drain.fuse()).build().fuse()
    };

    slog::Logger::root(
        drain,
        slog::o!("_hostname" => hostname::get().unwrap().into_string().unwrap(),
        "uuid" => uuid.to_string()),
    )
}
