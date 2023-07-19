use hyper::header::CONTENT_TYPE;
use hyper::service::{make_service_fn, service_fn};
use hyper::{server::Server, Body, Request, Response, StatusCode};
use std::convert::Infallible;

use prometheus::{Encoder, TextEncoder};

use crate::config;

async fn metrics(_req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let metric_families = prometheus::gather();
    let mut buffer = vec![];

    let encoder = TextEncoder::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();

    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, encoder.format_type())
        .body(Body::from(buffer))
        .unwrap();

    Ok(response)
}

async fn livez(_req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let status = if config::get_livez() { 200 } else { 500 };

    let response = hyper::Response::builder()
        .status(status)
        .body("livez".into())
        .unwrap();

    Ok(response)
}

async fn readyz(_req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let status = if config::get_readyz() { 200 } else { 500 };

    let response = hyper::Response::builder()
        .status(status)
        .body("readyz".into())
        .unwrap();

    Ok(response)
}

async fn not_found_handler(_req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let response = hyper::Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body("NOT FOUND".into())
        .unwrap();

    Ok(response)
}

async fn route(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    if req.uri() == "/metrics" {
        metrics(req).await
    } else if req.uri() == "/livez" {
        livez(req).await
    } else if req.uri() == "/healthz" {
        livez(req).await
    } else if req.uri() == "/readyz" {
        readyz(req).await
    } else {
        not_found_handler(req).await
    }
}

pub async fn serve(logger: slog::Logger) {
    let addr = ([0, 0, 0, 0], 9898).into();

    slog::info!(logger, "Metrics listening address: {:?}", addr);

    let make_svc = make_service_fn(|_conn| async {
        // service_fn converts our function into a `Service`
        Ok::<_, Infallible>(service_fn(route))
    });

    let server = Server::bind(&addr).serve(make_svc);

    // Run this server for... forever!
    if let Err(e) = server.await {
        slog::error!(logger, "metrics server error: {}", e);
    }
}
