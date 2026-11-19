use anyhow::{bail, Context, Result};

/// Host, port and TLS of a gRPC endpoint given as an `http://` or `https://` URL.
#[derive(Debug, Clone, PartialEq)]
pub struct GrpcEndpoint {
    pub host: String,
    pub port: u32,
    pub tls: bool,
}

/// Parses a gRPC endpoint URL; the port defaults to 80 / 443 and any path is ignored.
pub fn parse_grpc_endpoint(url: &str) -> Result<GrpcEndpoint> {
    let parsed = url::Url::parse(url).context("Invalid gRPC endpoint URL")?;

    let tls = match parsed.scheme() {
        "http" => false,
        "https" => true,
        scheme => bail!("Unsupported scheme: {scheme}. Use http:// or https://"),
    };

    let host = parsed
        .host_str()
        .context("Missing host in URL")?
        .to_string();

    let port = parsed.port().unwrap_or(if tls { 443 } else { 80 }) as u32;

    Ok(GrpcEndpoint { host, port, tls })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_grpc_endpoint_http() {
        let endpoint = parse_grpc_endpoint("http://conductor:9090").unwrap();
        assert_eq!(endpoint.host, "conductor");
        assert_eq!(endpoint.port, 9090);
        assert!(!endpoint.tls);
    }

    #[test]
    fn test_parse_grpc_endpoint_https() {
        let endpoint = parse_grpc_endpoint("https://conductor:9090").unwrap();
        assert_eq!(endpoint.host, "conductor");
        assert_eq!(endpoint.port, 9090);
        assert!(endpoint.tls);
    }

    #[test]
    fn test_parse_grpc_endpoint_default_port_http() {
        let endpoint = parse_grpc_endpoint("http://host").unwrap();
        assert_eq!(endpoint.port, 80);
        assert!(!endpoint.tls);
    }

    #[test]
    fn test_parse_grpc_endpoint_default_port_https() {
        let endpoint = parse_grpc_endpoint("https://host").unwrap();
        assert_eq!(endpoint.port, 443);
        assert!(endpoint.tls);
    }

    #[test]
    fn test_parse_grpc_endpoint_trailing_slash() {
        let endpoint = parse_grpc_endpoint("http://host:9090/").unwrap();
        assert_eq!(endpoint.host, "host");
        assert_eq!(endpoint.port, 9090);
    }

    #[test]
    fn test_parse_grpc_endpoint_ipv6_host_keeps_brackets() {
        let endpoint = parse_grpc_endpoint("http://[::1]:9000").unwrap();
        assert_eq!(endpoint.host, "[::1]");
        assert_eq!(endpoint.port, 9000);
    }

    #[test]
    fn test_parse_grpc_endpoint_max_port() {
        let endpoint = parse_grpc_endpoint("http://host:65535").unwrap();
        assert_eq!(endpoint.port, 65535);
    }

    #[test]
    fn test_parse_grpc_endpoint_port_out_of_range() {
        assert!(parse_grpc_endpoint("http://host:65536").is_err());
    }

    #[test]
    fn test_parse_grpc_endpoint_invalid_scheme() {
        let result = parse_grpc_endpoint("ftp://host");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unsupported scheme"));
    }

    #[test]
    fn test_parse_grpc_endpoint_empty() {
        assert!(parse_grpc_endpoint("").is_err());
    }
}
