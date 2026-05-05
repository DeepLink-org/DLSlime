//! Network utility functions for IP resolution.

/// Get server's IP address for a remote client.
///
/// Uses the UDP socket trick: "connecting" a UDP socket (no data sent)
/// to the client IP causes the OS to select the correct local interface.
pub fn get_server_ip_for_client(client_ip: &str) -> Option<String> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect(format!("{client_ip}:80")).ok()?;
    let local_addr = socket.local_addr().ok()?;
    let ip = local_addr.ip().to_string();
    if ip == "0.0.0.0" || ip == "127.0.0.1" {
        None
    } else {
        Some(ip)
    }
}

/// Detect a non-loopback local IP by connecting a UDP socket to 8.8.8.8.
pub fn get_local_public_ip() -> Option<String> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:80").ok()?;
    let local_addr = socket.local_addr().ok()?;
    let ip = local_addr.ip().to_string();
    if ip == "0.0.0.0" || ip == "127.0.0.1" {
        None
    } else {
        Some(ip)
    }
}

/// Replace 127.0.0.1/localhost in a Redis URL with the host's detected
/// non-loopback IP (via UDP socket trick).  Falls back to the original
/// URL if detection fails or the URL already uses a public address.
pub fn resolve_public_redis_url(url: &str) -> String {
    if let Some(stripped) = url.strip_prefix("redis://") {
        if stripped.starts_with("127.0.0.1") || stripped.starts_with("localhost") {
            // Try NANOCTRL_REDIS_URL env var first (set by NanoOps)
            if let Ok(env_url) = std::env::var("NANOCTRL_REDIS_URL") {
                if !env_url.is_empty()
                    && !env_url.contains("127.0.0.1")
                    && !env_url.contains("localhost")
                {
                    return env_url;
                }
            }
            // Fallback: detect local IP via UDP socket
            if let Some(public_ip) = get_local_public_ip() {
                let port_part = stripped
                    .find(':')
                    .map(|pos| &stripped[pos..])
                    .unwrap_or(":6379");
                return format!("redis://{public_ip}{port_part}");
            }
        }
    }
    url.to_string()
}

/// Resolve Redis address for a remote client.
///
/// If the Redis URL points to localhost but the client is remote,
/// try to resolve to a public IP the client can reach.
pub fn resolve_redis_for_client(redis_url: &str, client_address: &str) -> String {
    if redis_url.starts_with("redis://") {
        let addr = redis_url.strip_prefix("redis://").unwrap_or(redis_url);
        // If Redis is on localhost and client is remote
        if (addr.starts_with("127.0.0.1") || addr.starts_with("localhost"))
            && !client_address.starts_with("127.0.0.1")
            && !client_address.starts_with("localhost")
        {
            // Use environment variable REDIS_PUBLIC_ADDRESS if set
            if let Ok(public_addr) = std::env::var("REDIS_PUBLIC_ADDRESS") {
                tracing::info!(
                    "Client {} is remote, using REDIS_PUBLIC_ADDRESS={}",
                    client_address,
                    public_addr
                );
                return public_addr;
            }
            // Extract port
            let port = addr.find(':').map(|pos| &addr[pos + 1..]).unwrap_or("6379");
            // Try to get server's IP from the client's perspective
            let server_ip = get_server_ip_for_client(client_address)
                .or_else(get_local_public_ip)
                .unwrap_or_else(|| {
                    tracing::warn!(
                        "Cannot determine server IP for remote client {}. \
                         Please set REDIS_PUBLIC_ADDRESS environment variable. \
                         Falling back to 127.0.0.1 (may not work for remote clients).",
                        client_address
                    );
                    "127.0.0.1".to_string()
                });
            format!("{server_ip}:{port}")
        } else {
            addr.to_string()
        }
    } else {
        redis_url.to_string()
    }
}
