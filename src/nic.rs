use pnet_datalink as nic;
use std::net::IpAddr;

pub struct LocalAddr {
    pub iface: String,
    pub ip: IpAddr,
}

pub fn enumerate(filter_loopback: bool) -> Vec<LocalAddr> {
    let mut out = Vec::new();
    for iface in nic::interfaces() {
        if filter_loopback && iface.is_loopback() {
            continue;
        }
        if !iface.is_up() {
            continue;
        }
        for ipn in &iface.ips {
            let ip = ipn.ip();
            if ip.is_unspecified() {
                continue;
            }
            if filter_loopback && ip.is_loopback() {
                continue;
            }
            out.push(LocalAddr {
                iface: iface.name.clone(),
                ip,
            });
        }
    }
    out
}

pub fn is_likely_infiniband(name: &str) -> bool {
    name.starts_with("ib") || name.starts_with("ibp") || name.starts_with("ibs")
}
