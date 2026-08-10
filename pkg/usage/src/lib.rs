use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Per-tenant counters for managed-cloud usage metering.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageCounters {
    /// Number of billable operations for this tenant.
    pub requests_total: u64,
    /// Approximate bytes ingested/retrieved for this tenant.
    pub bytes_total: u64,
}

impl UsageCounters {
    /// Record one operation and its byte size.
    pub fn record(&mut self, bytes: usize) {
        self.requests_total = self.requests_total.saturating_add(1);
        self.bytes_total = self.bytes_total.saturating_add(bytes as u64);
    }

    /// Merge another counter into this one.
    pub fn add(&mut self, other: &Self) {
        self.requests_total = self.requests_total.saturating_add(other.requests_total);
        self.bytes_total = self.bytes_total.saturating_add(other.bytes_total);
    }
}

/// A point-in-time usage snapshot keyed by tenant ID.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageSnapshot {
    pub tenants: HashMap<String, UsageCounters>,
}

impl UsageSnapshot {
    /// Record an operation against a tenant.
    pub fn record(&mut self, tenant_id: impl Into<String>, bytes: usize) {
        self.tenants
            .entry(tenant_id.into())
            .or_default()
            .record(bytes);
    }

    /// Merge another snapshot into this one.
    pub fn merge(&mut self, other: &Self) {
        for (tenant, counters) in &other.tenants {
            self.tenants
                .entry(tenant.clone())
                .or_default()
                .add(counters);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_counters_record_and_merge() {
        let mut a = UsageCounters::default();
        a.record(100);
        a.record(50);
        assert_eq!(a.requests_total, 2);
        assert_eq!(a.bytes_total, 150);

        let mut b = UsageCounters::default();
        b.record(10);

        a.add(&b);
        assert_eq!(a.requests_total, 3);
        assert_eq!(a.bytes_total, 160);
    }

    #[test]
    fn usage_snapshot_records_per_tenant() {
        let mut snapshot = UsageSnapshot::default();
        snapshot.record("tenant-a", 10);
        snapshot.record("tenant-a", 20);
        snapshot.record("tenant-b", 5);

        assert_eq!(snapshot.tenants["tenant-a"].requests_total, 2);
        assert_eq!(snapshot.tenants["tenant-a"].bytes_total, 30);
        assert_eq!(snapshot.tenants["tenant-b"].requests_total, 1);
        assert_eq!(snapshot.tenants["tenant-b"].bytes_total, 5);
    }
}
