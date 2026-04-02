// dashboardEngine.js
// Mock dashboard engine for Well Operations AI demo
// Standalone, no dependencies on server_v1_stable.js or backend logic

// Mock production data
const mockProductionRecords = [
  {
    well_name: "ANDR-101",
    field_code: "ANDR",
    production_status: "On Production",
    oil_rate_bopd: 1200,
    timestamp: Date.now() - 10 * 60 * 1000 // 10 minutes ago
  },
  {
    well_name: "ANDR-102",
    field_code: "ANDR",
    production_status: "Testing",
    oil_rate_bopd: 950,
    timestamp: Date.now() - 30 * 60 * 1000 // 30 minutes ago
  },
  {
    well_name: "ABQQ-201",
    field_code: "ABQQ",
    production_status: "Shut-in",
    oil_rate_bopd: 0,
    timestamp: Date.now() - 5 * 60 * 1000 // 5 minutes ago
  },
  {
    well_name: "ABQQ-202",
    field_code: "ABQQ",
    production_status: "On Production",
    oil_rate_bopd: 800,
    timestamp: Date.now() - 2 * 60 * 60 * 1000 // 2 hours ago
  },
  {
    well_name: "ANDR-103",
    field_code: "ANDR",
    production_status: "On Production",
    oil_rate_bopd: 1100,
    timestamp: Date.now() - 25 * 60 * 60 * 1000 // 25 hours ago
  }
];

// Helper: Get active production records (On Production, Testing)
function getActiveProductionRecords(records = mockProductionRecords) {
  return records.filter(
    (rec) =>
      rec.production_status === "On Production" ||
      rec.production_status === "Testing"
  );
}

// Helper: Calculate total oil rate for active records
function calculateTotalRate(records = mockProductionRecords) {
  const active = getActiveProductionRecords(records);
  if (!active.length) return 0;
  return active.reduce((sum, rec) => sum + (Number(rec.oil_rate_bopd) || 0), 0);
}

// Helper: Calculate hourly average oil rate (last 60 min)
function calculateHourlyAverage(records = mockProductionRecords, now = Date.now()) {
  const oneHourAgo = now - 60 * 60 * 1000;
  const recent = getActiveProductionRecords(records).filter(
    (rec) => rec.timestamp >= oneHourAgo && rec.timestamp <= now
  );
  if (!recent.length) return 0;
  return (
    recent.reduce((sum, rec) => sum + (Number(rec.oil_rate_bopd) || 0), 0) /
    recent.length
  );
}

// Helper: Calculate daily average oil rate (last 24 hours)
function calculateDailyAverage(records = mockProductionRecords, now = Date.now()) {
  const oneDayAgo = now - 24 * 60 * 60 * 1000;
  const recent = getActiveProductionRecords(records).filter(
    (rec) => rec.timestamp >= oneDayAgo && rec.timestamp <= now
  );
  if (!recent.length) return 0;
  return (
    recent.reduce((sum, rec) => sum + (Number(rec.oil_rate_bopd) || 0), 0) /
    recent.length
  );
}

module.exports = {
  mockProductionRecords,
  getActiveProductionRecords,
  calculateTotalRate,
  calculateHourlyAverage,
  calculateDailyAverage
};
