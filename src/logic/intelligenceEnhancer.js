"use strict";

/*
 * Intelligence Enhancer (Phase 8)
 *
 * Adds:
 * - DN production impact
 * - anomaly detection
 * - field imbalance detection
 */

function safeArray(v) {
  return Array.isArray(v) ? v : [];
}

function clean(v) {
  return String(v ?? "").trim();
}

function lower(v) {
  return clean(v).toLowerCase();
}

function num(v, fallback = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function isProducing(status) {
  const s = lower(status);
  return s === "on production" || s === "testing";
}

function enhanceIntelligence({ wells = [], dns = [] }) {
  const safeWells = safeArray(wells);
  const safeDNs = safeArray(dns);

  const wellMap = new Map();

  safeWells.forEach((w) => {
    const id = clean(w.well_id || w.id);
    if (!id) return;

    wellMap.set(id, {
      ...w,
      rate: num(w.oil_rate_bopd, 0),
      status: lower(w.production_status)
    });
  });

  /* ======================
     DN → production impact
  ====================== */
  const dnImpact = [];

  safeDNs.forEach((dn) => {
    const wellId = clean(dn.well_id);
    const well = wellMap.get(wellId);
    if (!well) return;

    const priority = lower(dn.priority);
    const blocked =
      lower(dn.workflow_status).includes("waiting") ||
      lower(dn.current_step).includes("package");

    if (priority === "high" || blocked) {
      dnImpact.push({
        dn_id: dn.dn_id,
        well_id: wellId,
        well_name: well.well_name,
        estimated_loss_bopd: well.rate
      });
    }
  });

  /* ======================
     anomaly detection
  ====================== */
  const producing = safeWells
    .filter((w) => isProducing(w.production_status))
    .map((w) => num(w.oil_rate_bopd, 0));

  const anomalies = [];

  if (producing.length > 0) {
    const mean = producing.reduce((a, b) => a + b, 0) / producing.length;

    const variance =
      producing.reduce((a, b) => a + Math.pow(b - mean, 2), 0) /
      producing.length;

    const std = Math.sqrt(variance);

    safeWells.forEach((w) => {
      const rate = num(w.oil_rate_bopd, 0);

      if (!isProducing(w.production_status)) return;

      if (rate > mean + 2 * std) {
        anomalies.push({
          well_id: w.well_id,
          well_name: w.well_name,
          issue: "high_production",
          value: rate
        });
      }

      if (rate < mean - 2 * std) {
        anomalies.push({
          well_id: w.well_id,
          well_name: w.well_name,
          issue: "low_production",
          value: rate
        });
      }

      if (rate === 0) {
        anomalies.push({
          well_id: w.well_id,
          well_name: w.well_name,
          issue: "zero_production",
          value: rate
        });
      }
    });
  }

  /* ======================
     field imbalance
  ====================== */
  let andr = 0;
  let abqq = 0;

  safeWells.forEach((w) => {
    if (!isProducing(w.production_status)) return;

    const rate = num(w.oil_rate_bopd, 0);
    const field = clean(w.field_code).toUpperCase();

    if (field === "ANDR") andr += rate;
    if (field === "ABQQ") abqq += rate;
  });

  const total = andr + abqq;

  let imbalance = "balanced";
  let diff = 0;

  if (total > 0) {
    const p1 = (andr / total) * 100;
    const p2 = (abqq / total) * 100;

    diff = Math.abs(p1 - p2);

    if (diff >= 20) {
      imbalance = p1 > p2 ? "ANDR-heavy" : "ABQQ-heavy";
    }
  }

  return {
    dn_production_impact: dnImpact,
    anomalies,
    field_imbalance: {
      imbalance,
      difference_percent: Math.round(diff * 10) / 10,
      details: {
        andr_rate_bopd: andr,
        abqq_rate_bopd: abqq
      }
    }
  };
}

module.exports = {
  enhanceIntelligence
};
