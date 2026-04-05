"use strict";

function safeString(value) {
  return String(value ?? "").trim();
}

function safeLower(value) {
  return safeString(value).toLowerCase();
}

function toNumber(value, fallback = 0) {
  if (value === null || value === undefined) return fallback;
  const parsed = Number(String(value).replace(/,/g, "").replace(/%/g, "").trim());
  return Number.isFinite(parsed) ? parsed : fallback;
}

function getArray(value) {
  return Array.isArray(value) ? value : [];
}

function safeDateValue(value) {
  const ts = new Date(value).getTime();
  return Number.isFinite(ts) ? ts : 0;
}

function isProducingStatus(status) {
  const s = safeLower(status);
  return s === "on production" || s === "testing";
}

function getDNSeverityWeight(dnType) {
  const type = safeLower(dnType);
  if (
    type.includes("sand") ||
    type.includes("valve") ||
    type.includes("tubing")
  ) {
    return 1.25;
  }
  if (
    type.includes("pinhole") ||
    type.includes("instrument")
  ) {
    return 1.1;
  }
  if (
    type.includes("corrosion") ||
    type.includes("flowline")
  ) {
    return 1.05;
  }
  return 1;
}

function getPriorityWeight(priority) {
  const p = safeLower(priority);
  if (p === "high" || p === "critical") return 1.3;
  if (p === "medium") return 1.15;
  if (p === "low") return 1;
  return 1.05;
}

function getAgeDays(dn) {
  return Math.max(
    0,
    Math.floor(
      (Date.now() - safeDateValue(dn?.created_date || dn?.update_date || dn?.last_updated)) /
        (1000 * 60 * 60 * 24)
    )
  );
}

function getAgingWeight(ageDays) {
  if (ageDays > 180) return 1.35;
  if (ageDays > 90) return 1.25;
  if (ageDays > 30) return 1.15;
  if (ageDays > 14) return 1.05;
  return 1;
}

function getProgressWeight(progressPercent) {
  const progress = toNumber(progressPercent, 0);
  if (progress <= 10) return 1.3;
  if (progress <= 30) return 1.2;
  if (progress <= 60) return 1.1;
  if (progress <= 85) return 1.02;
  return 0.95;
}

function isResolvedDN(dn) {
  const workflow = safeLower(dn?.workflow_status);
  return (
    dn?.is_closed === true ||
    workflow === "closed" ||
    workflow === "completed"
  );
}

function buildWellMap(wells) {
  const map = new Map();

  for (const well of getArray(wells)) {
    const wellId = safeString(well?.well_id || well?.id);
    if (!wellId) continue;
    map.set(wellId, well);
  }

  return map;
}

function buildDNMapByWell(dnsOrMap) {
  if (dnsOrMap instanceof Map) return dnsOrMap;

  const byWell = new Map();

  for (const dn of getArray(dnsOrMap)) {
    const wellId = safeString(dn?.well_id);
    if (!wellId) continue;

    if (!byWell.has(wellId)) {
      byWell.set(wellId, []);
    }

    byWell.get(wellId).push(dn);
  }

  return byWell;
}

function normalizeImpactBand(well, estimatedLoss) {
  if (estimatedLoss <= 0) return "none";
  return isProducingStatus(well?.production_status) ? "producing" : "deferred";
}

function estimateLossFromWell(well) {
  const rate = toNumber(well?.oil_rate_bopd, 0);
  if (rate > 0) return rate;
  return 0;
}

function calculateDNImpact(wells = [], dnsOrMap = []) {
  const safeWells = getArray(wells);
  const wellMap = buildWellMap(safeWells);
  const dnByWell = buildDNMapByWell(dnsOrMap);

  const dnDetails = [];
  const wellImpact = new Map();
  const dnTypeImpact = new Map();

  let totalEstimatedLoss = 0;
  let deferredOpportunity = 0;
  let impactedWellsCount = 0;
  let producingImpactedWellsCount = 0;
  let nonProducingImpactedWellsCount = 0;

  for (const well of safeWells) {
    const wellId = safeString(well?.well_id || well?.id);
    if (!wellId) continue;

    const wellDns = getArray(dnByWell.get(wellId));
    const activeDns = wellDns.filter((dn) => {
      return !isResolvedDN(dn);
    });

    if (activeDns.length === 0) continue;

    const baseLoss = estimateLossFromWell(well);
    const perDnLoss =
      activeDns.length > 0 && baseLoss > 0
        ? Math.round((baseLoss / activeDns.length) * 10) / 10
        : 0;

    let wellTotalLoss = 0;
    let wellHasImpact = false;

    for (const dn of activeDns) {
      const dnId = safeString(dn?.dn_id);
      const dnType = safeString(dn?.dn_type || "Unknown");
      const estimatedLoss =
        toNumber(dn?.estimated_loss_bopd, NaN);
      const ageDays = getAgeDays(dn);
      const severityWeight = getDNSeverityWeight(dnType);
      const priorityWeight = getPriorityWeight(dn?.priority);
      const agingWeight = getAgingWeight(ageDays);
      const progressWeight = getProgressWeight(dn?.progress_percent);
      const operationalWeight =
        severityWeight * priorityWeight * agingWeight * progressWeight;

      const weightedBaseLoss = isProducingStatus(well?.production_status)
        ? perDnLoss * operationalWeight
        : baseLoss * 0.35 * operationalWeight;

      const finalEstimatedLoss = Number.isFinite(estimatedLoss)
        ? estimatedLoss
        : Math.round(weightedBaseLoss * 10) / 10;

      const impactBand = normalizeImpactBand(well, finalEstimatedLoss);

      const detail = {
        ...dn,
        dn_id: dnId,
        well_id: wellId,
        well_name: safeString(well?.well_name || well?.name),
        field_code: safeString(well?.field_code).toUpperCase(),
        production_status: safeString(well?.production_status),
        dn_type: dnType,
        estimated_loss_bopd: finalEstimatedLoss,
        impact_band: impactBand,
        age_days: ageDays,
        severity_weight: severityWeight,
        priority_weight: priorityWeight,
        aging_weight: agingWeight,
        progress_weight: progressWeight,
        is_blocked:
          dn?.is_blocked === true ||
          safeLower(dn?.workflow_status).includes("waiting") ||
          safeLower(dn?.current_step).includes("package"),
        is_completed:
          dn?.is_completed === true ||
          safeLower(dn?.workflow_status) === "completed" ||
          safeLower(dn?.workflow_status) === "closed"
      };

      dnDetails.push(detail);

      if (finalEstimatedLoss > 0) {
        wellHasImpact = true;
        wellTotalLoss += finalEstimatedLoss;

        if (impactBand === "deferred") {
          deferredOpportunity += finalEstimatedLoss;
        } else {
          totalEstimatedLoss += finalEstimatedLoss;
        }

        if (!dnTypeImpact.has(dnType)) {
          dnTypeImpact.set(dnType, {
            dn_type: dnType,
            active_dn_count: 0,
            total_loss_bopd: 0
          });
        }

        const typeBucket = dnTypeImpact.get(dnType);
        typeBucket.active_dn_count += 1;
        typeBucket.total_loss_bopd += finalEstimatedLoss;
      }
    }

    if (wellHasImpact) {
      impactedWellsCount += 1;

      if (isProducingStatus(well?.production_status)) {
        producingImpactedWellsCount += 1;
      } else {
        nonProducingImpactedWellsCount += 1;
      }

      wellImpact.set(wellId, {
        well_id: wellId,
        well_name: safeString(well?.well_name || well?.name),
        field_code: safeString(well?.field_code).toUpperCase(),
        production_status: safeString(well?.production_status),
        oil_rate_bopd: toNumber(well?.oil_rate_bopd, 0),
        total_estimated_loss_bopd: Math.round(wellTotalLoss * 10) / 10,
        active_dn_count: activeDns.length
      });
    }
  }

  const topImpactedWells = [...wellImpact.values()]
    .sort((a, b) => b.total_estimated_loss_bopd - a.total_estimated_loss_bopd)
    .slice(0, 10);

  const topImpactDnTypes = [...dnTypeImpact.values()]
    .sort((a, b) => b.total_loss_bopd - a.total_loss_bopd)
    .slice(0, 10);

  return {
    total_estimated_loss_bopd: Math.round(totalEstimatedLoss * 10) / 10,
    deferred_opportunity_bopd: Math.round(deferredOpportunity * 10) / 10,
    impacted_wells_count: impactedWellsCount,
    producing_impacted_wells_count: producingImpactedWellsCount,
    non_producing_impacted_wells_count: nonProducingImpactedWellsCount,
    dn_details_count: dnDetails.length,
    dn_details: dnDetails,
    top_impacted_wells: topImpactedWells,
    top_impact_dn_types: topImpactDnTypes
  };
}

module.exports = {
  calculateDNImpact,
  buildDNMapByWell
};
