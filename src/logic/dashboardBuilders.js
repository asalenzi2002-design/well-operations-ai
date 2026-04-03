/*
 * Dashboard builder module (Phase 6 -> Phase 10)
 *
 * Centralizes dashboard assembly so server_working.js stays lean.
 */

const {
  buildProductionIntelligence,
  buildDNIntelligence,
  buildSystemIntelligence
} = require("./intelligenceBuilders");

const {
  calculateWellRisk,
  calculateDNRisk,
  buildRiskDashboard
} = require("./riskEngine");

const { generateRecommendations } = require("./recommendationEngine");
const { enhanceIntelligence } = require("./intelligenceEnhancer");

/* ====================================================================
   Helpers
==================================================================== */

function toSafeString(value) {
  return String(value ?? "").trim();
}

function toSafeLower(value) {
  return toSafeString(value).toLowerCase();
}

function toNumber(value, fallback = 0) {
  if (value === null || value === undefined) return fallback;
  const cleaned = String(value).replace(/[,\s%]+/g, "").trim();
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function getArray(value) {
  return Array.isArray(value) ? value : [];
}

function isProducingStatus(status) {
  const normalized = toSafeLower(status);
  return normalized === "on production" || normalized === "testing";
}

function buildDNMapByWell(dns) {
  const map = new Map();

  getArray(dns).forEach((dn) => {
    const wellId = toSafeString(dn?.well_id);
    if (!wellId) return;

    if (!map.has(wellId)) {
      map.set(wellId, []);
    }

    map.get(wellId).push(dn);
  });

  return map;
}

function getCoreEngineWellEntries(coreEngine) {
  if (!coreEngine || !(coreEngine.wellIndex instanceof Map)) return [];
  return Array.from(coreEngine.wellIndex.values());
}

function getCoreEngineDNEntries(coreEngine) {
  if (!coreEngine) return [];

  if (coreEngine.dnIndexById instanceof Map) {
    return Array.from(coreEngine.dnIndexById.values());
  }

  if (coreEngine.dnIndexByid instanceof Map) {
    return Array.from(coreEngine.dnIndexByid.values());
  }

  if (coreEngine.dnIndex instanceof Map) {
    return Array.from(coreEngine.dnIndex.values());
  }

  return [];
}

/* ====================================================================
   KPI / metric helpers
==================================================================== */

function calculateTotalRate(wellsData) {
  return getArray(wellsData)
    .filter((well) => isProducingStatus(well?.production_status))
    .reduce((sum, well) => sum + toNumber(well?.oil_rate_bopd, 0), 0);
}

function calculateHourlyAverage(history) {
  const safeHistory = getArray(history);
  if (safeHistory.length === 0) return 0;

  const latest = safeHistory[safeHistory.length - 1];
  return toNumber(latest?.bopd, 0);
}

function calculateDailyAverage(history) {
  const safeHistory = getArray(history);
  if (safeHistory.length === 0) return 0;

  const total = safeHistory.reduce((sum, item) => sum + toNumber(item?.bopd, 0), 0);
  return Math.round(total / safeHistory.length);
}

function calculateProductionDrop(history) {
  const safeHistory = getArray(history);

  if (safeHistory.length < 2) {
    return {
      latest: 0,
      previous: 0,
      delta: 0,
      percent: 0,
      direction: "stable"
    };
  }

  const sorted = [...safeHistory].sort(
    (a, b) => new Date(b.timestamp) - new Date(a.timestamp)
  );

  const latest = toNumber(sorted[0]?.bopd, 0);
  const previous = toNumber(sorted[1]?.bopd, 0);
  const delta = latest - previous;
  const percent = previous > 0 ? Number(((delta / previous) * 100).toFixed(1)) : 0;

  let direction = "stable";
  if (delta > 0) direction = "up";
  if (delta < 0) direction = "down";

  return { latest, previous, delta, percent, direction };
}

function calculateDNImpact(normalizedWells, latestDNsByWell) {
  const wellsList = getArray(normalizedWells);
  const dnByWell = latestDNsByWell instanceof Map ? latestDNsByWell : new Map();

  let lostProduction = 0;
  let affectedWells = 0;

  for (const well of wellsList) {
    const wellId = toSafeString(well?.well_id || well?.id);
    if (!wellId) continue;

    const status = toSafeLower(well?.production_status);
    const rate = toNumber(well?.oil_rate_bopd, 0);
    const wellDNs = getArray(dnByWell.get(wellId));

    const hasActiveDN = wellDNs.some((dn) => !dn?.is_closed);
    const isProducing = isProducingStatus(status);

    if (!isProducing && hasActiveDN) {
      lostProduction += rate;
      affectedWells += 1;
    }
  }

  return {
    lost_production: lostProduction,
    affected_wells: affectedWells
  };
}

function getTopAndLowWells(normalizedWells) {
  const producing = getArray(normalizedWells).filter((w) =>
    isProducingStatus(w?.production_status)
  );

  const sorted = [...producing].sort(
    (a, b) => toNumber(b?.oil_rate_bopd, 0) - toNumber(a?.oil_rate_bopd, 0)
  );

  return {
    top: sorted.slice(0, 3),
    low: sorted.slice(-3)
  };
}

/* ====================================================================
   Section builders
==================================================================== */

function buildOverviewSection(normalizedWells, latestDNs, productionHistory, monthlyTargetInput) {
  const safeWells = getArray(normalizedWells);
  const safeDNs = getArray(latestDNs);

  const totalRate = calculateTotalRate(safeWells);
  const hourlyAverage = calculateHourlyAverage(productionHistory);
  const dailyAverage = calculateDailyAverage(productionHistory);
  const monthlyTarget = toNumber(monthlyTargetInput, 0);
  const targetGap = totalRate - monthlyTarget;
  const activeDNCount = safeDNs.filter((dn) => !dn?.is_closed).length;

  const statusCounts = {
    on_production: 0,
    testing: 0,
    standby: 0,
    mothball: 0,
    shut_in: 0,
    locked_potential: 0
  };

  safeWells.forEach((w) => {
    const rawStatus = toSafeLower(w?.production_status);

    if (rawStatus.includes("locked")) {
      statusCounts.locked_potential += 1;
    } else if (rawStatus === "on production") {
      statusCounts.on_production += 1;
    } else if (rawStatus === "testing") {
      statusCounts.testing += 1;
    } else if (rawStatus === "standby") {
      statusCounts.standby += 1;
    } else if (rawStatus === "mothball" || rawStatus === "moth ball") {
      statusCounts.mothball += 1;
    } else if (rawStatus === "shut-in" || rawStatus === "shut in") {
      statusCounts.shut_in += 1;
    }
  });

  let ainDarRate = 0;
  let abqaiqRate = 0;

  safeWells.forEach((w) => {
    const rate = toNumber(w?.oil_rate_bopd, 0);
    const field = toSafeString(w?.field_code).toUpperCase();
    const includeInProduction = isProducingStatus(w?.production_status);

    if (!includeInProduction) return;

    if (field === "ANDR") {
      ainDarRate += rate;
    } else if (field === "ABQQ") {
      abqaiqRate += rate;
    }
  });

  const totalFieldRate = ainDarRate + abqaiqRate;
  const ainDarPercent =
    totalFieldRate > 0 ? Number(((ainDarRate / totalFieldRate) * 100).toFixed(1)) : 0;
  const abqaiqPercent =
    totalFieldRate > 0 ? Number(((abqaiqRate / totalFieldRate) * 100).toFixed(1)) : 0;

  const insights = [];

  if (monthlyTarget > 0) {
    if (targetGap < 0) {
      insights.push(`Production is below target by ${Math.abs(targetGap)} BOPD.`);
    } else if (targetGap > 0) {
      insights.push(`Production is above target by ${targetGap} BOPD.`);
    } else {
      insights.push("Production is exactly on target.");
    }
  }

  if (ainDarPercent > abqaiqPercent) {
    insights.push("Ain Dar field is the main contributor.");
  } else if (abqaiqPercent > ainDarPercent) {
    insights.push("Abqaiq field is the main contributor.");
  } else {
    insights.push("Field contributions are balanced.");
  }

  if (statusCounts.locked_potential > 0) {
    insights.push(`${statusCounts.locked_potential} wells have locked potential.`);
  }

  const trend = calculateProductionDrop(productionHistory);
  const dnMap = buildDNMapByWell(safeDNs);
  const dnImpact = calculateDNImpact(safeWells, dnMap);
  const performance = getTopAndLowWells(safeWells);

  return {
    kpis: {
      total_rate: totalRate,
      hourly_average: hourlyAverage,
      daily_average: dailyAverage,
      monthly_target: monthlyTarget,
      target_gap: targetGap,
      active_dn_count: activeDNCount
    },
    well_status: statusCounts,
    field_contribution: {
      ain_dar_rate: ainDarRate,
      abqaiq_rate: abqaiqRate,
      ain_dar_percent: ainDarPercent,
      abqaiq_percent: abqaiqPercent
    },
    insights,
    production_trend: trend,
    dn_impact: dnImpact,
    performance
  };
}

function buildProductionSection(normalizedWells, latestDNs) {
  return buildProductionIntelligence({
    wells: getArray(normalizedWells),
    dns: getArray(latestDNs)
  });
}

function buildDNSection(latestDNs, normalizedWells, latestDNsByWell) {
  const safeDNs = getArray(latestDNs);
  const safeWells = getArray(normalizedWells);

  const dnIntel = buildDNIntelligence({ dns: safeDNs });
  const dnImpact = calculateDNImpact(safeWells, latestDNsByWell);

  const dnByStatus = {
    open: 0,
    in_progress: 0,
    completed: 0,
    closed: 0,
    waiting: 0
  };

  safeDNs.forEach((dn) => {
    const ws = toSafeLower(dn?.workflow_status);

    if (ws === "closed") dnByStatus.closed += 1;
    else if (ws === "completed") dnByStatus.completed += 1;
    else if (ws === "in progress") dnByStatus.in_progress += 1;
    else if (ws === "waiting") dnByStatus.waiting += 1;
    else dnByStatus.open += 1;
  });

  const dnByOwner = {};

  safeDNs.forEach((dn) => {
    const owner = toSafeString(dn?.current_owner_name || "Unassigned");
    if (!dnByOwner[owner]) dnByOwner[owner] = 0;
    dnByOwner[owner] += 1;
  });

  const ownerList = Object.entries(dnByOwner)
    .map(([name, count]) => ({ name, dn_count: count }))
    .sort((a, b) => b.dn_count - a.dn_count);

  return {
    active_count: dnIntel?.active_count ?? 0,
    high_priority_count: dnIntel?.high_priority_count ?? 0,
    aged_count: dnIntel?.aged_count ?? 0,
    blocked_count: dnIntel?.blocked_count ?? 0,
    high_priority_dns: getArray(dnIntel?.high_priority_dns).slice(0, 5),
    aged_dns: getArray(dnIntel?.aged_dns).slice(0, 5),
    blocked_dns: getArray(dnIntel?.blocked_dns).slice(0, 5),
    by_owner: dnIntel?.by_owner || [],
    by_phase: dnIntel?.by_phase || [],
    bottleneck_owner: dnIntel?.bottleneck_owner || null,
    bottleneck_phase: dnIntel?.bottleneck_phase || null,
    insight: dnIntel?.insight || "",
    total_dn_count: safeDNs.length,
    dn_by_status: dnByStatus,
    dn_by_owner: ownerList.slice(0, 5),
    dn_impact: dnImpact
  };
}

function buildRiskSection(riskData) {
  if (!riskData) {
    return {
      top_risk_wells: [],
      top_risk_dns: [],
      risk_concentration: null,
      insight: ""
    };
  }

  return {
    top_risk_wells: getArray(riskData.top_risk_wells).slice(0, 10),
    top_risk_dns: getArray(riskData.top_risk_dns).slice(0, 10),
    risk_concentration: riskData.summary || riskData.risk_concentration || null,
    insight: riskData.insight || ""
  };
}

function buildRiskSectionFromEngine(coreEngine) {
  if (!coreEngine) {
    return buildRiskSection(null);
  }

  const wellsArr = getCoreEngineWellEntries(coreEngine);
  const dnsArr = getCoreEngineDNEntries(coreEngine);

  const riskDash = buildRiskDashboard(wellsArr, dnsArr);

  const dnRisks = dnsArr
    .map((dn) => {
      const risk = calculateDNRisk(dn);

      return {
        dn_id: dn.dn_id,
        well_id: dn.well_id,
        dn_type: dn.dn_type,
        priority: dn.priority,
        workflow_status: dn.workflow_status,
        current_step: dn.current_step,
        risk_score: risk.score,
        risk_level: risk.level,
        reasons: risk.reasons
      };
    })
    .sort((a, b) => b.risk_score - a.risk_score);

  const topRiskWells = getArray(riskDash?.top_risk_wells);
  const insight = topRiskWells.length
    ? `Highest risk well is ${topRiskWells[0].well_name}`
    : "";

  return {
    top_risk_wells: topRiskWells.slice(0, 10),
    top_risk_dns: dnRisks.slice(0, 10),
    risk_concentration: riskDash?.summary || null,
    insight
  };
}

function buildIntelligenceSection(intelligence) {
  if (!intelligence || typeof intelligence !== "object") {
    return {
      production: null,
      dn: null,
      risk: null,
      drop: null,
      overdue_kpi_impact: null,
      bottlenecks: null,
      recommendations: null,
      flags: [],
      summary: ""
    };
  }

  return {
    production: intelligence.production ?? null,
    dn: intelligence.dn ?? null,
    risk: intelligence.risk ?? null,
    drop: intelligence.drop ?? null,
    overdue_kpi_impact: intelligence.overdue_kpi_impact ?? null,
    bottlenecks: intelligence.bottlenecks ?? null,
    recommendations: intelligence.recommendations ?? null,
    flags: getArray(intelligence.flags),
    summary: intelligence.summary ?? ""
  };
}

/* ====================================================================
   Executive / field / well dashboards
==================================================================== */

function buildExecutiveDashboard({
  wells = [],
  dns = [],
  productionHistory = [],
  monthlyTarget = 0,
  coreEngine = null
}) {
  const normalizedWells = getArray(wells);
  const latestDNs = getArray(dns);
  const target = toNumber(monthlyTarget, 0);
  const dnMap = buildDNMapByWell(latestDNs);

  const overview = buildOverviewSection(normalizedWells, latestDNs, productionHistory, target);
  const production = buildProductionSection(normalizedWells, latestDNs, productionHistory);
  const dnSection = buildDNSection(latestDNs, normalizedWells, dnMap);

  let riskSection;
  let riskData;

  if (coreEngine) {
    riskSection = buildRiskSectionFromEngine(coreEngine);
    riskData = {
      top_risk_wells: getArray(riskSection.top_risk_wells),
      top_risk_dns: getArray(riskSection.top_risk_dns),
      summary: riskSection.risk_concentration || null,
      insight: riskSection.insight || ""
    };
  } else {
    riskData = buildRiskDashboard(normalizedWells, latestDNs);
    riskSection = buildRiskSection(riskData);
  }

  const baseIntelligence = buildSystemIntelligence({
    wells: normalizedWells,
    dns: latestDNs,
    riskData,
    riskItems: getArray(riskData?.top_risk_wells)
  });

  const intelligenceSection = buildIntelligenceSection(baseIntelligence);

  const recommendations = generateRecommendations({
    wells: normalizedWells,
    dns: latestDNs,
    risk: riskData,
    intelligence: baseIntelligence
  });

  const enhancements = enhanceIntelligence({
    wells: normalizedWells,
    dns: latestDNs,
    risk: riskData,
    intelligence: baseIntelligence
  });

  const kpis = {
    ...overview.kpis,
    total_dn_count: latestDNs.length,
    high_priority_dn_count: dnSection.high_priority_count,
    aged_dn_count: dnSection.aged_count,
    blocked_dn_count: dnSection.blocked_count,
    highest_risk_score:
      riskSection.top_risk_wells && riskSection.top_risk_wells[0]
        ? riskSection.top_risk_wells[0].risk_score
        : 0
  };

  const insights = [];
  if (Array.isArray(overview.insights)) insights.push(...overview.insights);
  if (riskSection.insight) insights.push(riskSection.insight);
  if (intelligenceSection.summary) insights.push(intelligenceSection.summary);

  const alerts = [];

  if (Array.isArray(baseIntelligence?.flags)) {
    baseIntelligence.flags.forEach((flag) => {
      alerts.push({
        type: flag.type || "flag",
        message: flag.message || String(flag)
      });
    });
  }

  if (Array.isArray(enhancements?.dn_production_impact)) {
    enhancements.dn_production_impact.forEach((item) => {
      alerts.push({
        type: "dn_production_impact",
        dn_id: item.dn_id,
        well_id: item.well_id,
        message: `DN ${item.dn_id} on ${item.well_name} estimated loss ${item.estimated_loss_bopd} BOPD`,
        value: item.estimated_loss_bopd
      });
    });
  }

  if (Array.isArray(enhancements?.anomalies)) {
    enhancements.anomalies.forEach((a) => {
      alerts.push({
        type: `anomaly_${a.issue}`,
        well_id: a.well_id,
        message: `Well ${a.well_name} has ${String(a.issue).replace(/_/g, " ")} at ${a.value} BOPD`,
        value: a.value
      });
    });
  }

  if (
    enhancements?.field_imbalance &&
    enhancements.field_imbalance.imbalance !== "balanced"
  ) {
    alerts.push({
      type: "field_imbalance",
      imbalance: enhancements.field_imbalance.imbalance,
      message: `Field imbalance: ${enhancements.field_imbalance.imbalance} (${enhancements.field_imbalance.difference_percent}% difference)`
    });
  }

  return {
    generated_at: new Date().toISOString(),
    summary: baseIntelligence?.summary || "",
    overview,
    production,
    dn: dnSection,
    risk: riskSection,
    intelligence: intelligenceSection,
    kpis,
    insights,
    recommendations,
    alerts,
    enhancements
  };
}

function buildFieldDashboard(
  fieldCode,
  { wells = [], dns = [], productionHistory = [], monthlyTarget = 0 }
) {
  const target = String(fieldCode ?? "").toUpperCase().trim();
  if (!target) return null;

  const allWells = getArray(wells);
  const fieldWells = allWells.filter(
    (w) => String(w.field_code || "").toUpperCase() === target
  );

  if (fieldWells.length === 0) return null;

  const wellIds = new Set(fieldWells.map((w) => String(w.well_id)));
  const allDns = getArray(dns);
  const fieldDns = allDns.filter((dn) => wellIds.has(String(dn.well_id)));
  const dnMap = buildDNMapByWell(fieldDns);

  const overview = buildOverviewSection(
    fieldWells,
    fieldDns,
    productionHistory,
    toNumber(monthlyTarget, 0)
  );

  const production = buildProductionSection(fieldWells, fieldDns, productionHistory);
  const dnSection = buildDNSection(fieldDns, fieldWells, dnMap);

  const riskData = buildRiskDashboard(fieldWells, fieldDns);
  const riskSection = buildRiskSection(riskData);

  const baseIntel = buildSystemIntelligence({
    wells: fieldWells,
    dns: fieldDns,
    riskData,
    riskItems: getArray(riskData?.top_risk_wells)
  });

  const intelligenceSection = buildIntelligenceSection(baseIntel);

  const recommendations = generateRecommendations({
    wells: fieldWells,
    dns: fieldDns,
    risk: riskData,
    intelligence: baseIntel
  });

  const enhancements = enhanceIntelligence({
    wells: fieldWells,
    dns: fieldDns,
    risk: riskData,
    intelligence: baseIntel
  });

  return {
    field_code: target,
    generated_at: new Date().toISOString(),
    overview,
    production,
    dn: dnSection,
    risk: riskSection,
    intelligence: intelligenceSection,
    recommendations,
    enhancements
  };
}

function buildWellDashboard(
  wellId,
  { wells = [], dns = [] }
) {
  const targetId = String(wellId ?? "").trim();
  if (!targetId) return null;

  const allWells = getArray(wells);
  const well = allWells.find((w) => String(w.well_id) === targetId);
  if (!well) return null;

  const allDns = getArray(dns);
  const wellDns = allDns.filter((dn) => String(dn.well_id) === targetId);

  let wellRisk = { score: 0, level: "LOW", reasons: [] };

  try {
    wellRisk = calculateWellRisk(well, wellDns);
  } catch (_error) {
    wellRisk = { score: 0, level: "LOW", reasons: [] };
  }

  const dnRisks = wellDns
    .map((dn) => {
      let risk = { score: 0, level: "LOW", reasons: [] };

      try {
        risk = calculateDNRisk(dn);
      } catch (_error) {
        risk = { score: 0, level: "LOW", reasons: [] };
      }

      return {
        dn_id: dn.dn_id,
        dn_type: dn.dn_type,
        priority: dn.priority,
        workflow_status: dn.workflow_status,
        current_step: dn.current_step,
        risk_score: risk.score,
        risk_level: risk.level,
        reasons: risk.reasons
      };
    })
    .sort((a, b) => b.risk_score - a.risk_score);

  const riskSection = {
    well_risk: {
      well_id: well.well_id,
      well_name: well.well_name,
      field_code: well.field_code,
      production_status: well.production_status,
      oil_rate_bopd: well.oil_rate_bopd,
      risk_score: wellRisk.score,
      risk_level: wellRisk.level,
      reasons: wellRisk.reasons
    },
    dn_risks: dnRisks.slice(0, 10)
  };

  const riskData = {
    top_risk_wells: [
      {
        well_id: well.well_id,
        well_name: well.well_name,
        field_code: well.field_code,
        production_status: well.production_status,
        oil_rate_bopd: well.oil_rate_bopd,
        risk_score: wellRisk.score,
        risk_level: wellRisk.level,
        reasons: wellRisk.reasons
      }
    ],
    top_risk_dns: dnRisks.slice(0, 10)
  };

  const intel = buildSystemIntelligence({
    wells: [well],
    dns: wellDns,
    riskData,
    riskItems: riskData.top_risk_wells
  });

  const intelligenceSection = buildIntelligenceSection(intel);

  const recommendations = generateRecommendations({
    wells: [well],
    dns: wellDns,
    risk: riskData,
    intelligence: intel
  });

  const enhancements = enhanceIntelligence({
    wells: [well],
    dns: wellDns,
    risk: riskData,
    intelligence: intel
  });

  return {
    well: {
      well_id: well.well_id,
      well_name: well.well_name,
      field_code: well.field_code,
      production_status: well.production_status,
      oil_rate_bopd: well.oil_rate_bopd,
      last_updated: well.last_updated
    },
    dn: wellDns,
    risk: riskSection,
    intelligence: intelligenceSection,
    recommendations,
    enhancements
  };
}

module.exports = {
  buildOverviewSection,
  buildProductionSection,
  buildDNSection,
  buildRiskSection,
  buildRiskSectionFromEngine,
  buildIntelligenceSection,
  buildExecutiveDashboard,
  buildFieldDashboard,
  buildWellDashboard
};
