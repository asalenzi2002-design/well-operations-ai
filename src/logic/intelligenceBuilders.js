"use strict";

const {
  normalizeFieldCode: normalizeDomainFieldCode,
  normalizeProductionStatus,
  isProducingStatus: isDomainProducingStatus,
  isDNResolvedStatus
} = require("../core/domain");
const {
  buildFormationOperationalSummary
} = require("./formationLineEngine");
const { enhanceIntelligence } = require("./intelligenceEnhancer");
const { buildExecutiveCommandLayer } = require("./recommendationEngine");

/* =========================================================
   Helpers
========================================================= */

const DAY_MS = 24 * 60 * 60 * 1000;
const HIGH_PRIORITY_SET = new Set(["high", "critical"]);
const PRODUCING_STATUSES = new Set(["on production", "testing"]);
const NON_PRODUCING_DROP_STATUSES = new Set([
  "shut-in",
  "shut in",
  "locked potential",
  "standby",
  "mothball",
  "moth ball"
]);

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function cleanText(value) {
  return String(value ?? "").trim();
}

function lower(value) {
  return cleanText(value).toLowerCase();
}

function toNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function round(value, digits = 1) {
  const num = toNumber(value, 0);
  const factor = 10 ** digits;
  return Math.round(num * factor) / factor;
}

function percent(part, whole, digits = 1) {
  if (!whole) return 0;
  return round((part / whole) * 100, digits);
}

function safeDate(value) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function daysSince(value, now = new Date()) {
  const date = safeDate(value);
  if (!date) return null;
  return Math.max(0, Math.floor((now - date) / DAY_MS));
}

function pickFirst(...values) {
  for (const value of values) {
    if (value !== undefined && value !== null && String(value).trim() !== "") {
      return value;
    }
  }
  return "";
}

function extractFieldCodeFromName(wellName) {
  const name = cleanText(wellName).toUpperCase();
  if (name.startsWith("ANDR-")) return "ANDR";
  if (name.startsWith("ABQQ-")) return "ABQQ";
  return "";
}

function normalizeFieldCode(well) {
  return normalizeDomainFieldCode(
    well?.field_code,
    well?.well_name || well?.name || "",
    well?.field
  );
}

function normalizeWell(well) {
  const wellName = cleanText(pickFirst(well?.well_name, well?.name));
  const productionStatus = cleanText(
    normalizeProductionStatus(
      pickFirst(well?.production_status, well?.status, "Unknown")
    )
  );
  const oilRate = toNumber(
    pickFirst(well?.oil_rate_bopd, well?.oil_rate, well?.oilrate, well?.rate, 0),
    0
  );

  return {
    ...well,
    well_id: cleanText(pickFirst(well?.well_id, well?.id)),
    well_name: wellName,
    field_code: normalizeFieldCode(well),
    production_status: productionStatus,
    oil_rate_bopd: oilRate
  };
}

function normalizeOwner(owner) {
  const raw = lower(owner);

  if (raw.includes("field")) return "Field Operations";
  if (raw.includes("foeu")) return "FOEU";
  if (raw.includes("cfc")) return "Maintenance (CFC)";
  if (raw.includes("crd") || raw.includes("planner")) return "Maintenance Planner (CRD)";
  if (raw.includes("inspection")) return "Inspection";

  return cleanText(owner) || "Unknown";
}

function normalizePhase(dn) {
  const step = lower(dn?.current_step);
  const status = lower(dn?.workflow_status || dn?.dn_status || dn?.status);

  if (step.includes("execution") || status.includes("execution") || status.includes("depressurizing")) {
    return "Execution";
  }

  if (step.includes("rfi") || status.includes("rfi")) {
    return "RFI";
  }

  if (
    step.includes("package") ||
    status.includes("package") ||
    status.includes("not issuing")
  ) {
    return "Package";
  }

  if (step.includes("review") || status.includes("review")) {
    return "Field Review";
  }

  if (status.includes("completed")) {
    return "Execution";
  }

  if (status.includes("closed")) {
    return "Closed";
  }

  return cleanText(dn?.current_step) || "Unknown";
}

function normalizeWorkflowStatus(rawStatus) {
  const status = lower(rawStatus);

  if (!status) return "Open";
  if (status.includes("closed")) return "Closed";
  if (status.includes("completed")) return "Completed";
  if (status.includes("waiting")) return "Waiting";
  if (status.includes("not issuing")) return "Waiting";
  if (status.includes("rfi")) return "In Progress";
  if (status.includes("execution")) return "In Progress";
  if (status.includes("depressurizing")) return "In Progress";

  return cleanText(rawStatus) || "Open";
}

function normalizeDN(dn) {
  const workflowStatus = normalizeWorkflowStatus(
    pickFirst(dn?.workflow_status, dn?.dn_status, dn?.status)
  );

  const progressPercent = toNumber(
    String(pickFirst(dn?.progress_percent, dn?.progress, 0)).replace("%", ""),
    0
  );

  return {
    ...dn,
    dn_id: cleanText(dn?.dn_id),
    well_id: cleanText(dn?.well_id),
    dn_type: cleanText(pickFirst(dn?.dn_type, dn?.type, "Unknown")),
    priority: cleanText(pickFirst(dn?.priority, "Unknown")),
    workflow_status: workflowStatus,
    current_step: normalizePhase(dn),
    current_owner_name: normalizeOwner(
      pickFirst(dn?.current_owner_name, dn?.dn_owner, dn?.owner, dn?.updated_by)
    ),
    progress_percent: progressPercent,
    created_date: cleanText(dn?.created_date),
    update_date: cleanText(pickFirst(dn?.update_date, dn?.last_updated)),
    is_closed: lower(workflowStatus) === "closed"
  };
}

function isProducingWell(well) {
  return isDomainProducingStatus(well?.production_status);
}

function isNonProducingDropWell(well) {
  return NON_PRODUCING_DROP_STATUSES.has(lower(normalizeProductionStatus(well?.production_status)));
}

function isActiveDN(dn) {
  return !isDNResolvedStatus(dn?.workflow_status || dn?.dn_status || dn?.status);
}

function isHighPriorityDN(dn) {
  return HIGH_PRIORITY_SET.has(lower(dn?.priority));
}

function isAgedDN(dn, now = new Date(), thresholdDays = 14) {
  const age = daysSince(pickFirst(dn?.update_date, dn?.created_date), now);
  return age !== null && age > thresholdDays;
}

function isBlockedDN(dn) {
  const status = lower(dn?.workflow_status || dn?.dn_status || dn?.status);
  const phase = lower(dn?.current_step);

  return (
    status.includes("waiting") ||
    status.includes("not issuing") ||
    status.includes("rfi") ||
    status.includes("hold") ||
    status.includes("delay") ||
    phase.includes("package") ||
    phase.includes("rfi")
  );
}

function isOverdueDN(dn, now = new Date(), thresholdDays = 120) {
  const age = daysSince(pickFirst(dn?.update_date, dn?.created_date), now);
  return age !== null && age > thresholdDays;
}

function buildWellMap(wells) {
  const map = new Map();
  for (const well of safeArray(wells)) {
    const normalized = normalizeWell(well);
    map.set(normalized.well_id, normalized);
  }
  return map;
}

function buildDNsByWell(dns) {
  const map = new Map();

  for (const rawDN of safeArray(dns)) {
    const dn = normalizeDN(rawDN);
    const wellId = dn.well_id;

    if (!map.has(wellId)) {
      map.set(wellId, []);
    }

    map.get(wellId).push(dn);
  }

  return map;
}

function sortDescBy(arr, selector) {
  return [...safeArray(arr)].sort((a, b) => selector(b) - selector(a));
}

function sortAscBy(arr, selector) {
  return [...safeArray(arr)].sort((a, b) => selector(a) - selector(b));
}

function groupCount(items, keyFn) {
  const map = new Map();

  for (const item of safeArray(items)) {
    const key = keyFn(item) || "Unknown";
    map.set(key, (map.get(key) || 0) + 1);
  }

  return Array.from(map.entries())
    .map(([key, count]) => ({ key, count }))
    .sort((a, b) => b.count - a.count);
}

function groupDNLoad(items, keyFn, now = new Date()) {
  const map = new Map();

  for (const item of safeArray(items)) {
    const key = keyFn(item) || "Unknown";

    if (!map.has(key)) {
      map.set(key, {
        name: key,
        total: 0,
        high_priority: 0,
        aged: 0,
        blocked: 0
      });
    }

    const bucket = map.get(key);
    bucket.total += 1;
    if (isHighPriorityDN(item)) bucket.high_priority += 1;
    if (isAgedDN(item, now)) bucket.aged += 1;
    if (isBlockedDN(item)) bucket.blocked += 1;
  }

  return Array.from(map.values()).sort((a, b) => {
    const scoreA = a.blocked * 3 + a.aged * 2 + a.high_priority * 2 + a.total;
    const scoreB = b.blocked * 3 + b.aged * 2 + b.high_priority * 2 + b.total;
    return scoreB - scoreA;
  });
}

function buildFieldBuckets() {
  return new Map([
    ["ANDR", {
      field_code: "ANDR",
      wells: [],
      active_dns: [],
      blocked_dns: [],
      aged_dns: [],
      high_priority_dns: [],
      high_risk_wells: [],
      recovery_wells: [],
      producing_rate_bopd: 0,
      deferred_rate_bopd: 0
    }],
    ["ABQQ", {
      field_code: "ABQQ",
      wells: [],
      active_dns: [],
      blocked_dns: [],
      aged_dns: [],
      high_priority_dns: [],
      high_risk_wells: [],
      recovery_wells: [],
      producing_rate_bopd: 0,
      deferred_rate_bopd: 0
    }]
  ]);
}

function dnActionScore(dn, well, now = new Date()) {
  const age = daysSince(pickFirst(dn?.update_date, dn?.created_date), now) || 0;
  const progress = toNumber(dn?.progress_percent, 0);
  const rate = toNumber(well?.oil_rate_bopd, 0);
  const blocked = isBlockedDN(dn) ? 25 : 0;
  const priority = isHighPriorityDN(dn) ? 35 : lower(dn?.priority) === "medium" ? 15 : 5;
  return rate * 0.05 + age + blocked + priority + ((100 - progress) / 5);
}

function recoveryScore(well, dns = [], now = new Date()) {
  const rate = toNumber(well?.oil_rate_bopd, 0);
  const blocked = dns.filter(isBlockedDN).length;
  const highPriority = dns.filter(isHighPriorityDN).length;
  const maxAge = Math.max(
    0,
    ...dns.map((dn) => daysSince(pickFirst(dn?.update_date, dn?.created_date), now) || 0)
  );
  return rate + blocked * 120 + highPriority * 90 + maxAge * 2;
}

function buildFieldOperationalIntelligence({ wells = [], dns = [], riskData = null }) {
  const now = new Date();
  const normalizedWells = safeArray(wells).map(normalizeWell);
  const normalizedDNs = safeArray(dns).map(normalizeDN);
  const dnsByWell = buildDNsByWell(normalizedDNs);
  const fieldBuckets = buildFieldBuckets();

  normalizedWells.forEach((well) => {
    const fieldCode = normalizeFieldCode(well);
    const bucket = fieldBuckets.get(fieldCode);
    if (!bucket) return;

    bucket.wells.push(well);

    if (isProducingWell(well)) {
      bucket.producing_rate_bopd += toNumber(well.oil_rate_bopd, 0);
    } else if (isNonProducingDropWell(well) && toNumber(well.oil_rate_bopd, 0) > 0) {
      const relatedActiveDNs = safeArray(dnsByWell.get(well.well_id)).filter(isActiveDN);
      if (relatedActiveDNs.length > 0) {
        bucket.deferred_rate_bopd += toNumber(well.oil_rate_bopd, 0);
        bucket.recovery_wells.push({
          ...formatWellLite(well),
          active_dn_count: relatedActiveDNs.length,
          blocked_dn_count: relatedActiveDNs.filter(isBlockedDN).length,
          high_priority_dn_count: relatedActiveDNs.filter(isHighPriorityDN).length,
          recovery_score: recoveryScore(well, relatedActiveDNs, now)
        });
      }
    }
  });

  normalizedDNs.filter(isActiveDN).forEach((dn) => {
    const well = normalizedWells.find((item) => item.well_id === dn.well_id);
    const fieldCode = normalizeFieldCode(well || { field_code: "UNKNOWN" });
    const bucket = fieldBuckets.get(fieldCode);
    if (!bucket) return;

    bucket.active_dns.push(dn);
    if (isBlockedDN(dn)) bucket.blocked_dns.push(dn);
    if (isAgedDN(dn, now)) bucket.aged_dns.push(dn);
    if (isHighPriorityDN(dn)) bucket.high_priority_dns.push(dn);
  });

  safeArray(riskData?.top_risk_wells).forEach((well) => {
    const fieldCode = normalizeFieldCode(well);
    const bucket = fieldBuckets.get(fieldCode);
    if (!bucket) return;
    bucket.high_risk_wells.push(well);
  });

  const result = {};
  for (const [fieldCode, bucket] of fieldBuckets.entries()) {
    const rankedDNs = topItems(bucket.active_dns, 3, (dn) => {
      const well = normalizedWells.find((item) => item.well_id === dn.well_id);
      return dnActionScore(dn, well, now);
    }).map((dn) => {
      const well = normalizedWells.find((item) => item.well_id === dn.well_id);
      return {
        ...formatDNLite(dn, now),
        well_name: well?.well_name || "",
        field_code: fieldCode,
        estimated_gain_bopd: toNumber(well?.oil_rate_bopd, 0)
      };
    });

    const rankedRecoveryWells = [...bucket.recovery_wells]
      .sort((a, b) => b.recovery_score - a.recovery_score)
      .slice(0, 3)
      .map((well) => ({
        ...well,
        estimated_gain_bopd: toNumber(well.oil_rate_bopd, 0)
      }));

    result[fieldCode] = {
      field_code: fieldCode,
      producing_rate_bopd: round(bucket.producing_rate_bopd, 1),
      deferred_rate_bopd: round(bucket.deferred_rate_bopd, 1),
      active_dn_count: bucket.active_dns.length,
      blocked_dn_count: bucket.blocked_dns.length,
      aged_dn_count: bucket.aged_dns.length,
      high_priority_dn_count: bucket.high_priority_dns.length,
      top_dns_to_act: rankedDNs,
      top_wells_to_recover: rankedRecoveryWells,
      high_risk_wells: bucket.high_risk_wells.slice(0, 3)
    };
  }

  return result;
}

function sumOilRate(wells) {
  return safeArray(wells).reduce((sum, well) => sum + toNumber(well.oil_rate_bopd, 0), 0);
}

function topItems(items, count, selector) {
  return sortDescBy(items, selector).slice(0, count);
}

function lowItems(items, count, selector) {
  return sortAscBy(items, selector).slice(0, count);
}

function uniqueReasons(riskItems) {
  const reasonCounts = new Map();

  for (const item of safeArray(riskItems)) {
    for (const reason of safeArray(item?.reasons)) {
      const key = cleanText(reason);
      if (!key) continue;
      reasonCounts.set(key, (reasonCounts.get(key) || 0) + 1);
    }
  }

  return Array.from(reasonCounts.entries())
    .map(([reason, count]) => ({ reason, count }))
    .sort((a, b) => b.count - a.count);
}

function inferLikelyDropCause(impactedWells, dnsByWell) {
  let blockedDNCount = 0;
  let highPriorityDNCount = 0;
  let shutInCount = 0;
  let lockedPotentialCount = 0;

  for (const well of safeArray(impactedWells)) {
    const dns = safeArray(dnsByWell.get(well.well_id));

    if (lower(well.production_status).includes("shut")) shutInCount += 1;
    if (lower(well.production_status).includes("locked")) lockedPotentialCount += 1;

    for (const dn of dns) {
      if (!isActiveDN(dn)) continue;
      if (isBlockedDN(dn)) blockedDNCount += 1;
      if (isHighPriorityDN(dn)) highPriorityDNCount += 1;
    }
  }

  if (blockedDNCount > 0 && shutInCount > 0) {
    return `Drop exposure is mainly linked to ${shutInCount} shut-in wells with ${blockedDNCount} blocked active DNs.`;
  }

  if (lockedPotentialCount > 0 && highPriorityDNCount > 0) {
    return `Deferred production is concentrated in locked potential wells tied to ${highPriorityDNCount} high-priority DNs.`;
  }

  if (shutInCount > 0) {
    return `Drop exposure is primarily coming from ${shutInCount} shut-in wells.`;
  }

  if (blockedDNCount > 0) {
    return `Workflow blockage is the strongest operational signal behind the current production exposure.`;
  }

  return "No strong drop driver was isolated from the current dataset.";
}

function formatWellLite(well) {
  return {
    well_id: well.well_id,
    well_name: well.well_name,
    field_code: well.field_code,
    production_status: well.production_status,
    oil_rate_bopd: well.oil_rate_bopd
  };
}

function formatDNLite(dn, now = new Date()) {
  return {
    dn_id: dn.dn_id,
    well_id: dn.well_id,
    dn_type: dn.dn_type,
    priority: dn.priority,
    workflow_status: dn.workflow_status,
    current_step: dn.current_step,
    current_owner_name: dn.current_owner_name,
    progress_percent: dn.progress_percent,
    age_days: daysSince(pickFirst(dn.update_date, dn.created_date), now)
  };
}

/* =========================================================
   Production Intelligence
========================================================= */

function buildProductionIntelligence({ wells = [], dns = [] }) {
  const normalizedWells = safeArray(wells).map(normalizeWell);
  const dnsByWell = buildDNsByWell(dns);
  const fieldIntel = buildFieldOperationalIntelligence({ wells: normalizedWells, dns });

  const producingWells = normalizedWells.filter(isProducingWell);
  const nonProducingExposureWells = normalizedWells.filter(
    (well) => isNonProducingDropWell(well) && toNumber(well.oil_rate_bopd, 0) > 0
  );

  const currentRate = sumOilRate(producingWells);
  const deferredRate = sumOilRate(nonProducingExposureWells);
  const previousEstimate = currentRate + deferredRate;
  const deltaBopd = currentRate - previousEstimate;
  const deltaPercent = previousEstimate > 0 ? round((deltaBopd / previousEstimate) * 100, 1) : 0;

  const strongestWells = topItems(producingWells, 3, (w) => w.oil_rate_bopd).map(formatWellLite);
  const weakestWells = lowItems(
    producingWells.filter((w) => w.oil_rate_bopd > 0),
    3,
    (w) => w.oil_rate_bopd
  ).map(formatWellLite);

  const andrProducing = producingWells.filter((w) => w.field_code === "ANDR");
  const abqqProducing = producingWells.filter((w) => w.field_code === "ABQQ");

  const andrRate = sumOilRate(andrProducing);
  const abqqRate = sumOilRate(abqqProducing);
  const totalFieldRate = andrRate + abqqRate;

  const andrPercent = percent(andrRate, totalFieldRate, 1);
  const abqqPercent = percent(abqqRate, totalFieldRate, 1);
  const fieldGapPercent = Math.abs(andrPercent - abqqPercent);

  let imbalance = "balanced";
  if (fieldGapPercent >= 20) {
    imbalance = andrPercent > abqqPercent ? "ANDR-heavy" : "ABQQ-heavy";
  }

  const topDeferredWells = topItems(nonProducingExposureWells, 3, (w) => w.oil_rate_bopd).map((well) => {
    const relatedDNs = safeArray(dnsByWell.get(well.well_id)).filter(isActiveDN);
    return {
      ...formatWellLite(well),
      active_dn_count: relatedDNs.length,
      blocked_dn_count: relatedDNs.filter(isBlockedDN).length
    };
  });

  let insight = "Production is stable based on current operational state.";

  if (deferredRate > 0) {
    const topDriver = topDeferredWells[0];
    insight = `Estimated production loss is ${deferredRate} BOPD, led by non-producing wells${topDriver ? ` such as ${topDriver.well_name}` : ""}.`;
  }

  if (imbalance !== "balanced") {
    insight += ` Field contribution is ${imbalance} (${andrPercent}% ANDR vs ${abqqPercent}% ABQQ).`;
  }

  const dominantDeferredField =
    toNumber(fieldIntel?.ANDR?.deferred_rate_bopd, 0) >= toNumber(fieldIntel?.ABQQ?.deferred_rate_bopd, 0)
      ? fieldIntel?.ANDR
      : fieldIntel?.ABQQ;
  if (dominantDeferredField && dominantDeferredField.deferred_rate_bopd > 0) {
    insight += ` Deferred opportunity is heaviest in ${dominantDeferredField.field_code} at ${dominantDeferredField.deferred_rate_bopd} BOPD.`;
  }

  return {
    current_rate_bopd: currentRate,
    estimated_previous_rate_bopd: previousEstimate,
    estimated_delta_bopd: deltaBopd,
    estimated_delta_percent: deltaPercent,
    strongest_wells: strongestWells,
    weakest_wells: weakestWells,
    field_balance: {
      andr_rate_bopd: andrRate,
      abqq_rate_bopd: abqqRate,
      andr_percent: andrPercent,
      abqq_percent: abqqPercent,
      imbalance
    },
    deferred_production_bopd: deferredRate,
    top_deferred_wells: topDeferredWells,
    insight
  };
}

/* =========================================================
   DN Intelligence
========================================================= */

function buildDNIntelligence({ dns = [] }) {
  const now = new Date();
  const normalizedDNs = safeArray(dns).map(normalizeDN);
  const activeDNs = normalizedDNs.filter(isActiveDN);
  const highPriorityDNs = activeDNs.filter(isHighPriorityDN);
  const agedDNs = activeDNs.filter((dn) => isAgedDN(dn, now, 14));
  const blockedDNs = activeDNs.filter(isBlockedDN);

  const ownerLoad = groupDNLoad(activeDNs, (dn) => dn.current_owner_name, now);
  const phaseLoad = groupDNLoad(activeDNs, (dn) => dn.current_step, now);

  const bottleneckOwner = ownerLoad[0] || null;
  const bottleneckPhase = phaseLoad[0] || null;

  let insight = "DN flow is under control.";

  if (blockedDNs.length > 0 || agedDNs.length > 0) {
    const ownerText = bottleneckOwner?.name ? ` under ${bottleneckOwner.name}` : "";
    const phaseText = bottleneckPhase?.name ? ` at ${bottleneckPhase.name}` : "";
    insight = `DN congestion is driven by ${blockedDNs.length} blocked and ${agedDNs.length} aged items${ownerText}${phaseText}.`;
  } else if (highPriorityDNs.length > 0) {
    insight = `${highPriorityDNs.length} high-priority DNs remain open and should be cleared first.`;
  }

  const byField = groupDNLoad(activeDNs, (dn) => {
    const fieldPrefix = cleanText(dn?.well_id).toUpperCase();
    if (fieldPrefix.startsWith("ANDR-")) return "ANDR";
    if (fieldPrefix.startsWith("ABQQ-")) return "ABQQ";
    return "UNKNOWN";
  }, now);
  const topField = byField[0] || null;
  if (topField && topField.name !== "UNKNOWN") {
    insight += ` ${topField.name} carries the highest active DN load (${topField.total} items).`;
  }

  return {
    active_count: activeDNs.length,
    high_priority_count: highPriorityDNs.length,
    aged_count: agedDNs.length,
    blocked_count: blockedDNs.length,
    high_priority_dns: topItems(highPriorityDNs, 5, (dn) => {
      const age = daysSince(pickFirst(dn.update_date, dn.created_date), now) || 0;
      return age + dn.progress_percent;
    }).map((dn) => formatDNLite(dn, now)),
    aged_dns: topItems(agedDNs, 5, (dn) => daysSince(pickFirst(dn.update_date, dn.created_date), now) || 0)
      .map((dn) => formatDNLite(dn, now)),
    blocked_dns: topItems(blockedDNs, 5, (dn) => {
      const age = daysSince(pickFirst(dn.update_date, dn.created_date), now) || 0;
      return age + (isHighPriorityDN(dn) ? 20 : 0);
    }).map((dn) => formatDNLite(dn, now)),
    by_field: byField,
    by_owner: ownerLoad,
    by_phase: phaseLoad,
    bottleneck_owner: bottleneckOwner,
    bottleneck_phase: bottleneckPhase,
    insight
  };
}

/* =========================================================
   Risk Intelligence
========================================================= */

function buildRiskIntelligence({ riskData = null, riskItems = [] }) {
  const topRiskWells = safeArray(riskData?.top_risk_wells).length
    ? safeArray(riskData.top_risk_wells)
    : safeArray(riskItems);

  const topRiskDNs = safeArray(riskData?.top_risk_dns);

  const fieldCounts = groupCount(topRiskWells, (item) => cleanText(item.field_code) || "UNKNOWN");
  const dominantField = fieldCounts[0] || null;

  const reasonCounts = uniqueReasons(topRiskWells);
  const topReason = reasonCounts[0] || null;

  const blockedLikeCount = safeArray(topRiskWells).filter((item) =>
    safeArray(item.reasons).some((reason) => {
      const text = lower(reason);
      return text.includes("blocked") || text.includes("waiting") || text.includes("rfi");
    })
  ).length;

  const highLevelCount = safeArray(topRiskWells).filter(
    (item) => lower(item.risk_level) === "high" || toNumber(item.risk_score, 0) >= 70
  ).length;

  let insight = "Risk distribution is moderate.";

  if (dominantField && dominantField.count >= Math.ceil(Math.max(topRiskWells.length, 1) * 0.6)) {
    insight = `Risk is concentrated in ${dominantField.key}, led by repeated ${topReason?.reason || "operational"} issues.`;
  } else if (topReason) {
    insight = `Top risk driver is ${topReason.reason}, repeated across ${topReason.count} high-risk wells.`;
  }

  if (blockedLikeCount > 0) {
    insight += ` ${blockedLikeCount} high-risk wells show blockage-related signals.`;
  }

  return {
    top_risk_wells: topRiskWells.slice(0, 10),
    top_risk_dns: topRiskDNs.slice(0, 10),
    risk_concentration: {
      by_field: fieldCounts,
      dominant_field: dominantField,
      repeated_reasons: reasonCounts.slice(0, 5),
      blocked_related_count: blockedLikeCount,
      high_level_count: highLevelCount
    },
    insight
  };
}

/* =========================================================
   Drop Intelligence
========================================================= */

function calculateDNProductionImpact({ wells = [], dns = [] }) {
  const normalizedWells = safeArray(wells).map(normalizeWell);
  const normalizedDNs = safeArray(dns).map(normalizeDN);
  const dnsByWell = buildDNsByWell(normalizedDNs);

  const impactedWells = normalizedWells
    .filter((well) => isNonProducingDropWell(well) && toNumber(well.oil_rate_bopd, 0) > 0)
    .map((well) => {
      const relatedDNs = safeArray(dnsByWell.get(well.well_id)).filter(isActiveDN);
      const blockedCount = relatedDNs.filter(isBlockedDN).length;
      const highPriorityCount = relatedDNs.filter(isHighPriorityDN).length;

      return {
        ...formatWellLite(well),
        active_dn_count: relatedDNs.length,
        blocked_dn_count: blockedCount,
        high_priority_dn_count: highPriorityCount,
        linked_dns: relatedDNs.slice(0, 5).map((dn) => formatDNLite(dn))
      };
    });

  impactedWells.sort((a, b) => b.oil_rate_bopd - a.oil_rate_bopd);

  return {
    impacted_wells: impactedWells,
    estimated_lost_bopd: impactedWells.reduce((sum, well) => sum + toNumber(well.oil_rate_bopd, 0), 0),
    impacted_well_count: impactedWells.length
  };
}

function buildDropIntelligence({ wells = [], dns = [], riskData = null }) {
  const normalizedDNs = safeArray(dns).map(normalizeDN);
  const dnsByWell = buildDNsByWell(normalizedDNs);
  const dnImpact = calculateDNProductionImpact({ wells, dns: normalizedDNs });
  const fieldIntel = buildFieldOperationalIntelligence({ wells, dns: normalizedDNs, riskData });

  const linkedWellCount = dnImpact.impacted_wells.filter((well) => well.active_dn_count > 0).length;
  const blockedLinkedWellCount = dnImpact.impacted_wells.filter((well) => well.blocked_dn_count > 0).length;

  const riskTop = safeArray(riskData?.top_risk_wells);
  const impactedRiskWells = dnImpact.impacted_wells.filter((well) =>
    riskTop.some((riskWell) => cleanText(riskWell.well_id) === cleanText(well.well_id))
  );

  const likelyCause = inferLikelyDropCause(dnImpact.impacted_wells, dnsByWell);
  const topRecoveryField =
    toNumber(fieldIntel?.ANDR?.deferred_rate_bopd, 0) >= toNumber(fieldIntel?.ABQQ?.deferred_rate_bopd, 0)
      ? fieldIntel?.ANDR
      : fieldIntel?.ABQQ;

  return {
    estimated_lost_bopd: dnImpact.estimated_lost_bopd,
    impacted_well_count: dnImpact.impacted_well_count,
    dn_linked_well_count: linkedWellCount,
    blocked_dn_linked_well_count: blockedLinkedWellCount,
    impacted_wells: dnImpact.impacted_wells.slice(0, 5),
    high_risk_impacted_wells: impactedRiskWells.slice(0, 5),
    likely_cause:
      topRecoveryField && topRecoveryField.deferred_rate_bopd > 0
        ? `${likelyCause} Recovery upside is currently highest in ${topRecoveryField.field_code}.`
        : likelyCause
  };
}

/* =========================================================
   Overdue KPI Impact (PHASE 5)
========================================================= */

function buildOverdueKPIImpact({ wells = [], dns = [] }) {
  const now = new Date();
  const normalizedWells = safeArray(wells).map(normalizeWell);
  const normalizedDNs = safeArray(dns).map(normalizeDN);
  const dnsByWell = buildDNsByWell(normalizedDNs);

  const candidateWells = normalizedWells.filter(
    (well) => isNonProducingDropWell(well) && toNumber(well.oil_rate_bopd, 0) > 0
  );

  const overdueRecords = [];
  const byOwnerMap = new Map();
  const byFieldMap = new Map();
  let totalOverdueDnCount = 0;
  let totalOverdueLossBopd = 0;

  for (const well of candidateWells) {
    const relatedDNs = safeArray(dnsByWell.get(well.well_id)).filter((dn) => isOverdueDN(dn, now));
    if (!relatedDNs.length) continue;

    const estimatedLoss = toNumber(well.oil_rate_bopd, 0);
    const ownersMap = new Map();
    const highestPriority = relatedDNs.some(isHighPriorityDN) ? "High" : relatedDNs[0]?.priority || "Unknown";
    const maxAgeDays = Math.max(
      ...relatedDNs.map((dn) => daysSince(pickFirst(dn.update_date, dn.created_date), now) || 0)
    );
    const blockedDnCount = relatedDNs.filter(isBlockedDN).length;

    for (const dn of relatedDNs) {
      const ownerKey = dn.current_owner_name || "Unknown";
      ownersMap.set(ownerKey, (ownersMap.get(ownerKey) || 0) + 1);
    }

    const ownerBreakdown = Array.from(ownersMap.entries())
      .map(([owner, dn_count]) => {
        const attributedLoss = relatedDNs.length > 0
          ? round((estimatedLoss * dn_count) / relatedDNs.length, 1)
          : 0;

        return {
          owner,
          dn_count,
          attributed_loss_bopd: attributedLoss
        };
      })
      .sort((a, b) => b.attributed_loss_bopd - a.attributed_loss_bopd);

    const leadOwner = ownerBreakdown[0]?.owner || "Unknown";

    const record = {
      well_name: well.well_name,
      well_id: well.well_id,
      field_code: well.field_code,
      production_status: well.production_status,
      estimated_loss_bopd: estimatedLoss,
      overdue_dn_count: relatedDNs.length,
      blocked_overdue_dn_count: blockedDnCount,
      lead_owner: leadOwner,
      priority: highestPriority,
      age_days: maxAgeDays,
      dn_ids: relatedDNs.map((dn) => dn.dn_id),
      owners: ownerBreakdown.map((item) => item.owner),
      owner_breakdown: ownerBreakdown
    };

    overdueRecords.push(record);
    totalOverdueDnCount += relatedDNs.length;
    totalOverdueLossBopd += estimatedLoss;

    const fieldKey = well.field_code || "UNKNOWN";
    if (!byFieldMap.has(fieldKey)) {
      byFieldMap.set(fieldKey, {
        field: fieldKey,
        well_count: 0,
        dn_count: 0,
        total_loss_bopd: 0
      });
    }

    const fieldBucket = byFieldMap.get(fieldKey);
    fieldBucket.well_count += 1;
    fieldBucket.dn_count += relatedDNs.length;
    fieldBucket.total_loss_bopd += estimatedLoss;

    for (const ownerItem of ownerBreakdown) {
      const ownerKey = ownerItem.owner || "Unknown";
      if (!byOwnerMap.has(ownerKey)) {
        byOwnerMap.set(ownerKey, {
          owner: ownerKey,
          well_count: 0,
          dn_count: 0,
          total_loss_bopd: 0
        });
      }

      const ownerBucket = byOwnerMap.get(ownerKey);
      ownerBucket.dn_count += ownerItem.dn_count;
      ownerBucket.total_loss_bopd += ownerItem.attributed_loss_bopd;
    }

    for (const ownerName of ownerBreakdown.map((item) => item.owner)) {
      const ownerBucket = byOwnerMap.get(ownerName);
      if (ownerBucket) {
        ownerBucket.well_count += 1;
      }
    }
  }

  const byOwner = Array.from(byOwnerMap.values())
    .map((item) => ({
      ...item,
      total_loss_bopd: round(item.total_loss_bopd, 1)
    }))
    .sort((a, b) => b.total_loss_bopd - a.total_loss_bopd);

  const byField = Array.from(byFieldMap.values())
    .map((item) => ({
      ...item,
      total_loss_bopd: round(item.total_loss_bopd, 1)
    }))
    .sort((a, b) => b.total_loss_bopd - a.total_loss_bopd);

  const topOverdueWells = topItems(overdueRecords, 5, (rec) => rec.estimated_loss_bopd);

  return {
    total_overdue_dn_count: totalOverdueDnCount,
    affected_well_count: overdueRecords.length,
    total_overdue_loss_bopd: round(totalOverdueLossBopd, 1),
    by_owner: byOwner,
    by_field: byField,
    top_overdue_wells: topOverdueWells,
    overdue_records: overdueRecords
  };
}

/* =========================================================
   Bottleneck Intelligence (PHASE 5)
========================================================= */

function buildBottleneckIntelligence({ wells = [], dns = [], overdueKpiImpact = null }) {
  const normalizedDNs = safeArray(dns).map(normalizeDN);
  const overdueData = overdueKpiImpact || buildOverdueKPIImpact({ wells, dns });
  const overdueOpenDNs = normalizedDNs.filter((dn) => isActiveDN(dn) && isOverdueDN(dn));

  const topOwnerByOverdueCount = overdueData.by_owner.length
    ? overdueData.by_owner.reduce((prev, curr) => (curr.dn_count > prev.dn_count ? curr : prev))
    : null;

  const topOwnerByLoss = overdueData.by_owner.length ? overdueData.by_owner[0] : null;

  const blockingStatuses = new Map();
  for (const dn of overdueOpenDNs) {
    const statusKey = dn.workflow_status || "Unknown";
    if (isBlockedDN(dn)) {
      blockingStatuses.set(statusKey, (blockingStatuses.get(statusKey) || 0) + 1);
    }
  }

  const topStatusBlockers = Array.from(blockingStatuses.entries())
    .map(([status, count]) => ({ status, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 3);

  const criticalBottlenecks = [];

  if (topOwnerByOverdueCount) {
    criticalBottlenecks.push({
      type: "overdue_owner_backlog",
      owner: topOwnerByOverdueCount.owner,
      dn_count: topOwnerByOverdueCount.dn_count,
      total_loss_bopd: round(topOwnerByOverdueCount.total_loss_bopd, 1),
      message: `${topOwnerByOverdueCount.owner} carries the largest overdue DN backlog (${topOwnerByOverdueCount.dn_count} items).`
    });
  }

  if (topOwnerByLoss) {
    criticalBottlenecks.push({
      type: "overdue_owner_loss",
      owner: topOwnerByLoss.owner,
      dn_count: topOwnerByLoss.dn_count,
      total_loss_bopd: round(topOwnerByLoss.total_loss_bopd, 1),
      message: `${topOwnerByLoss.owner} is tied to the highest overdue production exposure (${round(topOwnerByLoss.total_loss_bopd, 1)} BOPD).`
    });
  }

  if (topStatusBlockers.length > 0) {
    const topBlocker = topStatusBlockers[0];
    criticalBottlenecks.push({
      type: "blocking_status",
      status: topBlocker.status,
      dn_count: topBlocker.count,
      total_loss_bopd: 0,
      message: `${topBlocker.count} overdue active DNs are stalled in ${topBlocker.status} status.`
    });
  }

  if (overdueData.total_overdue_loss_bopd > 0) {
    criticalBottlenecks.push({
      type: "overdue_production_loss",
      owner: topOwnerByLoss?.owner || "Unknown",
      dn_count: overdueData.total_overdue_dn_count,
      total_loss_bopd: round(overdueData.total_overdue_loss_bopd, 1),
      message: `Total overdue exposure is ${round(overdueData.total_overdue_loss_bopd, 1)} BOPD across ${overdueData.affected_well_count} wells.`
    });
  }

  return {
    top_owner_by_overdue_count: topOwnerByOverdueCount,
    top_owner_by_loss: topOwnerByLoss,
    top_status_blockers: topStatusBlockers,
    critical_bottlenecks: criticalBottlenecks.slice(0, 5)
  };
}

/* =========================================================
   Action Recommendations (PHASE 5)
========================================================= */

function buildActionRecommendations({ wells = [], dns = [], overdueKpiImpact = null, bottlenecks = null }) {
  const recommendations = [];

  const overdueData = overdueKpiImpact || buildOverdueKPIImpact({ wells, dns });
  const bottleneckData = bottlenecks || buildBottleneckIntelligence({ wells, dns, overdueKpiImpact: overdueData });
  const fieldIntel = buildFieldOperationalIntelligence({ wells, dns });

  const topOverdueWell = overdueData.top_overdue_wells.length ? overdueData.top_overdue_wells[0] : null;
  if (topOverdueWell && topOverdueWell.estimated_loss_bopd > 0) {
    recommendations.push(
      `Prioritize ${topOverdueWell.well_name}; overdue work is holding ${topOverdueWell.estimated_loss_bopd} BOPD with ${topOverdueWell.overdue_dn_count} overdue DNs.`
    );
  }

  const topOwnerByLoss = bottleneckData.top_owner_by_loss;
  if (topOwnerByLoss && topOwnerByLoss.total_loss_bopd > 0) {
    recommendations.push(
      `Escalate ${topOwnerByLoss.owner}; it carries the highest overdue exposure at ${round(topOwnerByLoss.total_loss_bopd, 1)} BOPD.`
    );
  }

  const topOwnerByCount = bottleneckData.top_owner_by_overdue_count;
  if (
    topOwnerByCount &&
    (!topOwnerByLoss || topOwnerByCount.owner !== topOwnerByLoss.owner) &&
    topOwnerByCount.dn_count >= 2
  ) {
    recommendations.push(
      `Reduce ${topOwnerByCount.owner} backlog first; it holds the largest overdue DN count (${topOwnerByCount.dn_count}).`
    );
  }

  const topBlockingStatus = bottleneckData.top_status_blockers.length ? bottleneckData.top_status_blockers[0] : null;
  if (topBlockingStatus && topBlockingStatus.count >= 2) {
    recommendations.push(
      `Clear ${topBlockingStatus.status} blockage; ${topBlockingStatus.count} overdue active DNs are stuck at this status.`
    );
  }

  const topField = overdueData.by_field.length ? overdueData.by_field[0] : null;
  if (topField && topField.total_loss_bopd > 0) {
    recommendations.push(
      `Focus recovery in ${topField.field}; it carries ${round(topField.total_loss_bopd, 1)} BOPD of overdue-related loss.`
    );
  }

  const rankedFieldDNs = ["ANDR", "ABQQ"]
    .flatMap((fieldCode) => safeArray(fieldIntel?.[fieldCode]?.top_dns_to_act))
    .sort((a, b) => toNumber(b.estimated_gain_bopd, 0) - toNumber(a.estimated_gain_bopd, 0));
  const topDN = rankedFieldDNs[0] || null;
  if (topDN && topDN.dn_id) {
    recommendations.push(
      `Act on DN ${topDN.dn_id} in ${topDN.field_code}; it is tied to ${topDN.well_name} and protects about ${round(topDN.estimated_gain_bopd, 1)} BOPD.`
    );
  }

  const rankedRecoveryWells = ["ANDR", "ABQQ"]
    .flatMap((fieldCode) => safeArray(fieldIntel?.[fieldCode]?.top_wells_to_recover))
    .sort((a, b) => toNumber(b.estimated_gain_bopd, 0) - toNumber(a.estimated_gain_bopd, 0));
  const topRecoveryWell = rankedRecoveryWells[0] || null;
  if (topRecoveryWell && topRecoveryWell.well_name) {
    recommendations.push(
      `Recover ${topRecoveryWell.well_name} first; it offers about ${round(topRecoveryWell.estimated_gain_bopd, 1)} BOPD with ${topRecoveryWell.active_dn_count} active DNs attached.`
    );
  }

  return recommendations.slice(0, 5);
}

/* =========================================================
   Flags Builder
========================================================= */

function buildFlags({ production, dn, risk, drop }) {
  const flags = [];

  if (dn.active_count >= 10) {
    flags.push("High DN backlog");
  }

  if (dn.blocked_count >= 3) {
    flags.push("Blocked workflow detected");
  }

  if (dn.aged_count >= 5) {
    flags.push("Multiple aged DNs");
  }

  if (
    risk.risk_concentration?.dominant_field?.key &&
    risk.risk_concentration?.dominant_field?.count >= Math.ceil(Math.max(safeArray(risk.top_risk_wells).length, 1) * 0.6)
  ) {
    flags.push(`High-risk concentration in ${risk.risk_concentration.dominant_field.key}`);
  }

  if (drop.estimated_lost_bopd > 0) {
    flags.push("Deferred production exposure");
  }

  if (drop.blocked_dn_linked_well_count >= 2) {
    flags.push("Drop linked to blocked DNs");
  }

  if (production.field_balance?.imbalance && production.field_balance.imbalance !== "balanced") {
    flags.push(`Field imbalance: ${production.field_balance.imbalance}`);
  }

  if (safeArray(risk.top_risk_wells).filter((item) => lower(item.risk_level) === "high").length >= 3) {
    flags.push("Multiple high-risk wells");
  }

  return flags.slice(0, 8);
}

/* =========================================================
   Executive Summary
========================================================= */

function buildExecutiveSummary({ production, dn, risk, drop, flags, overdueKpiImpact, bottlenecks, fieldIntelligence = null, executiveCommand = null, enhancements = null }) {
  const lines = [];

  const overdueData = overdueKpiImpact || { total_overdue_dn_count: 0, total_overdue_loss_bopd: 0 };
  const bottleneckData = bottlenecks || { top_owner_by_loss: null, top_status_blockers: [] };

  const deltaText =
    production.estimated_delta_bopd < 0
      ? `Estimated production is down by ${Math.abs(production.estimated_delta_bopd)} BOPD from recoverable capacity.`
      : `Current production is holding at ${production.current_rate_bopd} BOPD.`;

  lines.push(deltaText);

  if (overdueData.total_overdue_dn_count > 0) {
    const ownerText = bottleneckData.top_owner_by_loss?.owner
      ? `, led by ${bottleneckData.top_owner_by_loss.owner}`
      : "";
    lines.push(
      `Critical overdue exposure stands at ${round(overdueData.total_overdue_loss_bopd, 1)} BOPD across ${overdueData.affected_well_count} wells${ownerText}.`
    );
  } else if (dn.blocked_count > 0 || dn.aged_count > 0) {
    const owner = dn.bottleneck_owner?.name ? ` under ${dn.bottleneck_owner.name}` : "";
    lines.push(`Main issue is DN congestion with ${dn.blocked_count} blocked and ${dn.aged_count} aged items${owner}.`);
  } else {
    lines.push(`Main issue is open DN load with ${dn.active_count} active items.`);
  }

  if (risk.risk_concentration?.dominant_field?.key) {
    lines.push(
      `Main risk is concentrated in ${risk.risk_concentration.dominant_field.key}${risk.risk_concentration.repeated_reasons?.[0]?.reason ? ` around ${risk.risk_concentration.repeated_reasons[0].reason}` : ""}.`
    );
  } else {
    lines.push("Main risk is distributed with no single dominant field.");
  }

  const andr = fieldIntelligence?.ANDR || null;
  const abqq = fieldIntelligence?.ABQQ || null;
  if (andr && abqq) {
    const andrSignal = toNumber(andr.deferred_rate_bopd, 0) + toNumber(andr.active_dn_count, 0) * 25;
    const abqqSignal = toNumber(abqq.deferred_rate_bopd, 0) + toNumber(abqq.active_dn_count, 0) * 25;
    if (andrSignal > 0 || abqqSignal > 0) {
      const dominant = andrSignal >= abqqSignal ? "ANDR" : "ABQQ";
      lines[2] = `${lines[2]} Field-specific pressure is highest in ${dominant}.`;
    }
  }

  if (overdueData.top_overdue_wells?.[0]?.well_name) {
    const topWell = overdueData.top_overdue_wells[0];
    lines.push(
      `Primary focus should be ${topWell.well_name}, where overdue work is holding ${topWell.estimated_loss_bopd} BOPD.`
    );
  } else if (drop.estimated_lost_bopd > 0) {
    lines.push(
      `Focus on recovering ${drop.estimated_lost_bopd} BOPD tied to non-producing wells and clearing blocked high-priority work.`
    );
  } else if (flags.length > 0) {
    lines.push(`Focus on ${flags[0].toLowerCase()}.`);
  } else {
    lines.push("Focus on maintaining flow and preventing backlog growth.");
  }

  if (executiveCommand?.mode?.mode) {
    lines[0] = `${lines[0]} Command mode is ${executiveCommand.mode.mode}.`;
  }

  if (executiveCommand?.top_action?.title) {
    lines[3] = `Top command action is ${executiveCommand.top_action.title} in ${executiveCommand.top_action.field_code}, where acting now protects or unlocks about ${executiveCommand.top_action.estimated_impact_bopd} BOPD.`;
  } else if (enhancements?.executive_signals?.biggest_operational_pressure_field) {
    lines[3] = `Top command focus should remain on ${enhancements.executive_signals.biggest_operational_pressure_field}, where operational pressure is highest in the current data.`;
  }

  return lines.slice(0, 4).join(" ");
}

/* =========================================================
   buildSystemIntelligence (main function)
========================================================= */

function attachProductionImpact(riskItems = [], wells = []) {
  const wellMap = buildWellMap(wells);

  return safeArray(riskItems).map((item) => {
    const well = wellMap.get(cleanText(item.well_id));
    return {
      ...item,
      oil_rate_bopd: toNumber(item.oil_rate_bopd, toNumber(well?.oil_rate_bopd, 0)),
      production_status: cleanText(item.production_status || well?.production_status)
    };
  });
}

function enrichRiskWithDN(riskItems = [], dns = []) {
  const dnsByWell = buildDNsByWell(dns);

  return safeArray(riskItems).map((item) => {
    const related = safeArray(dnsByWell.get(cleanText(item.well_id))).filter(isActiveDN);

    return {
      ...item,
      active_dn_count: related.length,
      blocked_dn_count: related.filter(isBlockedDN).length,
      high_priority_dn_count: related.filter(isHighPriorityDN).length
    };
  });
}

function buildSystemIntelligence(payload = {}) {
  const wells = safeArray(payload.wells).map(normalizeWell);
  const dns = safeArray(payload.dns).map(normalizeDN);
  const formationProjects = safeArray(payload.formationProjects);
  const formationTasks = safeArray(payload.formationTasks);

  const riskData = payload.riskData || {};
  const enrichedRiskItems = enrichRiskWithDN(
    attachProductionImpact(payload.riskItems || riskData?.top_risk_wells || [], wells),
    dns
  );

  const mergedRiskData = {
    ...riskData,
    top_risk_wells: safeArray(riskData?.top_risk_wells).length
      ? enrichRiskWithDN(attachProductionImpact(riskData.top_risk_wells, wells), dns)
      : enrichedRiskItems
  };

  const production = buildProductionIntelligence({ wells, dns });
  const dn = buildDNIntelligence({ dns });
  const formation = buildFormationOperationalSummary(formationProjects, formationTasks);
  const risk = buildRiskIntelligence({ riskData: mergedRiskData, riskItems: enrichedRiskItems });
  const drop = buildDropIntelligence({ wells, dns, riskData: mergedRiskData });
  const fieldIntelligence = buildFieldOperationalIntelligence({ wells, dns, riskData: mergedRiskData });

  const overdueKpiImpact = buildOverdueKPIImpact({ wells, dns });
  const bottlenecks = buildBottleneckIntelligence({ wells, dns, overdueKpiImpact });
  const recommendations = buildActionRecommendations({ wells, dns, overdueKpiImpact, bottlenecks });
  const preliminary = {
    production,
    dn,
    formation,
    risk,
    drop,
    overdue_kpi_impact: overdueKpiImpact,
    bottlenecks,
    recommendations,
    field_intelligence: fieldIntelligence
  };
  const enhancements = enhanceIntelligence({
    wells,
    dns,
    risk: mergedRiskData,
    intelligence: preliminary
  });
  const executiveCommand = buildExecutiveCommandLayer({
    risk: mergedRiskData,
    intelligence: preliminary,
    formationProjects,
    formationTasks,
    enhancements
  });
  const flags = buildFlags({ production, dn, risk, drop });
  const summary = buildExecutiveSummary({
    production,
    dn,
    risk,
    drop,
    flags,
    overdueKpiImpact,
    bottlenecks,
    fieldIntelligence,
    executiveCommand,
    enhancements
  });

  return {
    production,
    dn,
    formation,
    risk,
    drop,
    overdue_kpi_impact: overdueKpiImpact,
    bottlenecks,
    recommendations,
    field_intelligence: fieldIntelligence,
    enhancements,
    executive_command: executiveCommand,
    flags,
    summary
  };
}

module.exports = {
  calculateDNProductionImpact,
  attachProductionImpact,
  enrichRiskWithDN,
  buildProductionIntelligence,
  buildDNIntelligence,
  buildRiskIntelligence,
  buildDropIntelligence,
  buildOverdueKPIImpact,
  buildBottleneckIntelligence,
  buildActionRecommendations,
  buildFlags,
  buildExecutiveSummary,
  buildSystemIntelligence
};
