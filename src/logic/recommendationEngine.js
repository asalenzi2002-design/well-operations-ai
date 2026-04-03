"use strict";

/*
 * Recommendation Engine (Phase 7)
 *
 * Purpose:
 * - Convert intelligence + risk signals into actionable recommendations
 * - Stay compatible with current dashboardBuilders.js usage
 * - Return:
 *   {
 *     prioritized_actions: [],
 *     quick_wins: [],
 *     high_impact_fixes: []
 *   }
 */

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
  const cleaned = String(value ?? "")
    .replace(/,/g, "")
    .replace(/%/g, "")
    .trim();

  const num = Number(cleaned);
  return Number.isFinite(num) ? num : fallback;
}

function round(value, digits = 1) {
  const factor = 10 ** digits;
  return Math.round(toNumber(value, 0) * factor) / factor;
}

function uniqBy(items, keyFn) {
  const seen = new Set();
  const output = [];

  for (const item of safeArray(items)) {
    const key = keyFn(item);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    output.push(item);
  }

  return output;
}

function priorityRank(priority) {
  const p = lower(priority);
  if (p === "high" || p === "critical") return 3;
  if (p === "medium") return 2;
  if (p === "low") return 1;
  return 0;
}

function riskLevelRank(level) {
  const l = lower(level);
  if (l === "critical") return 4;
  if (l === "high") return 3;
  if (l === "medium") return 2;
  if (l === "low") return 1;
  return 0;
}

function sortDesc(items, scoreFn) {
  return [...safeArray(items)].sort((a, b) => scoreFn(b) - scoreFn(a));
}

function formatBopd(value) {
  return round(value, 1);
}

function buildWellActionTitle(well) {
  return `Recover ${cleanText(well.well_name) || cleanText(well.well_id) || "well"}`;
}

function buildDNActionTitle(dn) {
  return `Resolve DN ${cleanText(dn.dn_id) || "Unknown"}`;
}

function inferWellActionType(well) {
  const status = lower(well.production_status);
  if (status.includes("locked")) return "unlock_well";
  if (status.includes("shut")) return "restart_well";
  if (status.includes("standby")) return "return_to_service";
  if (status.includes("moth")) return "reactivate_well";
  return "stabilize_well";
}

function inferDNActionType(dn) {
  const step = lower(dn.current_step);
  const status = lower(dn.workflow_status);

  if (step.includes("package") || status.includes("waiting")) return "clear_package_block";
  if (step.includes("rfi")) return "close_rfi";
  if (step.includes("execution")) return "push_execution";
  if (status.includes("completed")) return "close_completed_dn";
  return "advance_dn";
}

function buildWellRecommendation(well, extra = {}) {
  const wellId = cleanText(well.well_id);
  const wellName = cleanText(well.well_name);
  const oilRate = toNumber(well.oil_rate_bopd, 0);
  const blockedDnCount = toNumber(well.blocked_dn_count, 0);
  const activeDnCount = toNumber(well.active_dn_count, 0);
  const highPriorityDnCount = toNumber(well.high_priority_dn_count, 0);
  const riskScore = toNumber(well.risk_score, 0);
  const riskLevel = cleanText(well.risk_level);
  const productionStatus = cleanText(well.production_status);

  let details = `${productionStatus || "Well"} exposure`;
  const detailParts = [];

  if (oilRate > 0) detailParts.push(`${formatBopd(oilRate)} BOPD at risk`);
  if (activeDnCount > 0) detailParts.push(`${activeDnCount} active DN(s)`);
  if (blockedDnCount > 0) detailParts.push(`${blockedDnCount} blocked`);
  if (highPriorityDnCount > 0) detailParts.push(`${highPriorityDnCount} high-priority`);
  if (riskScore > 0) detailParts.push(`risk ${riskScore}${riskLevel ? ` (${riskLevel})` : ""}`);

  if (detailParts.length > 0) {
    details = detailParts.join(", ");
  }

  return {
    type: "well",
    action_type: inferWellActionType(well),
    id: wellId,
    well_id: wellId,
    well_name: wellName,
    field_code: cleanText(well.field_code),
    title: buildWellActionTitle(well),
    details,
    impact_bopd: oilRate,
    priority_score:
      toNumber(extra.priority_score, 0) ||
      riskScore + oilRate / 20 + blockedDnCount * 8 + highPriorityDnCount * 6
  };
}

function buildDNRecommendation(dn, extra = {}) {
  const dnId = cleanText(dn.dn_id);
  const ageDays = toNumber(dn.age_days, 0);
  const progressPercent = toNumber(dn.progress_percent, 0);
  const priority = cleanText(dn.priority);
  const workflowStatus = cleanText(dn.workflow_status);
  const currentStep = cleanText(dn.current_step);
  const owner = cleanText(dn.current_owner_name);
  const riskScore = toNumber(dn.risk_score, 0);

  const detailParts = [];
  if (priority) detailParts.push(`${priority} priority`);
  if (workflowStatus) detailParts.push(workflowStatus);
  if (currentStep) detailParts.push(currentStep);
  if (owner) detailParts.push(`owner ${owner}`);
  if (ageDays > 0) detailParts.push(`${ageDays} days aged`);
  detailParts.push(`${progressPercent}% progress`);
  if (riskScore > 0) detailParts.push(`risk ${riskScore}`);

  return {
    type: "dn",
    action_type: inferDNActionType(dn),
    id: dnId,
    dn_id: dnId,
    well_id: cleanText(dn.well_id),
    title: buildDNActionTitle(dn),
    details: detailParts.join(", "),
    priority_score:
      toNumber(extra.priority_score, 0) ||
      riskScore + priorityRank(priority) * 15 + ageDays + (100 - progressPercent) / 5
  };
}

function pickTopRiskWells(risk) {
  return safeArray(risk?.top_risk_wells);
}

function pickTopRiskDNs(risk) {
  return safeArray(risk?.top_risk_dns);
}

function pickHighPriorityDNs(intelligence) {
  return safeArray(intelligence?.dn?.high_priority_dns);
}

function pickBlockedDNs(intelligence) {
  return safeArray(intelligence?.dn?.blocked_dns);
}

function pickAgedDNs(intelligence) {
  return safeArray(intelligence?.dn?.aged_dns);
}

function pickDeferredWells(intelligence) {
  return safeArray(intelligence?.production?.top_deferred_wells);
}

function pickImpactedWells(intelligence) {
  return safeArray(intelligence?.drop?.impacted_wells);
}

function pickStrongestWells(intelligence) {
  return safeArray(intelligence?.production?.strongest_wells);
}

function scoreWellForPriority(well) {
  return (
    toNumber(well.risk_score, 0) +
    toNumber(well.oil_rate_bopd, 0) / 20 +
    toNumber(well.blocked_dn_count, 0) * 10 +
    toNumber(well.high_priority_dn_count, 0) * 8 +
    riskLevelRank(well.risk_level) * 12
  );
}

function scoreDNForPriority(dn) {
  return (
    toNumber(dn.risk_score, 0) +
    priorityRank(dn.priority) * 15 +
    toNumber(dn.age_days, 0) +
    (100 - toNumber(dn.progress_percent, 0)) / 4 +
    (lower(dn.workflow_status).includes("waiting") ? 12 : 0) +
    (lower(dn.current_step).includes("package") ? 8 : 0)
  );
}

function buildPrioritizedActions({ risk = null, intelligence = null }) {
  const candidateWells = uniqBy(
    [
      ...pickTopRiskWells(risk),
      ...pickDeferredWells(intelligence),
      ...pickImpactedWells(intelligence)
    ],
    (item) => cleanText(item.well_id)
  );

  const candidateDNs = uniqBy(
    [
      ...pickTopRiskDNs(risk),
      ...pickHighPriorityDNs(intelligence),
      ...pickBlockedDNs(intelligence),
      ...pickAgedDNs(intelligence)
    ],
    (item) => cleanText(item.dn_id)
  );

  const topWellActions = sortDesc(candidateWells, scoreWellForPriority)
    .slice(0, 4)
    .map((well) =>
      buildWellRecommendation(well, {
        priority_score: scoreWellForPriority(well)
      })
    );

  const topDNActions = sortDesc(candidateDNs, scoreDNForPriority)
    .slice(0, 4)
    .map((dn) =>
      buildDNRecommendation(dn, {
        priority_score: scoreDNForPriority(dn)
      })
    );

  return [...topWellActions, ...topDNActions]
    .sort((a, b) => b.priority_score - a.priority_score)
    .slice(0, 6);
}

function buildQuickWins({ intelligence = null }) {
  const quickDNs = uniqBy(
    [
      ...pickBlockedDNs(intelligence),
      ...pickHighPriorityDNs(intelligence)
    ],
    (item) => cleanText(item.dn_id)
  ).filter((dn) => {
    const progress = toNumber(dn.progress_percent, 0);
    return progress >= 0;
  });

  const quickWells = uniqBy(
    [
      ...pickStrongestWells(intelligence),
      ...pickDeferredWells(intelligence)
    ],
    (item) => cleanText(item.well_id)
  );

  const dnActions = sortDesc(quickDNs, (dn) => {
    const progress = toNumber(dn.progress_percent, 0);
    const age = toNumber(dn.age_days, 0);
    return priorityRank(dn.priority) * 20 + age + progress / 5;
  })
    .slice(0, 3)
    .map((dn) =>
      buildDNRecommendation(dn, {
        priority_score:
          priorityRank(dn.priority) * 20 +
          toNumber(dn.age_days, 0) +
          toNumber(dn.progress_percent, 0) / 5
      })
    );

  const wellActions = sortDesc(quickWells, (well) => {
    const oilRate = toNumber(well.oil_rate_bopd, 0);
    const activeDnCount = toNumber(well.active_dn_count, 0);
    return oilRate / 25 + activeDnCount * 5;
  })
    .slice(0, 3)
    .map((well) =>
      buildWellRecommendation(well, {
        priority_score:
          toNumber(well.oil_rate_bopd, 0) / 25 +
          toNumber(well.active_dn_count, 0) * 5
      })
    );

  return [...dnActions, ...wellActions]
    .sort((a, b) => b.priority_score - a.priority_score)
    .slice(0, 5);
}

function buildHighImpactFixes({ intelligence = null, risk = null }) {
  const impactedWells = uniqBy(
    [
      ...pickImpactedWells(intelligence),
      ...pickDeferredWells(intelligence),
      ...pickTopRiskWells(risk)
    ],
    (item) => cleanText(item.well_id)
  ).filter((well) => toNumber(well.oil_rate_bopd, 0) > 0);

  return sortDesc(impactedWells, (well) => {
    return (
      toNumber(well.oil_rate_bopd, 0) +
      toNumber(well.blocked_dn_count, 0) * 40 +
      toNumber(well.high_priority_dn_count, 0) * 25 +
      toNumber(well.risk_score, 0)
    );
  })
    .slice(0, 5)
    .map((well) => ({
      ...buildWellRecommendation(well, {
        priority_score:
          toNumber(well.oil_rate_bopd, 0) +
          toNumber(well.blocked_dn_count, 0) * 40 +
          toNumber(well.high_priority_dn_count, 0) * 25 +
          toNumber(well.risk_score, 0)
      }),
      lost_bopd: toNumber(well.oil_rate_bopd, 0)
    }));
}

function generateRecommendations({ wells = [], dns = [], risk = null, intelligence = null }) {
  void wells;
  void dns;

  return {
    prioritized_actions: buildPrioritizedActions({ risk, intelligence }),
    quick_wins: buildQuickWins({ intelligence }),
    high_impact_fixes: buildHighImpactFixes({ intelligence, risk })
  };
}

module.exports = {
  generateRecommendations
};
