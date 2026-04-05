"use strict";

const {
  buildFormationExecutiveActions
} = require("./formationLineEngine");
const { normalizeFieldCode } = require("../core/domain");

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
    production_status: productionStatus,
    active_dn_count: activeDnCount,
    blocked_dn_count: blockedDnCount,
    high_priority_dn_count: highPriorityDnCount,
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
    well_name: cleanText(dn.well_name),
    field_code: cleanText(dn.field_code),
    priority,
    workflow_status: workflowStatus,
    current_step: currentStep,
    current_owner_name: owner,
    progress_percent: progressPercent,
    age_days: ageDays,
    title: buildDNActionTitle(dn),
    details: detailParts.join(", "),
    impact_bopd: toNumber(extra.impact_bopd, 0),
    priority_score:
      toNumber(extra.priority_score, 0) ||
      riskScore + priorityRank(priority) * 15 + ageDays + (100 - progressPercent) / 5
  };
}

function buildFormationRecommendation(action, extra = {}) {
  return {
    type: "formation",
    action_type: "formation_gain",
    id: cleanText(action.project_id || action.dn_id),
    project_id: cleanText(action.project_id || action.dn_id),
    project_name: cleanText(action.project_name),
    field_code: cleanText(action.field || action.field_code),
    readiness_state: cleanText(action.readiness_state),
    progress_percent: toNumber(action.progress_percent, 0),
    dependencies: safeArray(action.dependencies),
    blocking_items: safeArray(action.blocking_items),
    responsible_owner: cleanText(action.owner),
    workflow_status: cleanText(action.workflow_status),
    title: cleanText(action.action || "Advance formation project"),
    details: cleanText(extra.details || action.source || "formation_line"),
    impact_bopd: toNumber(action.impact_bopd, 0),
    priority_score: toNumber(extra.priority_score, 0)
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

function pickDNImpactCandidates(enhancements = null, intelligence = null) {
  return safeArray(enhancements?.dn_production_impact || intelligence?.enhancements?.dn_production_impact);
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

function getDNImpactMap(enhancements = null) {
  const impactMap = new Map();

  safeArray(enhancements?.dn_production_impact).forEach((item) => {
    const dnId = cleanText(item?.dn_id);
    if (!dnId) return;
    impactMap.set(dnId, {
      estimated_loss_bopd: toNumber(item?.estimated_loss_bopd, 0),
      field_code: cleanText(item?.field_code),
      well_id: cleanText(item?.well_id),
      well_name: cleanText(item?.well_name),
      owner: cleanText(item?.owner)
    });
  });

  return impactMap;
}

function getDNFieldMap(intelligence = null) {
  const dnFieldMap = new Map();

  ["ANDR", "ABQQ"].forEach((fieldCode) => {
    safeArray(intelligence?.field_intelligence?.[fieldCode]?.top_dns_to_act).forEach((item) => {
      const dnId = cleanText(item?.dn_id);
      if (!dnId || dnFieldMap.has(dnId)) return;
      dnFieldMap.set(dnId, fieldCode);
    });
  });

  return dnFieldMap;
}

function inferDNFieldCode(dn, impactData = null, fieldMap = null) {
  const normalized = normalizeFieldCode(
    impactData?.field_code || dn?.field_code,
    dn?.well_name || impactData?.well_name || "",
    dn?.field || ""
  );
  if (normalized === "ANDR" || normalized === "ABQQ") return normalized;

  const mappedField = cleanText(fieldMap?.get(cleanText(dn?.dn_id))).toUpperCase();
  if (mappedField === "ANDR" || mappedField === "ABQQ") return mappedField;

  return "ANDR";
}

function scoreDNImpactPriority(dn, estimatedLossBopd) {
  const priorityWeight = priorityRank(dn?.priority) * 100;
  const agingWeight = toNumber(dn?.age_days, 0) * 3;
  const blockedWeight =
    lower(dn?.workflow_status).includes("waiting") ||
    lower(dn?.current_step).includes("package") ||
    lower(dn?.current_step).includes("rfi")
      ? 120
      : 0;

  return round(
    estimatedLossBopd * 2 +
      priorityWeight +
      agingWeight +
      blockedWeight,
    1
  );
}

function scoreFormationAction(action) {
  const impact = toNumber(action.impact_bopd, 0);
  const priority = priorityRank(action.priority);
  const text = lower(action.action);
  const blockedBoost =
    text.includes("clear") || text.includes("remove") || text.includes("permit blocker")
      ? 20
      : 0;
  const tieInBoost = text.includes("tie-in") ? 25 : 0;
  return impact + priority * 30 + blockedBoost + tieInBoost;
}

function formatActionString(action) {
  const actionType = cleanText(action.action_type || action.type || "action").toUpperCase();
  const field = cleanText(action.field_code || action.field || "UNKNOWN");
  const impact = formatBopd(action.impact_bopd || action.estimated_gain_bopd || 0);
  const title = cleanText(action.title || action.action || "Action");
  return `[${actionType}] ${field} | ${title} | Impact ${impact} BOPD`;
}

function collectWellCandidates({ risk = null, intelligence = null }) {
  const candidateWells = uniqBy(
    [
      ...pickTopRiskWells(risk),
      ...pickDeferredWells(intelligence),
      ...pickImpactedWells(intelligence)
    ],
    (item) => cleanText(item.well_id)
  );
  return candidateWells.map((well) =>
    buildWellRecommendation(well, {
      priority_score: scoreWellForPriority(well)
    })
  );
}

function collectDNCandidates({ risk = null, intelligence = null, enhancements = null }) {
  const candidateMap = new Map();
  [
    ...pickDNImpactCandidates(enhancements, intelligence),
    ...pickTopRiskDNs(risk),
    ...pickHighPriorityDNs(intelligence),
    ...pickBlockedDNs(intelligence),
    ...pickAgedDNs(intelligence)
  ].forEach((item) => {
    const dnId = cleanText(item?.dn_id);
    if (!dnId) return;
    const existing = candidateMap.get(dnId) || {};
    candidateMap.set(dnId, { ...item, ...existing, dn_id: dnId });
  });
  const candidateDNs = Array.from(candidateMap.values());
  const impactMap = getDNImpactMap(enhancements || intelligence?.enhancements);
  const dnFieldMap = getDNFieldMap(intelligence);

  return candidateDNs.map((dn) =>
    {
      const impactData = impactMap.get(cleanText(dn.dn_id)) || null;
      const estimatedLossBopd = toNumber(impactData?.estimated_loss_bopd, 0);
      const fieldCode = inferDNFieldCode(dn, impactData, dnFieldMap);

      return buildDNRecommendation(
        {
          ...dn,
          field_code: fieldCode,
          well_name: cleanText(dn.well_name || impactData?.well_name),
          current_owner_name: cleanText(dn.current_owner_name || impactData?.owner),
          well_id: cleanText(dn.well_id || impactData?.well_id)
        },
        {
          impact_bopd: estimatedLossBopd,
          priority_score: scoreDNImpactPriority(dn, estimatedLossBopd)
        }
      );
    }
  );
}

function collectFormationCandidates({ formationProjects = [], formationTasks = [] }) {
  return buildFormationExecutiveActions(formationProjects, formationTasks).map((action) =>
    buildFormationRecommendation(action, {
      priority_score: scoreFormationAction(action)
    })
  );
}

function buildUnifiedActionList({ risk = null, intelligence = null, formationProjects = [], formationTasks = [], enhancements = null }) {
  return uniqBy(
    [
      ...collectWellCandidates({ risk, intelligence }),
      ...collectDNCandidates({ risk, intelligence, enhancements }),
      ...collectFormationCandidates({ formationProjects, formationTasks })
    ],
    (item) => `${cleanText(item.type)}:${cleanText(item.id || item.well_id || item.dn_id || item.project_id)}`
  ).sort((a, b) => b.priority_score - a.priority_score);
}

function getFieldMetric(value, fieldCode, fallback = 0) {
  return toNumber(value?.[cleanText(fieldCode).toUpperCase()]?.[fallback] ?? value?.[cleanText(fieldCode).toUpperCase()] ?? fallback, fallback);
}

function getFieldSignals(fieldCode, intelligence = null, enhancements = null) {
  const target = cleanText(fieldCode).toUpperCase();
  const fieldIntel = intelligence?.field_intelligence?.[target] || {};
  const fieldDn = intelligence?.dn?.by_field?.find?.((item) => cleanText(item.key).toUpperCase() === target) || null;
  const fieldFormation = intelligence?.formation?.by_field?.[target] || {};
  const fieldExposure = target === "ANDR"
    ? toNumber(enhancements?.field_imbalance?.details?.andr_exposure_score, 0)
    : toNumber(enhancements?.field_imbalance?.details?.abqq_exposure_score, 0);

  return {
    deferred_rate_bopd: toNumber(fieldIntel?.deferred_rate_bopd, 0),
    active_dn_count: toNumber(fieldIntel?.active_dn_count, toNumber(fieldDn?.count, 0)),
    blocked_dn_count: toNumber(fieldIntel?.blocked_dn_count, toNumber(fieldDn?.blocked, 0)),
    aged_dn_count: toNumber(fieldIntel?.aged_dn_count, toNumber(fieldDn?.aged, 0)),
    high_priority_dn_count: toNumber(fieldIntel?.high_priority_dn_count, toNumber(fieldDn?.high_priority, 0)),
    formation_gain_bopd: toNumber(fieldFormation?.expected_gain_bopd, 0),
    formation_blocked_count: toNumber(fieldFormation?.blocked_projects_count, 0),
    formation_tie_in_ready_count: toNumber(fieldFormation?.tie_in_ready_count, 0),
    exposure_score: fieldExposure
  };
}

function inferExposureType(action) {
  if (cleanText(action.type) === "formation") return "gain_capture";
  if (cleanText(action.type) === "well") return "recovery_push";
  return "loss_protection";
}

function inferDNResponsibleTeam(action) {
  const owner = cleanText(action.current_owner_name);
  const step = lower(action.current_step);
  const status = lower(action.workflow_status);

  if (owner) return owner;
  if (step.includes("package") || status.includes("not issuing")) return "FOEU";
  if (step.includes("rfi") || status.includes("rfi")) return "Inspection";
  if (step.includes("execution")) return "Maintenance";
  if (status.includes("depressur")) return "Field Operations";
  return "Field Operations";
}

function estimateDNUnblockDays(action) {
  const progress = toNumber(action.progress_percent, 0);
  const step = lower(action.current_step);
  const status = lower(action.workflow_status);

  if (progress > 80) return 2;
  if (step.includes("package") || status.includes("not issuing") || step.includes("rfi") || status.includes("rfi")) return progress < 20 ? 7 : 5;
  if (step.includes("execution") || status.includes("execution")) return progress < 20 ? 14 : 10;
  if (status.includes("depressur")) return 5;
  return progress < 20 ? 10 : 7;
}

function buildDNExecutionPlan(action) {
  const step = lower(action.current_step);
  const status = lower(action.workflow_status);
  const progress = toNumber(action.progress_percent, 0);
  const responsibleTeam = inferDNResponsibleTeam(action);

  let currentBlocker = "Workflow progression pending";
  let nextAction = "Advance DN workflow";
  let steps = [
    "Confirm DN owner and current workflow status",
    "Clear the current workflow blocker",
    "Advance work to execution or closeout"
  ];

  if (step.includes("package") || status.includes("not issuing") || status.includes("waiting")) {
    currentBlocker = "Engineering package not yet issued";
    nextAction = "FOEU to issue engineering package";
    steps = [
      "Confirm package scope and remaining inputs",
      "FOEU to issue engineering package",
      "Hand over to execution owner after package release"
    ];
  } else if (step.includes("rfi") || status.includes("rfi")) {
    currentBlocker = "RFI clearance is holding DN progression";
    nextAction = "Inspection to complete RFI clearance";
    steps = [
      "Review open RFI comments and acceptance criteria",
      "Inspection to clear the remaining RFI item",
      "Return DN to execution workflow"
    ];
  } else if (step.includes("execution") || status.includes("execution")) {
    currentBlocker = "Execution remains incomplete";
    nextAction = "Maintenance to complete execution";
    steps = [
      "Confirm execution scope and material readiness",
      "Complete remaining maintenance execution work",
      "Return well to service and close DN"
    ];
  } else if (status.includes("depressur")) {
    currentBlocker = "Depressurization is not yet complete";
    nextAction = "Field Operations to complete depressurization";
    steps = [
      "Verify depressurization window and operating conditions",
      "Field Operations to complete depressurization safely",
      "Release well for the next maintenance step"
    ];
  } else if (status.includes("not issuing")) {
    currentBlocker = "DN has not been issued by the current owner";
    nextAction = `${responsibleTeam} to issue DN`;
    steps = [
      "Confirm missing issue requirement or approval",
      `${responsibleTeam} to issue DN`,
      "Move DN into the next workflow stage"
    ];
  } else if (progress < 20 && toNumber(action.age_days, 0) > 30) {
    currentBlocker = "Low-progress aged DN is stalled early";
    nextAction = `${responsibleTeam} to remove the early-stage blocker`;
    steps = [
      "Identify the unresolved early-stage blocker",
      `${responsibleTeam} to remove the blocker and restart progression`,
      "Track progress to the next workflow gate"
    ];
  }

  return {
    steps,
    current_blocker: currentBlocker,
    next_action: nextAction,
    responsible_team: responsibleTeam,
    estimated_unblock_time_days: estimateDNUnblockDays(action),
    expected_impact_bopd: formatBopd(action.impact_bopd)
  };
}

function buildWellExecutionPlan(action) {
  return {
    steps: [
      "Confirm the DN or operational constraint holding the well",
      "Resolve the blocking DN or field constraint",
      "Return the well to production or testing"
    ],
    current_blocker:
      toNumber(action.blocked_dn_count, 0) > 0
        ? `${toNumber(action.blocked_dn_count, 0)} blocked DN(s) are limiting well recovery`
        : `${cleanText(action.production_status || "Well")} status is constraining production`,
    next_action:
      toNumber(action.blocked_dn_count, 0) > 0
        ? "Clear the highest-impact blocking DN"
        : "Prepare the well for return to production/testing",
    responsible_team: "Field Operations / Production Engineer",
    estimated_unblock_time_days: toNumber(action.impact_bopd, 0) > 0 ? 5 : 7,
    expected_impact_bopd: formatBopd(action.impact_bopd)
  };
}

function estimateFormationUnblockDays(action) {
  const progress = toNumber(action.progress_percent, 0);
  const readiness = lower(action.readiness_state);

  if (progress > 80 || readiness.includes("tie_in_ready")) return 2;
  if (readiness.includes("permit")) return progress < 20 ? 7 : 5;
  if (readiness.includes("execution") || readiness.includes("construction")) return progress < 20 ? 14 : 10;
  return progress < 20 ? 10 : 7;
}

function buildFormationExecutionPlan(action) {
  const readiness = lower(action.readiness_state);
  const blockers = safeArray(action.blocking_items);
  const firstBlocker = blockers[0];
  let currentBlocker = firstBlocker?.reason || "Project readiness remains incomplete";
  let nextAction = "Advance the project to the next readiness gate";
  let responsibleTeam = cleanText(action.responsible_owner) || "Field Operations";
  let steps = [
    "Review current readiness state and remaining dependencies",
    "Clear the immediate blocking item",
    "Advance the project to tie-in or execution"
  ];

  if (readiness.includes("permit")) {
    currentBlocker = firstBlocker?.reason || "Permit or security approval is pending";
    nextAction = "Obtain permits and security approval";
    responsibleTeam = "MESU / Security";
    steps = [
      "Confirm pending permit or security approval",
      "Obtain the required permit/security clearance",
      "Release the project for field execution"
    ];
  } else if (readiness.includes("execution_blocked")) {
    currentBlocker = firstBlocker?.reason || "Execution blocker is slowing construction progress";
    nextAction = "Remove execution blocker and resume construction";
    responsibleTeam = cleanText(action.responsible_owner) || "Field Operations";
    steps = [
      "Confirm blocked execution workfront",
      "Remove the execution blocker and restore construction progress",
      "Advance to tie-in readiness"
    ];
  } else if (readiness.includes("tie_in_ready")) {
    currentBlocker = "Tie-in execution window is still pending";
    nextAction = "Execute tie-in";
    responsibleTeam = "Maintenance Planner (CRD)";
    steps = [
      "Confirm tie-in window and isolation readiness",
      "Execute tie-in",
      "Commission the formation line for gain capture"
    ];
  } else if (readiness.includes("construction_in_progress") || readiness.includes("partial_ready")) {
    currentBlocker = firstBlocker?.reason || "Construction and prep work are not yet complete";
    nextAction = "Complete site prep and construction closeout";
    responsibleTeam = cleanText(action.responsible_owner) || "Field Operations";
    steps = [
      "Complete the remaining site preparation and construction work",
      "Clear any open dependency blocking readiness",
      "Move the project to tie-in readiness"
    ];
  }

  return {
    steps,
    current_blocker: currentBlocker,
    next_action: nextAction,
    responsible_team: responsibleTeam,
    estimated_unblock_time_days: estimateFormationUnblockDays(action),
    expected_impact_bopd: formatBopd(action.impact_bopd)
  };
}

function buildExecutionPlan(action) {
  if (action.type === "dn") return buildDNExecutionPlan(action);
  if (action.type === "well") return buildWellExecutionPlan(action);
  return buildFormationExecutionPlan(action);
}

function buildOperationalContext(action, fieldSignals) {
  const context = [];
  if (fieldSignals.active_dn_count > 0) context.push(`${fieldSignals.active_dn_count} active DNs in field`);
  if (fieldSignals.blocked_dn_count > 0) context.push(`${fieldSignals.blocked_dn_count} blocked`);
  if (fieldSignals.deferred_rate_bopd > 0) context.push(`${formatBopd(fieldSignals.deferred_rate_bopd)} BOPD deferred`);
  if (fieldSignals.formation_gain_bopd > 0) context.push(`${formatBopd(fieldSignals.formation_gain_bopd)} BOPD formation upside`);
  if (fieldSignals.exposure_score > 0) context.push(`exposure score ${fieldSignals.exposure_score}`);
  return context.join(", ");
}

function buildDecisionReasoning(action, fieldSignals) {
  const impact = formatBopd(action.impact_bopd);
  if (action.type === "dn") {
    return `Acting now protects about ${impact} BOPD by reducing DN pressure in ${cleanText(action.field_code || "UNKNOWN")} where ${fieldSignals.blocked_dn_count} blocked and ${fieldSignals.aged_dn_count} aged items are building exposure.`;
  }

  if (action.type === "well") {
    return `Prioritizing this well targets about ${impact} BOPD of deferred recovery in ${cleanText(action.field_code || "UNKNOWN")} with ${fieldSignals.active_dn_count} active DN-linked constraints in the field.`;
  }

  return `This formation action can unlock about ${impact} BOPD in ${cleanText(action.field_code || "UNKNOWN")} and matters now because field upside remains ${formatBopd(fieldSignals.formation_gain_bopd)} BOPD.`;
}

function scoreExecutiveUrgency(action, { intelligence = null, enhancements = null } = {}) {
  const fieldCode = cleanText(action.field_code || "UNKNOWN").toUpperCase();
  const fieldSignals = getFieldSignals(fieldCode, intelligence, enhancements);
  const impact = toNumber(action.impact_bopd, 0);
  const base = toNumber(action.priority_score, 0);
  const blockedBoost =
    cleanText(action.action_type).includes("clear") ||
    lower(action.details).includes("blocked") ||
    lower(action.title).includes("block")
      ? 20
      : 0;
  const ageBoost = lower(action.details).includes("aged") ? 12 : 0;
  const lossPressureBoost =
    inferExposureType(action) === "loss_protection"
      ? fieldSignals.exposure_score * 4 + fieldSignals.blocked_dn_count * 4 + fieldSignals.aged_dn_count * 2
      : 0;
  const recoveryBoost =
    inferExposureType(action) === "recovery_push"
      ? fieldSignals.deferred_rate_bopd / 30 + fieldSignals.active_dn_count * 2
      : 0;
  const gainBoost =
    inferExposureType(action) === "gain_capture"
      ? fieldSignals.formation_gain_bopd / 25 + fieldSignals.formation_tie_in_ready_count * 10
      : 0;
  const imbalanceBoost =
    cleanText(enhancements?.executive_signals?.biggest_operational_pressure_field).toUpperCase() === fieldCode
      ? 10
      : 0;

  return {
    urgency_score: round(
      base +
        impact / 15 +
        blockedBoost +
        ageBoost +
        lossPressureBoost +
        recoveryBoost +
        gainBoost +
        imbalanceBoost,
      1
    ),
    fieldSignals
  };
}

function classifyUrgency(score, action) {
  if (score >= 130) return "act_now";
  if (score >= 90) return "prioritize_next";
  if (score >= 55) return "monitor_closely";
  if (action.type === "formation" && lower(action.title).includes("construction")) return "monitor_closely";
  return "delay";
}

function buildExecutiveActionItem(action, context = {}) {
  const scored = scoreExecutiveUrgency(action, context);
  const urgencyBucket = classifyUrgency(scored.urgency_score, action);
  const exposureType = inferExposureType(action);

  return {
    action_type: cleanText(action.action_type || action.type),
    source_type: cleanText(action.type),
    field_code: cleanText(action.field_code || "UNKNOWN"),
    title: cleanText(action.title || action.action || "Action"),
    reasoning: buildDecisionReasoning(action, scored.fieldSignals),
    estimated_impact_bopd: formatBopd(action.impact_bopd),
    urgency_score: scored.urgency_score,
    urgency_bucket: urgencyBucket,
    exposure_type: exposureType === "recovery_push" ? "loss_protection" : exposureType,
    operational_context: buildOperationalContext(action, scored.fieldSignals),
    execution_plan: buildExecutionPlan(action)
  };
}

function bucketExecutiveActions(items = []) {
  return {
    act_now: safeArray(items).filter((item) => item.urgency_bucket === "act_now"),
    prioritize_next: safeArray(items).filter((item) => item.urgency_bucket === "prioritize_next"),
    monitor_closely: safeArray(items).filter((item) => item.urgency_bucket === "monitor_closely"),
    delay: safeArray(items).filter((item) => item.urgency_bucket === "delay")
  };
}

function detectDecisionMode({ intelligence = null, enhancements = null, executiveItems = [] }) {
  const dnPressure = toNumber(enhancements?.gain_vs_loss?.dn_pressure_bopd, 0);
  const formationGain = toNumber(enhancements?.gain_vs_loss?.formation_gain_bopd, 0);
  const deferredRecovery = toNumber(enhancements?.gain_vs_loss?.deferred_recovery_bopd, 0);
  const topItems = safeArray(executiveItems).slice(0, 8);
  const dnCount = topItems.filter((item) => item.source_type === "dn").length;
  const wellCount = topItems.filter((item) => item.source_type === "well").length;
  const formationCount = topItems.filter((item) => item.source_type === "formation").length;

  if (dnPressure >= Math.max(formationGain, deferredRecovery) && dnCount >= wellCount) {
    return {
      mode: "loss_protection",
      explanation: `The system is in loss-protection mode because live DN pressure is about ${formatBopd(dnPressure)} BOPD and the top command actions are dominated by DN clearance.`
    };
  }

  if (deferredRecovery >= Math.max(dnPressure, formationGain) && wellCount >= formationCount) {
    return {
      mode: "recovery_push",
      explanation: `The system is in recovery-push mode because deferred well recovery at about ${formatBopd(deferredRecovery)} BOPD is the strongest near-term upside.`
    };
  }

  if (formationGain > dnPressure && formationCount > 0) {
    return {
      mode: "gain_capture",
      explanation: `The system is in gain-capture mode because formation upside of about ${formatBopd(formationGain)} BOPD is larger than live DN pressure and ready formation actions are competitive with loss-protection work.`
    };
  }

  return {
    mode: "balanced_control",
    explanation: "The system is in balanced-control mode because loss protection and gain capture are both material and neither clearly dominates the current action mix."
  };
}

function buildFieldCommandSignals({ intelligence = null, enhancements = null }) {
  const andr = getFieldSignals("ANDR", intelligence, enhancements);
  const abqq = getFieldSignals("ABQQ", intelligence, enhancements);
  const andrDnLoss = safeArray(enhancements?.dn_production_impact)
    .filter((item) => cleanText(item.field_code).toUpperCase() === "ANDR")
    .reduce((sum, item) => sum + toNumber(item.estimated_loss_bopd, 0), 0);
  const abqqDnLoss = safeArray(enhancements?.dn_production_impact)
    .filter((item) => cleanText(item.field_code).toUpperCase() === "ABQQ")
    .reduce((sum, item) => sum + toNumber(item.estimated_loss_bopd, 0), 0);
  const pressureField = andrDnLoss >= abqqDnLoss ? "ANDR" : "ABQQ";
  const recoveryField =
    andr.deferred_rate_bopd + andr.formation_gain_bopd >=
    abqq.deferred_rate_bopd + abqq.formation_gain_bopd
      ? "ANDR"
      : "ABQQ";
  const blockageField = andr.blocked_dn_count + andr.formation_blocked_count >= abqq.blocked_dn_count + abqq.formation_blocked_count
    ? "ANDR"
    : "ABQQ";
  const gainField = andr.formation_gain_bopd >= abqq.formation_gain_bopd ? "ANDR" : "ABQQ";
  const monitoringField = pressureField === "ANDR" ? "ABQQ" : "ANDR";

  return {
    immediate_action_field: pressureField,
    highest_recovery_upside_field: recoveryField,
    highest_blockage_field: blockageField,
    highest_gain_opportunity_field: gainField,
    monitoring_field: monitoringField
  };
}

function buildExecutiveSummaryNote({ mode = null, topAction = null, enhancements = null, fieldSignals = null }) {
  const pressureField = cleanText(enhancements?.executive_signals?.biggest_operational_pressure_field || fieldSignals?.immediate_action_field || "UNKNOWN");
  const upsideField = cleanText(enhancements?.executive_signals?.biggest_recovery_opportunity_field || fieldSignals?.highest_recovery_upside_field || "UNKNOWN");
  const dominantGain = cleanText(enhancements?.executive_signals?.biggest_gain_source || "unknown");
  if (!topAction) {
    return `${pressureField} holds the main pressure, ${upsideField} holds the main upside, and the system is operating in ${cleanText(mode?.mode || "balanced_control")} mode.`;
  }

  return `${pressureField} carries the main DN pressure while ${upsideField} holds the strongest recovery upside. Current mode is ${cleanText(mode?.mode || "balanced_control")}. Top command DN is ${topAction.title} in ${topAction.field_code} because it protects about ${formatBopd(topAction.estimated_impact_bopd)} BOPD now.`;
}

function buildExecutiveCommandLayer({ risk = null, intelligence = null, formationProjects = [], formationTasks = [], enhancements = null }) {
  const unified = buildUnifiedActionList({ risk, intelligence, formationProjects, formationTasks, enhancements });
  const executiveItems = unified
    .map((action) => buildExecutiveActionItem(action, { intelligence, enhancements }))
    .sort((a, b) => b.urgency_score - a.urgency_score);
  const buckets = bucketExecutiveActions(executiveItems);
  const mode = detectDecisionMode({ intelligence, enhancements, executiveItems });
  const fieldCommand = buildFieldCommandSignals({ intelligence, enhancements });
  const topDnAction = unified
    .filter((item) => item.type === "dn")
    .sort((a, b) => b.priority_score - a.priority_score)
    .map((action) => buildExecutiveActionItem(action, { intelligence, enhancements }))[0] || null;
  const topAction =
    topDnAction ||
    executiveItems[0] ||
    null;

  return {
    act_now: buckets.act_now.slice(0, 5),
    prioritize_next: buckets.prioritize_next.slice(0, 5),
    monitor_closely: buckets.monitor_closely.slice(0, 5),
    delay: buckets.delay.slice(0, 5),
    mode,
    field_command: fieldCommand,
    top_action: topAction,
    summary: buildExecutiveSummaryNote({ mode, topAction, enhancements, fieldSignals: fieldCommand })
  };
}

function buildPrioritizedActions({ risk = null, intelligence = null, formationProjects = [], formationTasks = [] }) {
  return buildUnifiedActionList({ risk, intelligence, formationProjects, formationTasks, enhancements: intelligence?.enhancements })
    .slice(0, 5)
    .map(formatActionString);
}

function buildFieldActions({ risk = null, intelligence = null, formationProjects = [], formationTasks = [], fieldCode = "" }) {
  const target = cleanText(fieldCode).toUpperCase();
  return buildUnifiedActionList({ risk, intelligence, formationProjects, formationTasks, enhancements: intelligence?.enhancements })
    .filter((action) => cleanText(action.field_code).toUpperCase() === target)
    .slice(0, 5)
    .map(formatActionString);
}

function generateRecommendations({ wells = [], dns = [], risk = null, intelligence = null, formationProjects = [], formationTasks = [] }) {
  void wells;
  void dns;

  return {
    prioritized_actions: buildPrioritizedActions({ risk, intelligence, formationProjects, formationTasks }),
    quick_wins: buildFieldActions({ risk, intelligence, formationProjects, formationTasks, fieldCode: "ANDR" }),
    high_impact_fixes: buildFieldActions({ risk, intelligence, formationProjects, formationTasks, fieldCode: "ABQQ" })
  };
}

module.exports = {
  generateRecommendations,
  buildUnifiedActionList,
  buildExecutiveCommandLayer
};
