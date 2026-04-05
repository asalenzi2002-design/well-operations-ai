const DAY_MS = 24 * 60 * 60 * 1000;

function toSafeString(value) {
  return value == null ? "" : String(value).trim();
}

function toSafeLower(value) {
  return toSafeString(value).toLowerCase();
}

function toNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function parseDate(value) {
  const str = toSafeString(value);
  if (!str) return null;
  const date = new Date(str);
  return Number.isNaN(date.getTime()) ? null : date;
}

function daysBetween(fromDate, toDate = new Date()) {
  if (!(fromDate instanceof Date) || Number.isNaN(fromDate.getTime())) return 0;
  return Math.max(0, Math.floor((toDate.getTime() - fromDate.getTime()) / DAY_MS));
}

function normalizeFieldCode(value) {
  const v = toSafeLower(value);

  if (
    v.includes("ain dar") ||
    v.includes("andr") ||
    v === "ad" ||
    v === "andr"
  ) {
    return "ANDR";
  }

  if (
    v.includes("abqaiq") ||
    v.includes("abqq") ||
    v === "abq" ||
    v === "abqq"
  ) {
    return "ABQQ";
  }

  return "UNKNOWN";
}

function normalizeWellStatus(value) {
  const v = toSafeLower(value);

  if (v.includes("on production")) return "ON_PRODUCTION";
  if (v.includes("testing")) return "TESTING";
  if (v.includes("shut")) return "SHUT_IN";
  if (v.includes("locked")) return "LOCKED_POTENTIAL";
  if (v.includes("standby")) return "STANDBY";
  if (v.includes("mothball")) return "MOTHBALL";

  return "UNKNOWN";
}

function isDNClosed(dn) {
  const status = toSafeLower(
    dn?.workflow_status ||
    dn?.status ||
    dn?.status_update ||
    dn?.current_status
  );

  return (
    status.includes("closed") ||
    status.includes("completed") ||
    status.includes("done") ||
    status.includes("resolved")
  );
}

function getDNOwner(dn) {
  return (
    toSafeString(dn?.current_owner_name) ||
    toSafeString(dn?.owner) ||
    toSafeString(dn?.assigned_to) ||
    "Unassigned"
  );
}

function getDNStatus(dn) {
  return (
    toSafeString(dn?.workflow_status) ||
    toSafeString(dn?.status) ||
    toSafeString(dn?.status_update) ||
    "Unknown"
  );
}

function getDNType(dn) {
  return (
    toSafeString(dn?.dn_type_name) ||
    toSafeString(dn?.dn_type) ||
    toSafeString(dn?.type_name) ||
    "Unknown DN Type"
  );
}

function getDNPriority(dn) {
  const raw = toSafeLower(dn?.priority);

  if (raw.includes("critical")) return "CRITICAL";
  if (raw.includes("high")) return "HIGH";
  if (raw.includes("medium")) return "MEDIUM";
  if (raw.includes("low")) return "LOW";

  return "MEDIUM";
}

function getDNProgress(dn) {
  const progress = toNumber(
    dn?.progress_percent ?? dn?.progress ?? dn?.completion_percent,
    0
  );
  return Math.min(100, Math.max(0, progress));
}

function getDNCreatedDate(dn) {
  return (
    parseDate(dn?.created_date) ||
    parseDate(dn?.open_date) ||
    parseDate(dn?.date_created) ||
    null
  );
}

function getDNUpdateDate(dn) {
  return (
    parseDate(dn?.update_date) ||
    parseDate(dn?.updated_date) ||
    parseDate(dn?.last_updated) ||
    null
  );
}

function getWellRate(well) {
  return toNumber(
    well?.oil_rate_bopd ??
    well?.oil_rate ??
    well?.current_oil_rate,
    0
  );
}

function getWellId(well) {
  return (
    toSafeString(well?.well_id) ||
    toSafeString(well?.id) ||
    toSafeString(well?.well_name) ||
    toSafeString(well?.name)
  );
}

function getWellName(well) {
  return (
    toSafeString(well?.well_name) ||
    toSafeString(well?.name) ||
    getWellId(well)
  );
}

function getFieldCodeFromWell(well) {
  const explicit =
    toSafeString(well?.field_code) ||
    toSafeString(well?.field) ||
    toSafeString(well?.area);

  if (explicit) {
    return normalizeFieldCode(explicit);
  }

  const wellName = getWellName(well).toUpperCase();

  if (wellName.startsWith("ANDR-")) return "ANDR";
  if (wellName.startsWith("ABQQ-")) return "ABQQ";

  return "UNKNOWN";
}

function getPriorityWeight(priority) {
  switch (priority) {
    case "CRITICAL": return 100;
    case "HIGH": return 80;
    case "MEDIUM": return 55;
    case "LOW": return 30;
    default: return 50;
  }
}

function getStatusBlocker(status) {
  const s = toSafeLower(status);

  if (s.includes("foeu") && s.includes("package")) {
    return "FOEU package not issued";
  }

  if (s.includes("rfi")) {
    return "Inspection / RFI pending";
  }

  if (s.includes("not issuing")) {
    return "DN not issued";
  }

  if (s.includes("depressur")) {
    return "Depressurizing in progress";
  }

  if (s.includes("waiting")) {
    return "Waiting for next workflow step";
  }

  if (s.includes("hold")) {
    return "Work is on hold";
  }

  if (s.includes("permit")) {
    return "Permit dependency";
  }

  return "";
}

function inferNextAction(owner, dnType, status, typeGroup) {
  const ownerLower = toSafeLower(owner);
  const statusLower = toSafeLower(status);
  const dnTypeLower = toSafeLower(dnType);
  const groupLower = toSafeLower(typeGroup);

  if (statusLower.includes("foeu") && statusLower.includes("package")) {
    return "Issue engineering package and release work scope";
  }

  if (statusLower.includes("rfi")) {
    return "Follow up inspection and clear RFI";
  }

  if (statusLower.includes("depressur")) {
    return "Complete depressurizing and hand over for execution";
  }

  if (ownerLower.includes("field operations")) {
    return "Prepare site, isolate well, and clear operational readiness";
  }

  if (ownerLower.includes("foeu")) {
    return "Review technical scope and issue execution package";
  }

  if (ownerLower.includes("inspection")) {
    return "Inspect completed work and close verification gap";
  }

  if (ownerLower.includes("maintenance planner") || groupLower === "crd") {
    if (dnTypeLower.includes("leak") || dnTypeLower.includes("flowline")) {
      return "Issue work order and prepare tie-in / flowline execution";
    }
    return "Plan execution window and mobilize CRD work scope";
  }

  if (ownerLower.includes("maintenance") || groupLower === "cfc") {
    return "Mobilize support work and complete site preparation";
  }

  return "Review DN ownership and move to next executable step";
}

function inferTimelineDays(priority, daysOpen, progress, blocker, wellStatus) {
  let days = 7;

  if (priority === "CRITICAL") days = 2;
  else if (priority === "HIGH") days = 4;
  else if (priority === "MEDIUM") days = 7;
  else if (priority === "LOW") days = 12;

  if (daysOpen > 120) days -= 1;
  if (daysOpen > 180) days -= 1;
  if (progress >= 70) days -= 1;
  if (progress <= 10) days += 2;
  if (blocker) days += 2;
  if (wellStatus === "LOCKED_POTENTIAL") days -= 1;

  return Math.max(1, days);
}

function classifyActionType(owner, dnType, typeGroup) {
  const ownerLower = toSafeLower(owner);
  const dnTypeLower = toSafeLower(dnType);
  const groupLower = toSafeLower(typeGroup);

  if (ownerLower.includes("foeu")) return "ENGINEERING_RELEASE";
  if (ownerLower.includes("inspection")) return "INSPECTION_CLEARANCE";
  if (ownerLower.includes("field operations")) return "OPERATIONS_PREP";
  if (ownerLower.includes("maintenance planner") || groupLower === "crd") {
    if (dnTypeLower.includes("leak") || dnTypeLower.includes("flowline")) {
      return "FLOWLINE_EXECUTION";
    }
    return "CRD_EXECUTION";
  }
  if (ownerLower.includes("maintenance") || groupLower === "cfc") {
    return "SUPPORT_PREPARATION";
  }

  return "GENERAL_EXECUTION";
}

function scoreExecutionItem({ priority, daysOpen, impactBopd, progress, blocker, wellStatus }) {
  let score = 0;

  score += getPriorityWeight(priority);
  score += Math.min(80, Math.floor(daysOpen / 5));
  score += Math.min(120, Math.floor(impactBopd / 10));

  if (progress <= 10) score += 20;
  else if (progress <= 40) score += 10;
  else if (progress >= 80) score -= 10;

  if (blocker) score += 15;
  if (wellStatus === "SHUT_IN") score += 20;
  if (wellStatus === "LOCKED_POTENTIAL") score += 25;
  if (wellStatus === "TESTING") score += 5;

  return Math.max(0, score);
}

function buildExecutionPlan(normalizedWells, latestDNsByWell, options = {}) {
  const wellsList = Array.isArray(normalizedWells) ? normalizedWells : [];
  const dnByWell = latestDNsByWell instanceof Map ? latestDNsByWell : new Map();
  const topN = Number.isFinite(options.topN) ? options.topN : 25;

  const executionPlan = [];

  for (const well of wellsList) {
    const wellId = getWellId(well);
    const wellName = getWellName(well);
    const fieldCode = getFieldCodeFromWell(well);
    const wellStatus = normalizeWellStatus(well?.production_status);
    const impactBopd = getWellRate(well);
    const wellDNs = Array.isArray(dnByWell.get(wellId)) ? dnByWell.get(wellId) : [];

    for (const dn of wellDNs) {
      if (isDNClosed(dn)) continue;

      const owner = getDNOwner(dn);
      const dnType = getDNType(dn);
      const status = getDNStatus(dn);
      const priority = getDNPriority(dn);
      const progress = getDNProgress(dn);
      const createdDate = getDNCreatedDate(dn);
      const updateDate = getDNUpdateDate(dn);
      const daysOpen = daysBetween(createdDate);
      const blocker = getStatusBlocker(status);
      const nextAction = inferNextAction(owner, dnType, status, dn?.type_group);
      const etaDays = inferTimelineDays(priority, daysOpen, progress, blocker, wellStatus);
      const score = scoreExecutionItem({
        priority,
        daysOpen,
        impactBopd,
        progress,
        blocker,
        wellStatus
      });

      executionPlan.push({
        well_id: wellId,
        well: wellName,
        field_code: fieldCode,
        issue: dnType,
        dn_id: toSafeString(dn?.dn_id || dn?.id),
        owner,
        current_status: status,
        next_action: nextAction,
        blocker: blocker || "None",
        eta_days: etaDays,
        impact_bopd: impactBopd,
        priority,
        progress_percent: progress,
        days_open: daysOpen,
        last_update_date: updateDate ? updateDate.toISOString() : null,
        action_type: classifyActionType(owner, dnType, dn?.type_group),
        well_status: wellStatus,
        score
      });
    }
  }

  executionPlan.sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    if (b.impact_bopd !== a.impact_bopd) return b.impact_bopd - a.impact_bopd;
    if (b.days_open !== a.days_open) return b.days_open - a.days_open;
    return a.well.localeCompare(b.well);
  });

  const topItems = executionPlan.slice(0, topN);

  const summary = {
    total_open_actions: executionPlan.length,
    critical_actions: executionPlan.filter((x) => x.priority === "CRITICAL").length,
    high_actions: executionPlan.filter((x) => x.priority === "HIGH").length,
    blocked_actions: executionPlan.filter((x) => x.blocker && x.blocker !== "None").length,
    total_bopd_at_risk: executionPlan.reduce((sum, item) => sum + toNumber(item.impact_bopd, 0), 0),
    andr_actions: executionPlan.filter((x) => x.field_code === "ANDR").length,
    abqq_actions: executionPlan.filter((x) => x.field_code === "ABQQ").length
  };

  const byField = {
    ANDR: topItems.filter((x) => x.field_code === "ANDR"),
    ABQQ: topItems.filter((x) => x.field_code === "ABQQ"),
    UNKNOWN: topItems.filter((x) => x.field_code === "UNKNOWN")
  };

  return {
    summary,
    execution_plan: topItems,
    by_field: byField
  };
}

module.exports = {
  buildExecutionPlan
};