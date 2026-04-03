// backend/src/logic/riskEngine.js
// Phase 4.3 / V3 - Project-compatible risk scoring engine
//
// Design goals:
// - Preserve compatibility with current backend integration
// - Be tolerant to dirty / missing / mixed-format data
// - Avoid breaking endpoint response shapes
// - Improve realism for blocked/stuck workflows
// - Keep scoring strong, not toy-level

// ============================================================================
// UTILITY FUNCTIONS - SAFE NORMALIZATION
// ============================================================================

function safeString(value) {
  return String(value ?? "").trim();
}

function safeLower(value) {
  return safeString(value).toLowerCase();
}

function toNumber(value, fallback = 0) {
  const cleaned = String(value ?? "")
    .replace(/%/g, "")
    .replace(/,/g, "")
    .trim();

  const n = Number(cleaned);
  return Number.isFinite(n) ? n : fallback;
}

function ensureArray(value) {
  return Array.isArray(value) ? value : [];
}

function normalizePriority(priority) {
  const p = safeLower(priority);

  if (p === "high") return "High";
  if (p === "medium") return "Medium";
  if (p === "low") return "Low";

  return "Medium";
}

function normalizeProductionStatus(status) {
  const s = safeLower(status);

  if (s.includes("locked")) return "Locked Potential";
  if (s.includes("shut")) return "Shut-in";
  if (s.includes("production")) return "On Production";
  if (s.includes("test")) return "Testing";
  if (s.includes("mothball")) return "Mothball";
  if (s.includes("standby")) return "Standby";

  return safeString(status) || "Unknown";
}

function parseDateSafe(value) {
  const raw = safeString(value);
  if (!raw) return null;

  // If date-only format, force local midnight for more stable day math
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    const d = new Date(`${raw}T00:00:00`);
    return Number.isNaN(d.getTime()) ? null : d;
  }

  const d = new Date(raw);
  return Number.isNaN(d.getTime()) ? null : d;
}

// ============================================================================
// CLASSIFICATION HELPERS
// ============================================================================

function getRiskLevel(score) {
  if (score >= 80) return "CRITICAL";
  if (score >= 50) return "HIGH";
  if (score >= 25) return "MEDIUM";
  return "LOW";
}

function getAgingBand(days) {
  if (!Number.isFinite(days) || days < 0) return "UNKNOWN";
  if (days <= 7) return "0-7";
  if (days <= 14) return "8-14";
  if (days <= 30) return "15-30";
  return "31+";
}

function buildReasonBucket() {
  return new Set();
}

function finalizeReasons(reasonBucket, limit = 6) {
  return Array.from(reasonBucket).slice(0, limit);
}

// ============================================================================
// WEIGHT HELPERS
// ============================================================================

function getPriorityWeight(priority, { isCompleted = false } = {}) {
  const p = normalizePriority(priority);

  if (isCompleted) {
    if (p === "High") return 15;
    if (p === "Medium") return 10;
    if (p === "Low") return 5;
    return 8;
  }

  if (p === "High") return 40;
  if (p === "Medium") return 25;
  if (p === "Low") return 10;
  return 20;
}

function getAgingWeight(days, { isCompleted = false } = {}) {
  const band = getAgingBand(days);

  if (isCompleted) {
    if (band === "0-7") return 0;
    if (band === "8-14") return 2;
    if (band === "15-30") return 5;
    if (band === "31+") return 8;
    return 0;
  }

  if (band === "0-7") return 5;
  if (band === "8-14") return 15;
  if (band === "15-30") return 25;
  if (band === "31+") return 40;
  return 0;
}

function getOilRateWeight(oilRate) {
  const rate = toNumber(oilRate, 0);

  if (rate >= 1000) return 25;
  if (rate >= 500) return 15;
  if (rate > 0) return 5;
  return 0;
}

// ============================================================================
// DN STATE DETECTION
// ============================================================================

function isDNClosed(dn) {
  if (!dn) return false;

  if (typeof dn.isClosed === "function") {
    try {
      return !!dn.isClosed();
    } catch (_) {}
  }

  if (typeof dn.is_closed === "boolean") {
    return dn.is_closed;
  }

  const workflowStatus = safeLower(dn.workflow_status);
  const rawStatus = safeLower(dn.dn_status || dn.status_update || dn.status);

  return (
    workflowStatus === "closed" ||
    rawStatus === "closed" ||
    rawStatus.includes("closed")
  );
}

function isDNCompleted(dn) {
  if (!dn || isDNClosed(dn)) return false;

  const workflowStatus = safeLower(dn.workflow_status);
  const rawStatus = safeLower(dn.dn_status || dn.status_update || dn.status);

  return (
    workflowStatus === "completed" ||
    rawStatus.includes("completed")
  );
}

function getDNPhase(dn) {
  if (!dn) return "";

  if (typeof dn.getPhase === "function") {
    try {
      const phase = safeString(dn.getPhase());
      if (phase) return phase;
    } catch (_) {}
  }

  return safeString(
    dn.current_step ||
    dn.workflow_status ||
    dn.dn_status ||
    dn.status_update ||
    dn.status
  );
}

function getDNLastUpdateDays(dn) {
  if (!dn) return 0;

  if (typeof dn.getDaysSinceLastUpdate === "function") {
    try {
      const days = dn.getDaysSinceLastUpdate();
      return Number.isFinite(days) && days >= 0 ? days : 0;
    } catch (_) {}
  }

  const updateDate =
    dn.latest_update_date ||
    dn.update_date ||
    dn.last_updated ||
    dn.created_date;

  const parsed = parseDateSafe(updateDate);
  if (!parsed) return 0;

  const now = new Date();
  const diffMs = now.getTime() - parsed.getTime();
  return Math.max(0, Math.floor(diffMs / (1000 * 60 * 60 * 24)));
}

function isBlockedDN(dn) {
  if (!dn) return false;

  const phase = safeLower(getDNPhase(dn));
  const rawStatus = safeLower(dn.dn_status || dn.status_update || dn.status);
  const owner = safeLower(dn.current_owner_name || dn.owner);

  const text = `${phase} ${rawStatus} ${owner}`;

  return (
    text.includes("not issuing") ||
    text.includes("awaiting") ||
    text.includes("waiting") ||
    text.includes("hold") ||
    text.includes("blocked") ||
    text.includes("pending package") ||
    text.includes("under rfi") ||
    text.includes("foeu not issuing package") ||
    text.includes("dn not issuing")
  );
}

function getPhaseWeight(phase, { isCompleted = false } = {}) {
  if (isCompleted) return 0;

  const p = safeLower(phase);

  if (p.includes("execution")) return 15;
  if (p.includes("depressur")) return 10;
  if (p.includes("rfi")) return 6;
  if (p.includes("package")) return 8;

  return 0;
}

// ============================================================================
// DN-LEVEL RISK
// ============================================================================

function calculateDNRisk(dn) {
  if (!dn) {
    return {
      score: 0,
      level: "UNKNOWN",
      reasons: ["DN not found"]
    };
  }

  if (isDNClosed(dn)) {
    return {
      score: 0,
      level: "LOW",
      reasons: ["Closed"]
    };
  }

  const reasons = buildReasonBucket();
  let score = 0;

  const completed = isDNCompleted(dn);
  const priority = normalizePriority(dn.priority);
  const days = getDNLastUpdateDays(dn);
  const agingBand = getAgingBand(days);
  const phase = getDNPhase(dn);
  const blocked = isBlockedDN(dn);

  score += getPriorityWeight(priority, { isCompleted: completed });
  reasons.add(`${priority} priority`);

  score += getAgingWeight(days, { isCompleted: completed });

  if (!completed) {
    if (agingBand === "8-14") reasons.add("Aging 8-14 days");
    if (agingBand === "15-30") reasons.add("Aging 15-30 days");
    if (agingBand === "31+") reasons.add("Aging 31+ days");
  } else {
    if (agingBand === "15-30" || agingBand === "31+") {
      reasons.add("Completed but awaiting closure");
    } else {
      reasons.add("Completed (awaiting closure)");
    }
  }

  const phaseWeight = getPhaseWeight(phase, { isCompleted: completed });
  score += phaseWeight;

  if (!completed) {
    const p = safeLower(phase);
    if (p.includes("execution")) reasons.add("Execution phase");
    else if (p.includes("depressur")) reasons.add("Depressurizing phase");
    else if (p.includes("rfi")) reasons.add("RFI phase");
    else if (p.includes("package")) reasons.add("Package preparation");
  }

  if (blocked) {
    score += completed ? 4 : 12;
    reasons.add(completed ? "Pending final closure" : "Blocked / awaiting action");
  }

  score = Math.min(100, score);

  return {
    score,
    level: getRiskLevel(score),
    reasons: finalizeReasons(reasons)
  };
}

// ============================================================================
// WELL-LEVEL RISK
// ============================================================================

function calculateWellRisk(well, dns = [], options = {}) {
  if (!well) {
    return {
      score: 0,
      level: "UNKNOWN",
      reasons: ["Well not found"]
    };
  }

  const reasons = buildReasonBucket();
  let score = 0;

  const safeDNs = ensureArray(dns);
  const productionStatus = normalizeProductionStatus(well.production_status);
  const oilRate = toNumber(well.oil_rate_bopd, 0);
  const useProductionWeighting = options.useProductionWeighting === true;

  const activeDNs = safeDNs.filter((dn) => !isDNClosed(dn));
  const openDNs = activeDNs.filter((dn) => !isDNCompleted(dn));
  const completedDNs = activeDNs.filter((dn) => isDNCompleted(dn));

  // Production status contribution
  if (productionStatus === "Locked Potential") {
    score += 40;
    reasons.add("Locked potential");
  } else if (productionStatus === "Shut-in") {
    score += 30;
    reasons.add("Shut-in");
  } else if (productionStatus === "Standby") {
    score += 15;
    reasons.add("Standby");
  } else if (productionStatus === "Mothball") {
    score += 10;
    reasons.add("Mothball");
  } else if (productionStatus === "Testing") {
    score += 6;
    reasons.add("Testing");
  }

  // DN count contribution
  if (openDNs.length > 0) {
    score += openDNs.length * 6;
    reasons.add(`${openDNs.length} active DN(s)`);
  }

  if (completedDNs.length > 0) {
    score += completedDNs.length * 2;
    reasons.add(`${completedDNs.length} DN(s) awaiting closure`);
  }

  let hasHighPriorityOpenDN = false;
  let hasAgedOpenDN = false;
  let blockedOpenCount = 0;
  let worstOpenDNRisk = 0;

  for (const dn of openDNs) {
    const priority = normalizePriority(dn.priority);
    const days = getDNLastUpdateDays(dn);
    const blocked = isBlockedDN(dn);
    const dnRisk = calculateDNRisk(dn);

    worstOpenDNRisk = Math.max(worstOpenDNRisk, dnRisk.score);

    if (priority === "High") {
      score += 12;
      hasHighPriorityOpenDN = true;
    } else if (priority === "Medium") {
      score += 6;
    }

    if (days > 14) {
      score += 10;
      hasAgedOpenDN = true;
    }

    if (blocked) {
      blockedOpenCount += 1;
      score += 8;
    }
  }

  if (hasHighPriorityOpenDN) {
    reasons.add("High priority DN present");
  }

  if (hasAgedOpenDN) {
    reasons.add("Aged DN present");
  }

  if (blockedOpenCount > 0) {
    reasons.add(
      blockedOpenCount === 1
        ? "Blocked DN present"
        : `${blockedOpenCount} blocked DNs`
    );
  }

  // Escalate if one DN is already very severe
  if (worstOpenDNRisk >= 80) {
    score += 14;
    reasons.add("Critical DN exposure");
  } else if (worstOpenDNRisk >= 50) {
    score += 8;
    reasons.add("High-risk DN exposure");
  }

  // Production impact weighting
  if (
    (productionStatus === "Shut-in" || productionStatus === "Locked Potential") &&
    openDNs.length > 0
  ) {
    const oilWeight = getOilRateWeight(oilRate);
    score += oilWeight;

    if (oilWeight >= 25) {
      reasons.add("High production impact");
    } else if (oilWeight >= 15) {
      reasons.add("Moderate production impact");
    } else if (oilWeight > 0) {
      reasons.add("Low production impact");
    }
  }

  // Optional production-based weighting
  if (useProductionWeighting && oilRate > 0 && openDNs.length > 0) {
    const productionBoost = Math.min(10, Math.floor(oilRate / 200));
    score += productionBoost;
  }

  score = Math.min(100, score);

  return {
    score,
    level: getRiskLevel(score),
    reasons: finalizeReasons(reasons)
  };
}

// ============================================================================
// DASHBOARD AGGREGATION
// ============================================================================

function getWellDNs(wellId, dnMapOrList) {
  const normalizedWellId = safeString(wellId);
  if (!normalizedWellId) return [];

  if (dnMapOrList instanceof Map) {
    return Array.from(dnMapOrList.values()).filter(
      (dn) => safeString(dn.well_id) === normalizedWellId
    );
  }

  if (Array.isArray(dnMapOrList)) {
    return dnMapOrList.filter(
      (dn) => safeString(dn.well_id) === normalizedWellId
    );
  }

  return [];
}

function buildRiskDashboard(wells = [], dnMapOrList = null, options = {}) {
  const safeWells = ensureArray(wells);
  const limit = Number(options.limit) > 0 ? Number(options.limit) : 10;

  const topRiskWells = [];

  for (const well of safeWells) {
    if (!well || typeof well !== "object") continue;

    const wellId = safeString(well.well_id ?? well.id);
    const wellDNs = getWellDNs(wellId, dnMapOrList);
    const risk = calculateWellRisk(well, wellDNs, options);

    topRiskWells.push({
      well_id: safeString(well.well_id ?? well.id),
      well_name: safeString(well.well_name ?? well.name),
      field_code: safeString(well.field_code),
      production_status: normalizeProductionStatus(well.production_status),
      oil_rate_bopd: toNumber(well.oil_rate_bopd, 0),
      risk_score: risk.score,
      risk_level: risk.level,
      reasons: risk.reasons
    });
  }

  topRiskWells.sort((a, b) => {
    if (b.risk_score !== a.risk_score) return b.risk_score - a.risk_score;
    return toNumber(b.oil_rate_bopd, 0) - toNumber(a.oil_rate_bopd, 0);
  });

  return {
    top_risk_wells: topRiskWells.slice(0, limit),
    summary: {
      critical_count: topRiskWells.filter((w) => w.risk_level === "CRITICAL").length,
      high_count: topRiskWells.filter((w) => w.risk_level === "HIGH").length,
      medium_count: topRiskWells.filter((w) => w.risk_level === "MEDIUM").length,
      low_count: topRiskWells.filter((w) => w.risk_level === "LOW").length,
      total_wells: topRiskWells.length
    }
  };
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  calculateWellRisk,
  calculateDNRisk,
  buildRiskDashboard,
  getAgingBand,
  getRiskLevel
};