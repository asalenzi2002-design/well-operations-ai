// src/logic/dnImpactIntelligence.js
"use strict";

const {
  normalizeFieldCode,
  normalizeProductionStatus,
  isProducingStatus: isDomainProducingStatus
} = require("../core/domain");

/*
 * DN Impact Intelligence Layer (Field-Aware)
 * 
 * Purpose:
 * - Transform raw DN impact calculations into operational intelligence
 * - Separate and analyze impact between ANDR (Ain Dar) and ABQQ (Abqaiq) fields
 * - Classify DNs into actionable buckets
 * - Identify top loss drivers and patterns with field context
 * - Generate grounded operational recommendations
 * - Support dashboard prioritization and escalation decisions
 */

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function safeString(value) {
  return String(value ?? "").trim();
}

function safeLower(value) {
  return safeString(value).toLowerCase();
}

function toNumber(value, fallback = 0) {
  if (value === null || value === undefined) return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function getArray(value) {
  return Array.isArray(value) ? value : [];
}

function uniqueBy(items, keyFn) {
  const seen = new Set();
  const output = [];

  for (const item of getArray(items)) {
    const key = keyFn(item);
    if (seen.has(key)) continue;
    seen.add(key);
    output.push(item);
  }

  return output;
}

function buildWellMap(wells) {
  const map = new Map();
  for (const well of getArray(wells)) {
    const id = safeString(well?.well_id || well?.id);
    if (id) map.set(id, well);
  }
  return map;
}

function isProducingStatus(status) {
  return isDomainProducingStatus(status);
}

function sortByImpact(items) {
  return [...getArray(items)].sort(
    (a, b) => toNumber(b?.estimated_loss_bopd || b?.total_estimated_loss_bopd, 0) -
      toNumber(a?.estimated_loss_bopd || a?.total_estimated_loss_bopd, 0)
  );
}

// ============================================================================
// FIELD CODE DETECTION
// ============================================================================

function getWellFieldCode(well) {
  if (!well || typeof well !== "object") return "UNKNOWN";
  return normalizeFieldCode(
    well?.field_code,
    well?.well_name || well?.name || well?.well_id || well?.id || "",
    well?.field
  );
}

// ============================================================================
// DN CLASSIFICATION
// ============================================================================

function classifyDNActionBuckets(dnImpactResult) {
  const dnDetails = getArray(dnImpactResult?.dn_details);

  const buckets = {
    producing_impact_dns: [],
    deferred_opportunity_dns: [],
    blocked_dns: [],
    high_priority_low_progress_dns: [],
    aging_dns: [],
    completed_awaiting_closure_dns: []
  };

  for (const dn of dnDetails) {
    const priority = safeLower(dn?.priority || "");
    const progress = toNumber(dn?.progress_percent, 0);
    const isBlocked = dn?.is_blocked === true;
    const isCompleted = dn?.is_completed === true;
    const ageDays = toNumber(dn?.age_days, 0);
    const estimatedLoss = toNumber(dn?.estimated_loss_bopd, 0);
    const impactBand = safeString(dn?.impact_band || "");

    // Producing impact DNs: loss on active producing wells
    if (
      estimatedLoss > 0 &&
      impactBand !== "deferred" &&
      !isCompleted
    ) {
      buckets.producing_impact_dns.push(dn);
    }

    // Deferred opportunity DNs: impact on non-producing wells
    if (impactBand === "deferred" && !isCompleted) {
      buckets.deferred_opportunity_dns.push(dn);
    }

    // Blocked DNs: stuck in workflow
    if (isBlocked && !isCompleted) {
      buckets.blocked_dns.push(dn);
    }

    // High priority + low progress: escalation candidates
    if (
      (priority === "high" || priority === "critical") &&
      progress <= 30 &&
      !isCompleted
    ) {
      buckets.high_priority_low_progress_dns.push(dn);
    }

    // Aging DNs: unresolved for extended period
    if (ageDays > 30 && !isCompleted) {
      buckets.aging_dns.push(dn);
    }

    // Completed but awaiting closure
    if (isCompleted) {
      buckets.completed_awaiting_closure_dns.push(dn);
    }
  }

  // Remove duplicates while preserving order and prioritization
  return {
    producing_impact_dns: uniqueBy(buckets.producing_impact_dns, (dn) => dn.dn_id),
    deferred_opportunity_dns: uniqueBy(buckets.deferred_opportunity_dns, (dn) => dn.dn_id),
    blocked_dns: uniqueBy(buckets.blocked_dns, (dn) => dn.dn_id),
    high_priority_low_progress_dns: uniqueBy(
      buckets.high_priority_low_progress_dns,
      (dn) => dn.dn_id
    ),
    aging_dns: uniqueBy(buckets.aging_dns, (dn) => dn.dn_id),
    completed_awaiting_closure_dns: uniqueBy(buckets.completed_awaiting_closure_dns, (dn) => dn.dn_id)
  };
}

// ============================================================================
// FIELD-LEVEL IMPACT ANALYSIS
// ============================================================================

function buildFieldImpactBreakdown(wells = [], dnImpactResult = {}) {
  const wellMap = buildWellMap(wells);
  const dnDetails = getArray(dnImpactResult?.dn_details);

  const fieldData = {
    ANDR: {
      field_code: "ANDR",
      field_name: "Ain Dar",
      total_estimated_loss_bopd: 0,
      deferred_opportunity_bopd: 0,
      impacted_wells_count: 0,
      producing_impacted_wells_count: 0,
      non_producing_impacted_wells_count: 0,
      dn_count: 0,
      blocked_dn_count: 0,
      high_priority_low_progress_count: 0,
      aging_dn_count: 0,
      top_loss_dn: null
    },
    ABQQ: {
      field_code: "ABQQ",
      field_name: "Abqaiq",
      total_estimated_loss_bopd: 0,
      deferred_opportunity_bopd: 0,
      impacted_wells_count: 0,
      producing_impacted_wells_count: 0,
      non_producing_impacted_wells_count: 0,
      dn_count: 0,
      blocked_dn_count: 0,
      high_priority_low_progress_count: 0,
      aging_dn_count: 0,
      top_loss_dn: null
    }
  };

  const processedWellIds = new Set();

  // Process each DN and attribute to field
  for (const dn of dnDetails) {
    const wellId = safeString(dn?.well_id);
    if (!wellId) continue;

    const well = wellMap.get(wellId);
    if (!well) continue;

    const fieldCode = getWellFieldCode(well);
    if (!fieldCode || !fieldData[fieldCode]) continue;

    const field = fieldData[fieldCode];
    const estimatedLoss = toNumber(dn?.estimated_loss_bopd, 0);
    const impactBand = safeString(dn?.impact_band || "");
    const isProducing = isProducingStatus(well?.production_status);
    const isBlocked = dn?.is_blocked === true;
    const isCompleted = dn?.is_completed === true;
    const priority = safeLower(dn?.priority || "");
    const progress = toNumber(dn?.progress_percent, 0);
    const ageDays = toNumber(dn?.age_days, 0);

    // Track DN
    field.dn_count += 1;

    // Categorize impact
    if (impactBand === "deferred") {
      field.deferred_opportunity_bopd += estimatedLoss;
    } else if (estimatedLoss > 0) {
      field.total_estimated_loss_bopd += estimatedLoss;
    }

    // Track top loss DN for this field
    if (
      estimatedLoss > 0 &&
      (!field.top_loss_dn || estimatedLoss > toNumber(field.top_loss_dn?.estimated_loss_bopd, 0))
    ) {
      field.top_loss_dn = dn;
    }

    // Count blocked DNs
    if (isBlocked && !isCompleted) {
      field.blocked_dn_count += 1;
    }

    // Count high priority + low progress
    if ((priority === "high" || priority === "critical") && progress <= 30 && !isCompleted) {
      field.high_priority_low_progress_count += 1;
    }

    // Count aging DNs
    if (ageDays > 30 && !isCompleted) {
      field.aging_dn_count += 1;
    }

    // Track impacted wells (once per well)
    if (estimatedLoss > 0 && !processedWellIds.has(`${fieldCode}:${wellId}`)) {
      processedWellIds.add(`${fieldCode}:${wellId}`);
      field.impacted_wells_count += 1;

      if (isProducing) {
        field.producing_impacted_wells_count += 1;
      } else {
        field.non_producing_impacted_wells_count += 1;
      }
    }
  }

  return {
    ANDR: fieldData.ANDR,
    ABQQ: fieldData.ABQQ
  };
}

// ============================================================================
// FIELD DOMINANCE DETECTION
// ============================================================================

function detectFieldDominance(fieldImpact) {
  const andr = fieldImpact?.ANDR || {};
  const abqq = fieldImpact?.ABQQ || {};

  const andrProducingLoss = toNumber(andr?.total_estimated_loss_bopd, 0);
  const abqqProducingLoss = toNumber(abqq?.total_estimated_loss_bopd, 0);
  const andrDeferredOpp = toNumber(andr?.deferred_opportunity_bopd, 0);
  const abqqDeferredOpp = toNumber(abqq?.deferred_opportunity_bopd, 0);
  const andrBlockedDN = toNumber(andr?.blocked_dn_count, 0);
  const abqqBlockedDN = toNumber(abqq?.blocked_dn_count, 0);
  const andrAgingDN = toNumber(andr?.aging_dn_count, 0);
  const abqqAgingDN = toNumber(abqq?.aging_dn_count, 0);

  const totalProducingLoss = andrProducingLoss + abqqProducingLoss;
  const totalDeferredOpp = andrDeferredOpp + abqqDeferredOpp;
  const totalBlockedDN = andrBlockedDN + abqqBlockedDN;
  const totalAgingDN = andrAgingDN + abqqAgingDN;

  let dominantField = null;
  let dominantMode = "minimal";
  let dominanceReason = "";

  // Determine if any field is truly dominant (>60% of category)
  const dominanceThreshold = 0.6;

  // Check producing loss dominance
  if (totalProducingLoss > 0) {
    const andrPct = andrProducingLoss / totalProducingLoss;
    const abqqPct = abqqProducingLoss / totalProducingLoss;

    if (andrPct >= dominanceThreshold && andrProducingLoss > 0) {
      dominantField = "ANDR";
      dominantMode = "producing_loss";
      dominanceReason = `ANDR carries ${Math.round(andrPct * 100)}% of production loss (${andrProducingLoss} BOPD).`;
      return { dominantField, dominantMode, dominanceReason };
    } else if (abqqPct >= dominanceThreshold && abqqProducingLoss > 0) {
      dominantField = "ABQQ";
      dominantMode = "producing_loss";
      dominanceReason = `ABQQ carries ${Math.round(abqqPct * 100)}% of production loss (${abqqProducingLoss} BOPD).`;
      return { dominantField, dominantMode, dominanceReason };
    }
  }

  // Check deferred opportunity dominance
  if (totalDeferredOpp > 0) {
    const andrPct = andrDeferredOpp / totalDeferredOpp;
    const abqqPct = abqqDeferredOpp / totalDeferredOpp;

    if (andrPct >= dominanceThreshold && andrDeferredOpp > 0) {
      dominantField = "ANDR";
      dominantMode = "deferred_opportunity";
      dominanceReason = `ANDR carries ${Math.round(andrPct * 100)}% of deferred opportunity (${andrDeferredOpp} BOPD).`;
      return { dominantField, dominantMode, dominanceReason };
    } else if (abqqPct >= dominanceThreshold && abqqDeferredOpp > 0) {
      dominantField = "ABQQ";
      dominantMode = "deferred_opportunity";
      dominanceReason = `ABQQ carries ${Math.round(abqqPct * 100)}% of deferred opportunity (${abqqDeferredOpp} BOPD).`;
      return { dominantField, dominantMode, dominanceReason };
    }
  }

  // Check blocked workflow dominance
  if (totalBlockedDN > 0) {
    const andrPct = andrBlockedDN / totalBlockedDN;
    const abqqPct = abqqBlockedDN / totalBlockedDN;

    if (andrPct >= dominanceThreshold && andrBlockedDN > 0) {
      dominantField = "ANDR";
      dominantMode = "blocked_workflow";
      dominanceReason = `ANDR carries ${Math.round(andrPct * 100)}% of blocked DNs (${andrBlockedDN} items).`;
      return { dominantField, dominantMode, dominanceReason };
    } else if (abqqPct >= dominanceThreshold && abqqBlockedDN > 0) {
      dominantField = "ABQQ";
      dominantMode = "blocked_workflow";
      dominanceReason = `ABQQ carries ${Math.round(abqqPct * 100)}% of blocked DNs (${abqqBlockedDN} items).`;
      return { dominantField, dominantMode, dominanceReason };
    }
  }

  // Check aging DNS pressure
  if (totalAgingDN > 0) {
    const andrPct = andrAgingDN / totalAgingDN;
    const abqqPct = abqqAgingDN / totalAgingDN;

    if (andrPct >= dominanceThreshold && andrAgingDN > 0) {
      dominantField = "ANDR";
      dominantMode = "aging_pressure";
      dominanceReason = `ANDR carries ${Math.round(andrPct * 100)}% of aging DNs (${andrAgingDN} items).`;
      return { dominantField, dominantMode, dominanceReason };
    } else if (abqqPct >= dominanceThreshold && abqqAgingDN > 0) {
      dominantField = "ABQQ";
      dominantMode = "aging_pressure";
      dominanceReason = `ABQQ carries ${Math.round(abqqPct * 100)}% of aging DNs (${abqqAgingDN} items).`;
      return { dominantField, dominantMode, dominanceReason };
    }
  }

  // No clear dominance
  return {
    dominantField: null,
    dominantMode: "balanced",
    dominanceReason: "Impact is balanced or minimal across fields."
  };
}

// ============================================================================
// DRIVER ANALYSIS
// ============================================================================

function getTopLossDrivers(dnImpactResult) {
  const dnDetails = getArray(dnImpactResult?.dn_details);
  const topWells = getArray(dnImpactResult?.top_impacted_wells);
  const topDNTypes = getArray(dnImpactResult?.top_impact_dn_types);

  // Separate producing impact from deferred opportunity
  const producingImpactDNs = dnDetails.filter(
    (dn) => dn?.estimated_loss_bopd > 0 && dn?.impact_band !== "deferred"
  );

  const deferredOpportunityDNs = dnDetails.filter(
    (dn) => dn?.impact_band === "deferred"
  );

  // Find blocked DNs with highest impact
  const blockedByImpact = dnDetails
    .filter((dn) => dn?.is_blocked === true)
    .sort((a, b) => toNumber(b?.estimated_loss_bopd, 0) - toNumber(a?.estimated_loss_bopd, 0))
    .slice(0, 5);

  // Find aging DNs with highest impact
  const agingByImpact = dnDetails
    .filter((dn) => toNumber(dn?.age_days, 0) > 30)
    .sort((a, b) => toNumber(b?.estimated_loss_bopd, 0) - toNumber(a?.estimated_loss_bopd, 0))
    .slice(0, 5);

  // Find high-priority, low-progress DNs
  const escalationCandidates = dnDetails
    .filter(
      (dn) =>
        (safeLower(dn?.priority) === "high" || safeLower(dn?.priority) === "critical") &&
        toNumber(dn?.progress_percent, 0) <= 30
    )
    .sort((a, b) => toNumber(b?.estimated_loss_bopd, 0) - toNumber(a?.estimated_loss_bopd, 0))
    .slice(0, 5);

  return {
    top_producing_impact_wells: topWells
      .filter(
        (well) => isProducingStatus(well?.production_status)
      )
      .slice(0, 5),
    top_deferred_opportunity_wells: topWells
      .filter(
        (well) => !isProducingStatus(well?.production_status)
      )
      .slice(0, 5),
    top_dn_types: topDNTypes.slice(0, 5),
    top_blocked_dns: blockedByImpact,
    top_aging_dns: agingByImpact,
    escalation_candidates: escalationCandidates,
    producing_impact_dns_count: producingImpactDNs.length,
    deferred_opportunity_dns_count: deferredOpportunityDNs.length
  };
}

function buildDriversByField(wells, drivers, buckets) {
  const wellMap = buildWellMap(wells);
  const fieldDrivers = {
    ANDR: {
      top_wells: [],
      blocked_dns: [],
      aging_dns: []
    },
    ABQQ: {
      top_wells: [],
      blocked_dns: [],
      aging_dns: []
    }
  };

  const topWells = getArray(drivers?.top_producing_impact_wells);
  const blockedDns = getArray(buckets?.blocked_dns);
  const agingDns = getArray(buckets?.aging_dns);

  topWells.forEach((well) => {
    const fieldCode = getWellFieldCode(well);
    if (!fieldCode || !fieldDrivers[fieldCode]) return;
    fieldDrivers[fieldCode].top_wells.push(well);
  });

  blockedDns.forEach((dn) => {
    const well = wellMap.get(safeString(dn?.well_id));
    const fieldCode = getWellFieldCode(well);
    if (!fieldCode || !fieldDrivers[fieldCode]) return;
    fieldDrivers[fieldCode].blocked_dns.push(dn);
  });

  agingDns.forEach((dn) => {
    const well = wellMap.get(safeString(dn?.well_id));
    const fieldCode = getWellFieldCode(well);
    if (!fieldCode || !fieldDrivers[fieldCode]) return;
    fieldDrivers[fieldCode].aging_dns.push(dn);
  });

  return {
    ANDR: {
      top_wells: sortByImpact(fieldDrivers.ANDR.top_wells).slice(0, 5),
      blocked_dns: sortByImpact(fieldDrivers.ANDR.blocked_dns).slice(0, 5),
      aging_dns: sortByImpact(fieldDrivers.ANDR.aging_dns).slice(0, 5)
    },
    ABQQ: {
      top_wells: sortByImpact(fieldDrivers.ABQQ.top_wells).slice(0, 5),
      blocked_dns: sortByImpact(fieldDrivers.ABQQ.blocked_dns).slice(0, 5),
      aging_dns: sortByImpact(fieldDrivers.ABQQ.aging_dns).slice(0, 5)
    }
  };
}

// ============================================================================
// INSIGHT GENERATION
// ============================================================================

function generateDNImpactInsights(dnImpactResult, drivers, fieldDominance, wellMap = null) {
  const insights = [];

  const totalLoss = toNumber(dnImpactResult?.total_estimated_loss_bopd, 0);
  const deferredOpp = toNumber(dnImpactResult?.deferred_opportunity_bopd, 0);
  const totalImpacted = toNumber(dnImpactResult?.impacted_wells_count, 0);
  const producingImpacted = toNumber(dnImpactResult?.producing_impacted_wells_count, 0);
  const nonProducingImpacted = toNumber(dnImpactResult?.non_producing_impacted_wells_count, 0);

  // Primary impact mode
  if (producingImpacted > 0 && totalLoss > 0) {
    insights.push(
      `Active impact: ${totalLoss} BOPD loss across ${producingImpacted} producing well(s).`
    );
  } else if (nonProducingImpacted > 0 && deferredOpp > 0) {
    insights.push(
      `Deferred opportunity: ${deferredOpp} BOPD potential recovery from ${nonProducingImpacted} non-producing well(s).`
    );
  }

  // Field dominance insight
  if (fieldDominance?.dominantField && fieldDominance?.dominanceReason) {
    insights.push(fieldDominance.dominanceReason);
  }

  // Top well concentration
  const topProducingWells = getArray(drivers?.top_producing_impact_wells);
  if (topProducingWells.length > 0) {
    const topWell = topProducingWells[0];
    const topWellLoss = toNumber(topWell?.total_estimated_loss_bopd, 0);
    const percentOfTotal = totalLoss > 0 ? Math.round((topWellLoss / totalLoss) * 100) : 0;

    insights.push(
      `Top impact well is ${safeString(topWell?.well_name)} with ${topWellLoss} BOPD loss (${percentOfTotal}% of total).`
    );
  }

  // DN type concentration
  const topDNTypes = getArray(drivers?.top_dn_types);
  if (topDNTypes.length > 0) {
    const topType = topDNTypes[0];
    const typeCount = toNumber(topType?.active_dn_count, 0);
    const typeLoss = toNumber(topType?.total_loss_bopd, 0);

    insights.push(
      `Top DN type is ${safeString(topType?.dn_type)} with ${typeCount} active instance(s) and ${typeLoss} BOPD loss.`
    );
  }

  // Workflow blockage signal
  const blockedDNs = getArray(drivers?.top_blocked_dns);
  if (blockedDNs.length > 0) {
    const blockedLoss = blockedDNs.reduce((sum, dn) => sum + toNumber(dn?.estimated_loss_bopd, 0), 0);

    insights.push(
      `Workflow blockage detected: ${blockedDNs.length} DNs stuck with ${blockedLoss} BOPD combined impact.`
    );
  }

  // Aging signal
  const agingDNs = getArray(drivers?.top_aging_dns);
  if (agingDNs.length > 0) {
    const agingLoss = agingDNs.reduce((sum, dn) => sum + toNumber(dn?.estimated_loss_bopd, 0), 0);
    const maxAge = Math.max(...agingDNs.map((dn) => toNumber(dn?.age_days, 0)));

    insights.push(
      `${agingDNs.length} DNs unresolved for 30+ days with ${agingLoss} BOPD impact (oldest ${maxAge} days).`
    );
  }

  // Escalation readiness
  const escalationCandidates = getArray(drivers?.escalation_candidates);
  if (escalationCandidates.length > 0) {
    const escalationLoss = escalationCandidates.reduce((sum, dn) => sum + toNumber(dn?.estimated_loss_bopd, 0), 0);

    insights.push(
      `${escalationCandidates.length} high-priority, low-progress DNs need escalation (${escalationLoss} BOPD at risk).`
    );
  }

  return insights;
}

function generateFieldSpecificInsights(fieldImpact, driversByField) {
  const insights = [];

  ["ANDR", "ABQQ"].forEach((fieldCode) => {
    const field = fieldImpact?.[fieldCode] || {};
    const fieldDrivers = driversByField?.[fieldCode] || {};
    const fieldLoss = toNumber(field?.total_estimated_loss_bopd, 0);
    const deferredOpp = toNumber(field?.deferred_opportunity_bopd, 0);
    const topWell = getArray(fieldDrivers?.top_wells)[0];
    const blockedCount = getArray(fieldDrivers?.blocked_dns).length;
    const agingCount = getArray(fieldDrivers?.aging_dns).length;

    if (fieldLoss > 0) {
      insights.push(
        `${fieldCode} active impact is ${fieldLoss} BOPD across ${toNumber(field?.producing_impacted_wells_count, 0)} producing well(s).`
      );
    } else if (deferredOpp > 0) {
      insights.push(
        `${fieldCode} deferred opportunity is ${deferredOpp} BOPD across ${toNumber(field?.non_producing_impacted_wells_count, 0)} non-producing well(s).`
      );
    }

    if (topWell) {
      insights.push(
        `${fieldCode} top impact well is ${safeString(topWell?.well_name)} with ${toNumber(topWell?.total_estimated_loss_bopd, 0)} BOPD loss.`
      );
    }

    if (blockedCount > 0) {
      insights.push(
        `${fieldCode} has ${blockedCount} blocked DNs in the current driver set.`
      );
    }

    if (agingCount > 0) {
      insights.push(
        `${fieldCode} has ${agingCount} aging DNs in the current driver set.`
      );
    }
  });

  return insights;
}

// ============================================================================
// RECOMMENDATION GENERATION
// ============================================================================

function generateDNImpactRecommendations(intelligence) {
  const recommendations = [];
  const drivers = intelligence?.drivers || {};
  const buckets = intelligence?.buckets || {};

  // Producing impact prioritization
  const producingImpactDNs = getArray(buckets?.producing_impact_dns);
  if (producingImpactDNs.length > 0) {
    const topLossDN = producingImpactDNs.reduce((max, dn) =>
      toNumber(dn?.estimated_loss_bopd, 0) > toNumber(max?.estimated_loss_bopd, 0) ? dn : max
    );

    recommendations.push({
      priority: "critical",
      type: "resolve_producing_impact",
      dn_id: safeString(topLossDN?.dn_id),
      well_id: safeString(topLossDN?.well_id),
      action: `Resolve DN ${safeString(topLossDN?.dn_id)} on well ${safeString(topLossDN?.well_id)} — active production impact of ${toNumber(topLossDN?.estimated_loss_bopd, 0)} BOPD.`,
      impact_bopd: toNumber(topLossDN?.estimated_loss_bopd, 0),
      current_status: safeString(topLossDN?.workflow_status)
    });
  }

  // Blocked DNS resolution
  const blockedDNs = getArray(buckets?.blocked_dns);
  if (blockedDNs.length > 0) {
    const topBlockedByLoss = blockedDNs.reduce((max, dn) =>
      toNumber(dn?.estimated_loss_bopd, 0) > toNumber(max?.estimated_loss_bopd, 0) ? dn : max
    );

    recommendations.push({
      priority: "high",
      type: "unblock_workflow",
      dn_id: safeString(topBlockedByLoss?.dn_id),
      well_id: safeString(topBlockedByLoss?.well_id),
      action: `Clear blockage on DN ${safeString(topBlockedByLoss?.dn_id)} — stuck at ${safeString(topBlockedByLoss?.current_step)} (${toNumber(topBlockedByLoss?.estimated_loss_bopd, 0)} BOPD impact).`,
      impact_bopd: toNumber(topBlockedByLoss?.estimated_loss_bopd, 0),
      current_status: safeString(topBlockedByLoss?.workflow_status)
    });
  }

  // High priority + low progress escalation
  const escalationCandidates = getArray(buckets?.high_priority_low_progress_dns);
  if (escalationCandidates.length > 0) {
    const topEscalation = escalationCandidates[0];

    recommendations.push({
      priority: "high",
      type: "escalate_high_priority_stalled",
      dn_id: safeString(topEscalation?.dn_id),
      well_id: safeString(topEscalation?.well_id),
      action: `Escalate DN ${safeString(topEscalation?.dn_id)} — high priority but only ${toNumber(topEscalation?.progress_percent, 0)}% complete (${toNumber(topEscalation?.estimated_loss_bopd, 0)} BOPD impact).`,
      impact_bopd: toNumber(topEscalation?.estimated_loss_bopd, 0),
      progress_percent: toNumber(topEscalation?.progress_percent, 0)
    });
  }

  // Aging DNS remediation
  const agingDNs = getArray(buckets?.aging_dns);
  if (agingDNs.length > 0) {
    const topAging = agingDNs[0];

    recommendations.push({
      priority: "medium",
      type: "resolve_aging_dn",
      dn_id: safeString(topAging?.dn_id),
      well_id: safeString(topAging?.well_id),
      action: `Resolve aging DN ${safeString(topAging?.dn_id)} — unresolved for ${toNumber(topAging?.age_days, 0)} days (${toNumber(topAging?.estimated_loss_bopd, 0)} BOPD impact).`,
      impact_bopd: toNumber(topAging?.estimated_loss_bopd, 0),
      age_days: toNumber(topAging?.age_days, 0)
    });
  }

  // Deferred opportunity recovery
  const deferredDNs = getArray(buckets?.deferred_opportunity_dns);
  if (deferredDNs.length > 0) {
    const topDeferred = deferredDNs.reduce((max, dn) =>
      toNumber(dn?.estimated_loss_bopd, 0) > toNumber(max?.estimated_loss_bopd, 0) ? dn : max
    );

    recommendations.push({
      priority: "medium",
      type: "recovery_opportunity",
      dn_id: safeString(topDeferred?.dn_id),
      well_id: safeString(topDeferred?.well_id),
      action: `Resolve DN ${safeString(topDeferred?.dn_id)} to restore well — ${toNumber(topDeferred?.estimated_loss_bopd, 0)} BOPD recovery opportunity.`,
      recovery_opportunity_bopd: toNumber(topDeferred?.estimated_loss_bopd, 0),
      current_status: safeString(topDeferred?.workflow_status)
    });
  }

  // Completion closeout
  const completedAwaitingClosure = getArray(buckets?.completed_awaiting_closure_dns);
  if (completedAwaitingClosure.length > 0) {
    recommendations.push({
      priority: "low",
      type: "close_completed_dns",
      count: completedAwaitingClosure.length,
      action: `Close ${completedAwaitingClosure.length} completed DNs still open — administrative cleanup.`,
      dn_ids: completedAwaitingClosure.slice(0, 3).map((dn) => safeString(dn?.dn_id))
    });
  }

  return recommendations;
}

function selectTopDNByField(items, wells, fieldCode) {
  const wellMap = buildWellMap(wells);

  return sortByImpact(
    getArray(items).filter((dn) => {
      const well = wellMap.get(safeString(dn?.well_id));
      return getWellFieldCode(well) === fieldCode;
    })
  )[0] || null;
}

function generateFieldSpecificRecommendations(wells, buckets) {
  const recommendations = [];

  ["ANDR", "ABQQ"].forEach((fieldCode) => {
    const producingImpactDN = selectTopDNByField(buckets?.producing_impact_dns, wells, fieldCode);
    if (producingImpactDN) {
      recommendations.push({
        field: fieldCode,
        dn_id: safeString(producingImpactDN?.dn_id),
        well_id: safeString(producingImpactDN?.well_id),
        action: `Resolve DN ${safeString(producingImpactDN?.dn_id)} on well ${safeString(producingImpactDN?.well_id)} with active production impact.`,
        impact_bopd: toNumber(producingImpactDN?.estimated_loss_bopd, 0),
        priority: "critical"
      });
    }

    const blockedDN = selectTopDNByField(buckets?.blocked_dns, wells, fieldCode);
    if (blockedDN) {
      recommendations.push({
        field: fieldCode,
        dn_id: safeString(blockedDN?.dn_id),
        well_id: safeString(blockedDN?.well_id),
        action: `Unblock DN ${safeString(blockedDN?.dn_id)} at ${safeString(blockedDN?.current_step)}.`,
        impact_bopd: toNumber(blockedDN?.estimated_loss_bopd, 0),
        priority: "high"
      });
    }

    const stalledHighPriorityDN = selectTopDNByField(
      buckets?.high_priority_low_progress_dns,
      wells,
      fieldCode
    );
    if (stalledHighPriorityDN) {
      recommendations.push({
        field: fieldCode,
        dn_id: safeString(stalledHighPriorityDN?.dn_id),
        well_id: safeString(stalledHighPriorityDN?.well_id),
        action: `Escalate high-priority DN ${safeString(stalledHighPriorityDN?.dn_id)} stalled at ${toNumber(stalledHighPriorityDN?.progress_percent, 0)}% progress.`,
        impact_bopd: toNumber(stalledHighPriorityDN?.estimated_loss_bopd, 0),
        priority: "high"
      });
    }

    const agingDN = selectTopDNByField(buckets?.aging_dns, wells, fieldCode);
    if (agingDN) {
      recommendations.push({
        field: fieldCode,
        dn_id: safeString(agingDN?.dn_id),
        well_id: safeString(agingDN?.well_id),
        action: `Resolve aging DN ${safeString(agingDN?.dn_id)} open for ${toNumber(agingDN?.age_days, 0)} days.`,
        impact_bopd: toNumber(agingDN?.estimated_loss_bopd, 0),
        priority: "medium"
      });
    }

    const deferredDN = selectTopDNByField(buckets?.deferred_opportunity_dns, wells, fieldCode);
    if (deferredDN) {
      recommendations.push({
        field: fieldCode,
        dn_id: safeString(deferredDN?.dn_id),
        well_id: safeString(deferredDN?.well_id),
        action: `Resolve deferred DN ${safeString(deferredDN?.dn_id)} to recover shut-in opportunity.`,
        impact_bopd: toNumber(deferredDN?.estimated_loss_bopd, 0),
        priority: "medium"
      });
    }
  });

  return recommendations;
}

// ============================================================================
// MAIN INTELLIGENCE BUILDER
// ============================================================================

function buildDNImpactIntelligence(wells = [], dnImpactResult = {}, latestDNsByWell = null) {
  // Normalize inputs
  const safeWells = getArray(wells);
  const safeDNImpact = dnImpactResult && typeof dnImpactResult === "object" ? dnImpactResult : {};
  const wellMap = buildWellMap(safeWells);

  // Classify DNs into actionable buckets
  const buckets = classifyDNActionBuckets(safeDNImpact);

  // Extract top loss drivers
  const drivers = getTopLossDrivers(safeDNImpact);
  const driversByField = buildDriversByField(safeWells, drivers, buckets);

  // Build field-level impact breakdown
  const fieldImpact = buildFieldImpactBreakdown(safeWells, safeDNImpact);

  // Detect field dominance
  const fieldDominance = detectFieldDominance(fieldImpact);

  // Generate insights
  const insights = generateDNImpactInsights(safeDNImpact, drivers, fieldDominance, wellMap);
  const fieldSpecificInsights = generateFieldSpecificInsights(fieldImpact, driversByField);

  // Generate recommendations
  const recommendations = generateDNImpactRecommendations({
    drivers,
    buckets
  });
  const fieldSpecificRecommendations = generateFieldSpecificRecommendations(
    safeWells,
    buckets
  );

  // Determine dominant impact mode
  const totalLoss = toNumber(safeDNImpact?.total_estimated_loss_bopd, 0);
  const deferredOpp = toNumber(safeDNImpact?.deferred_opportunity_bopd, 0);
  let dominantMode = "minimal";

  if (totalLoss > 0 && deferredOpp > 0) {
    dominantMode = totalLoss >= deferredOpp ? "producing_impact_dominant" : "deferred_opportunity_dominant";
  } else if (totalLoss > 0) {
    dominantMode = "producing_impact_only";
  } else if (deferredOpp > 0) {
    dominantMode = "deferred_opportunity_only";
  }

  return {
    summary: {
      total_estimated_loss_bopd: toNumber(safeDNImpact?.total_estimated_loss_bopd, 0),
      deferred_opportunity_bopd: toNumber(safeDNImpact?.deferred_opportunity_bopd, 0),
      impacted_wells_count: toNumber(safeDNImpact?.impacted_wells_count, 0),
      producing_impacted_wells_count: toNumber(safeDNImpact?.producing_impacted_wells_count, 0),
      non_producing_impacted_wells_count: toNumber(safeDNImpact?.non_producing_impacted_wells_count, 0),
      dominant_impact_mode: dominantMode,
      total_active_dn_count: toNumber(safeDNImpact?.dn_details_count, 0)
    },

    drivers,

    drivers_by_field: driversByField,

    buckets,

    field_impact: fieldImpact,

    field_dominance: fieldDominance,

    insights,

    field_specific_insights: fieldSpecificInsights,

    recommendations,

    field_specific_recommendations: fieldSpecificRecommendations
  };
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  buildDNImpactIntelligence,
  classifyDNActionBuckets,
  getTopLossDrivers,
  generateDNImpactInsights,
  generateDNImpactRecommendations,
  buildFieldImpactBreakdown,
  detectFieldDominance,
  getWellFieldCode
};
