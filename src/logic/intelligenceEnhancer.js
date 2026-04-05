"use strict";

const {
  normalizeFieldCode,
  normalizeProductionStatus,
  isProducingStatus,
  isDNResolvedStatus
} = require("../core/domain");

/*
 * Intelligence Enhancer (Phase 8)
 *
 * Adds deeper operational reasoning on top of the existing system:
 * - DN production pressure
 * - anomaly detection
 * - field imbalance detection
 * - DN concentration
 * - gain vs loss comparison
 * - executive operational signals
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

function round(v, digits = 1) {
  const scale = Math.pow(10, digits);
  return Math.round(num(v, 0) * scale) / scale;
}

function parseDate(value) {
  if (!clean(value)) return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function daysSince(value, now = new Date()) {
  const parsed = parseDate(value);
  if (!parsed) return 0;
  return Math.max(0, Math.floor((now.getTime() - parsed.getTime()) / (1000 * 60 * 60 * 24)));
}

function isProducing(status) {
  return isProducingStatus(status);
}

function isDeferred(status) {
  const normalized = normalizeProductionStatus(status);
  return (
    normalized === "Shut-in" ||
    normalized === "Standby" ||
    normalized === "Mothball" ||
    normalized === "Locked Potential"
  );
}

function isHighPriority(dn) {
  return lower(dn?.priority) === "high" || lower(dn?.priority) === "critical";
}

function isBlockedDN(dn) {
  const status = lower(dn?.workflow_status || dn?.dn_status || dn?.status);
  const step = lower(dn?.current_step);

  return (
    status.includes("waiting") ||
    status.includes("hold") ||
    status.includes("delay") ||
    status.includes("rfi") ||
    status.includes("not issuing") ||
    step.includes("package") ||
    step.includes("rfi")
  );
}

function isProgressingDN(dn) {
  if (isBlockedDN(dn) || isDNResolvedStatus(dn?.workflow_status || dn?.dn_status || dn?.status)) {
    return false;
  }

  const progress = num(dn?.progress_percent, 0);
  const status = lower(dn?.workflow_status || dn?.dn_status || dn?.status);
  const step = lower(dn?.current_step);

  return (
    progress > 0 ||
    status.includes("progress") ||
    status.includes("execution") ||
    step.includes("execution") ||
    step.includes("depressur")
  );
}

function getDNFieldCode(dn, wellMap) {
  const linkedWell = wellMap.get(clean(dn?.well_id));
  return normalizeFieldCode(dn?.field_code, linkedWell?.well_name, linkedWell?.field);
}

function buildFieldBucket(fieldCode) {
  return {
    field_code: fieldCode,
    producing_rate_bopd: 0,
    deferred_rate_bopd: 0,
    active_dn_count: 0,
    blocked_dn_count: 0,
    progressing_dn_count: 0,
    aged_dn_count: 0,
    high_priority_dn_count: 0,
    dn_pressure_bopd: 0,
    impacted_wells: new Set(),
    active_dns: [],
    owner_counts: new Map(),
    formation_gain_bopd: 0,
    formation_blocked_count: 0,
    formation_progress_avg: 0,
    formation_active_projects: 0
  };
}

function getFormationFieldData(intelligence, fieldCode) {
  return intelligence?.formation?.by_field?.[fieldCode] || null;
}

function buildOwnerConcentration(ownerCounts) {
  const owners = Array.from(ownerCounts.entries())
    .map(([owner, count]) => ({ owner, count }))
    .sort((a, b) => b.count - a.count);

  const topOwner = owners[0] || null;
  return {
    top_owner: topOwner && topOwner.owner ? topOwner : null,
    owners: owners.slice(0, 5)
  };
}

function buildFieldExposure(bucket) {
  return round(
    bucket.blocked_dn_count * 3 +
      bucket.aged_dn_count * 2 +
      bucket.high_priority_dn_count * 2 +
      bucket.progressing_dn_count +
      bucket.dn_pressure_bopd / 500 +
      bucket.deferred_rate_bopd / 500 +
      bucket.formation_blocked_count * 1.5,
    1
  );
}

function buildActionReasoning(topDN, topWell, topFormation) {
  return {
    top_dn: topDN
      ? `DN ${topDN.dn_id} is ranked highly because it combines ${topDN.priority || "active"} priority, ${topDN.age_days} aging days, ${topDN.blocked ? "blocked workflow" : "active workflow"}, and ${topDN.estimated_loss_bopd} BOPD of exposed production.`
      : "",
    top_well: topWell
      ? `Well ${topWell.well_name} is a high-value recovery target because ${topWell.active_dn_count} active DN(s) are constraining ${topWell.deferred_rate_bopd} BOPD of deferred potential.`
      : "",
    top_formation: topFormation
      ? `Formation project ${topFormation.project_name} matters now because it carries ${topFormation.estimated_gain_bopd} BOPD upside at ${topFormation.progress_percent}% progress with readiness state ${topFormation.readiness_state}.`
      : ""
  };
}

function enhanceIntelligence({ wells = [], dns = [], intelligence = null }) {
  const now = new Date();
  const safeWells = safeArray(wells);
  const safeDNs = safeArray(dns);
  const fieldBuckets = {
    ANDR: buildFieldBucket("ANDR"),
    ABQQ: buildFieldBucket("ABQQ")
  };

  const wellMap = new Map();

  safeWells.forEach((w) => {
    const id = clean(w.well_id || w.id);
    if (!id) return;

    const normalizedWell = {
      ...w,
      well_id: id,
      well_name: clean(w.well_name),
      rate: num(w.oil_rate_bopd, 0),
      status: normalizeProductionStatus(w.production_status),
      field_code: normalizeFieldCode(w.field_code, w.well_name, w.field)
    };

    wellMap.set(id, normalizedWell);

    const bucket = fieldBuckets[normalizedWell.field_code];
    if (!bucket) return;

    if (isProducing(normalizedWell.status)) {
      bucket.producing_rate_bopd += normalizedWell.rate;
    }

    if (isDeferred(normalizedWell.status) && normalizedWell.rate > 0) {
      bucket.deferred_rate_bopd += normalizedWell.rate;
    }
  });

  const dnImpact = [];

  safeDNs.forEach((dn) => {
    const statusText = dn?.workflow_status || dn?.dn_status || dn?.status;
    if (isDNResolvedStatus(statusText)) return;

    const well = wellMap.get(clean(dn?.well_id));
    const fieldCode = getDNFieldCode(dn, wellMap);
    const bucket = fieldBuckets[fieldCode];
    if (!bucket) return;

    const priority = lower(dn?.priority);
    const blocked = isBlockedDN(dn);
    const progressing = isProgressingDN(dn);
    const ageDays = daysSince(dn?.created_date || dn?.update_date, now);
    const aged = ageDays >= 14;
    const exposedRate = num(well?.rate, 0);

    bucket.active_dn_count += 1;
    bucket.active_dns.push(dn);
    if (blocked) bucket.blocked_dn_count += 1;
    if (progressing) bucket.progressing_dn_count += 1;
    if (aged) bucket.aged_dn_count += 1;
    if (isHighPriority(dn)) bucket.high_priority_dn_count += 1;

    const owner = clean(dn?.dn_owner || dn?.owner || dn?.current_owner_name) || "Unknown";
    bucket.owner_counts.set(owner, (bucket.owner_counts.get(owner) || 0) + 1);

    if (exposedRate > 0 && (blocked || isHighPriority(dn) || aged)) {
      bucket.dn_pressure_bopd += exposedRate;
      bucket.impacted_wells.add(clean(dn?.well_id));

      dnImpact.push({
        dn_id: clean(dn?.dn_id),
        well_id: clean(dn?.well_id),
        well_name: clean(well?.well_name),
        field_code: fieldCode,
        owner,
        priority: clean(dn?.priority),
        blocked,
        age_days: ageDays,
        estimated_loss_bopd: exposedRate
      });
    }
  });

  Object.keys(fieldBuckets).forEach((fieldCode) => {
    const formationField = getFormationFieldData(intelligence, fieldCode);
    const bucket = fieldBuckets[fieldCode];
    if (!formationField) return;

    bucket.formation_gain_bopd = num(formationField.expected_gain_bopd, 0);
    bucket.formation_blocked_count = num(formationField.blocked_projects_count, 0);
    bucket.formation_active_projects = num(formationField.active_projects, 0);
    bucket.formation_progress_avg =
      bucket.formation_active_projects > 0
        ? round(
            safeArray(formationField.projects).reduce(
              (sum, project) => sum + num(project.progress_percent, 0),
              0
            ) / Math.max(bucket.formation_active_projects, 1),
            1
          )
        : 0;
  });

  dnImpact.sort((a, b) => {
    if (b.estimated_loss_bopd !== a.estimated_loss_bopd) {
      return b.estimated_loss_bopd - a.estimated_loss_bopd;
    }
    if (b.age_days !== a.age_days) return b.age_days - a.age_days;
    return clean(a.dn_id).localeCompare(clean(b.dn_id));
  });

  const producing = safeWells
    .filter((w) => isProducing(w.production_status))
    .map((w) => num(w.oil_rate_bopd, 0));

  const anomalies = [];

  if (producing.length > 0) {
    const mean = producing.reduce((a, b) => a + b, 0) / producing.length;
    const variance =
      producing.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / producing.length;
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

  const andr = fieldBuckets.ANDR;
  const abqq = fieldBuckets.ABQQ;
  const totalProducing = andr.producing_rate_bopd + abqq.producing_rate_bopd;
  const exposureANDR = buildFieldExposure(andr);
  const exposureABQQ = buildFieldExposure(abqq);
  const dnPressureField =
    andr.dn_pressure_bopd >= abqq.dn_pressure_bopd ? "ANDR" : "ABQQ";
  const deferredField =
    andr.deferred_rate_bopd >= abqq.deferred_rate_bopd ? "ANDR" : "ABQQ";
  const exposureField = exposureANDR >= exposureABQQ ? "ANDR" : "ABQQ";
  const formationUpsideField =
    andr.formation_gain_bopd >= abqq.formation_gain_bopd ? "ANDR" : "ABQQ";

  let imbalance = "balanced";
  let diff = 0;

  if (totalProducing > 0) {
    const andrShare = (andr.producing_rate_bopd / totalProducing) * 100;
    const abqqShare = (abqq.producing_rate_bopd / totalProducing) * 100;
    diff = Math.abs(andrShare - abqqShare);

    if (diff >= 20) {
      imbalance = andrShare > abqqShare ? "ANDR-heavy" : "ABQQ-heavy";
    }
  }

  const dnOwners = {
    ANDR: buildOwnerConcentration(andr.owner_counts),
    ABQQ: buildOwnerConcentration(abqq.owner_counts)
  };

  const dnConcentration = {
    highest_field: andr.active_dn_count >= abqq.active_dn_count ? "ANDR" : "ABQQ",
    highest_owner: dnOwners[dnPressureField].top_owner,
    by_field: {
      ANDR: {
        active_dn_count: andr.active_dn_count,
        blocked_dn_count: andr.blocked_dn_count,
        progressing_dn_count: andr.progressing_dn_count,
        aged_dn_count: andr.aged_dn_count,
        high_priority_dn_count: andr.high_priority_dn_count,
        owner_concentration: dnOwners.ANDR
      },
      ABQQ: {
        active_dn_count: abqq.active_dn_count,
        blocked_dn_count: abqq.blocked_dn_count,
        progressing_dn_count: abqq.progressing_dn_count,
        aged_dn_count: abqq.aged_dn_count,
        high_priority_dn_count: abqq.high_priority_dn_count,
        owner_concentration: dnOwners.ABQQ
      }
    },
    insight:
      `${dnPressureField} carries the heavier DN pressure with ${fieldBuckets[dnPressureField].active_dn_count} active DNs, ` +
      `${fieldBuckets[dnPressureField].blocked_dn_count} blocked items, and ${fieldBuckets[dnPressureField].aged_dn_count} aged items.`
  };

  const totalDNPressure = round(andr.dn_pressure_bopd + abqq.dn_pressure_bopd, 1);
  const totalFormationGain = round(andr.formation_gain_bopd + abqq.formation_gain_bopd, 1);
  const offsetRatio = totalDNPressure > 0 ? round(totalFormationGain / totalDNPressure, 2) : null;

  const gainVsLoss = {
    dn_pressure_bopd: totalDNPressure,
    formation_gain_bopd: totalFormationGain,
    deferred_recovery_bopd: round(andr.deferred_rate_bopd + abqq.deferred_rate_bopd, 1),
    offset_ratio: offsetRatio,
    pressure_field: dnPressureField,
    upside_field: formationUpsideField,
    insight:
      totalDNPressure > 0
        ? `${dnPressureField} is carrying the heaviest live DN pressure, while ${formationUpsideField} holds the stronger formation upside. ` +
          `Current formation upside ${totalFormationGain} BOPD ${totalFormationGain >= totalDNPressure ? "can offset" : "does not yet offset"} the ${totalDNPressure} BOPD DN pressure signal.`
        : totalFormationGain > 0
          ? `${formationUpsideField} holds the main formation upside with ${totalFormationGain} BOPD available and limited active DN pressure in the current data.`
          : "Current data shows limited DN pressure and no active formation upside."
  };

  const topRecoveryField =
    andr.deferred_rate_bopd + andr.formation_gain_bopd >=
    abqq.deferred_rate_bopd + abqq.formation_gain_bopd
      ? "ANDR"
      : "ABQQ";

  const topFormationProject =
    safeArray(intelligence?.formation?.intelligence?.top_gain_projects)[0] || null;

  const deferredWellLeaders = safeWells
    .map((well) => {
      const fieldCode = normalizeFieldCode(well.field_code, well.well_name, well.field);
      const activeDNCount = safeDNs.filter(
        (dn) =>
          clean(dn?.well_id) === clean(well?.well_id) &&
          !isDNResolvedStatus(dn?.workflow_status || dn?.dn_status || dn?.status)
      ).length;

      return {
        well_id: clean(well?.well_id),
        well_name: clean(well?.well_name),
        field_code: fieldCode,
        deferred_rate_bopd: isDeferred(well?.production_status) ? num(well?.oil_rate_bopd, 0) : 0,
        active_dn_count: activeDNCount
      };
    })
    .filter((well) => well.deferred_rate_bopd > 0 && well.active_dn_count > 0)
    .sort((a, b) => b.deferred_rate_bopd - a.deferred_rate_bopd || b.active_dn_count - a.active_dn_count);

  const actionReasoning = buildActionReasoning(
    dnImpact[0] || null,
    deferredWellLeaders[0] || null,
    topFormationProject
  );

  const executiveSignals = {
    biggest_operational_pressure_field: exposureField,
    biggest_recovery_opportunity_field: topRecoveryField,
    biggest_gain_source:
      totalFormationGain >= round(andr.deferred_rate_bopd + abqq.deferred_rate_bopd, 1)
        ? "formation_line"
        : "well_recovery",
    biggest_bottleneck_source:
      andr.blocked_dn_count + abqq.blocked_dn_count >=
      andr.formation_blocked_count + abqq.formation_blocked_count
        ? "dn_blockage"
        : "formation_blockage",
    insight:
      `${exposureField} is under the heaviest operational pressure, while ${topRecoveryField} carries the stronger recovery opportunity through ` +
      `${fieldBuckets[topRecoveryField].deferred_rate_bopd} BOPD deferred potential and ${fieldBuckets[topRecoveryField].formation_gain_bopd} BOPD formation upside.`
  };

  return {
    dn_production_impact: dnImpact,
    anomalies,
    field_imbalance: {
      imbalance,
      difference_percent: round(diff, 1),
      details: {
        andr_rate_bopd: round(andr.producing_rate_bopd, 1),
        abqq_rate_bopd: round(abqq.producing_rate_bopd, 1),
        dn_pressure_field: dnPressureField,
        dn_pressure_bopd: totalDNPressure,
        deferred_pressure_field: deferredField,
        andr_deferred_bopd: round(andr.deferred_rate_bopd, 1),
        abqq_deferred_bopd: round(abqq.deferred_rate_bopd, 1),
        operational_pressure_field: exposureField,
        andr_exposure_score: exposureANDR,
        abqq_exposure_score: exposureABQQ,
        formation_upside_field: formationUpsideField,
        andr_formation_gain_bopd: round(andr.formation_gain_bopd, 1),
        abqq_formation_gain_bopd: round(abqq.formation_gain_bopd, 1)
      }
    },
    dn_concentration: dnConcentration,
    gain_vs_loss: gainVsLoss,
    executive_signals: executiveSignals,
    action_reasoning: actionReasoning
  };
}

module.exports = {
  enhanceIntelligence
};
