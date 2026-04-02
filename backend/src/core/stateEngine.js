// src/core/stateEngine.js
// Domain state tracking and well classification

function classifyWell(well, dnMap) {
  if (!well) {
    return { classification: null, reason: 'Well not found', affectingDNs: [] };
  }

  const affectingDNs = getActiveWellDNs(well, dnMap);
  const hasActiveDNs = affectingDNs.length > 0;

  if (well.isProducing()) {
    if (hasActiveDNs) {
      return {
        classification: 'LOCKED_BY_WORKFLOW',
        reason: `Producing but has ${affectingDNs.length} active DN(s)`,
        affectingDNs
      };
    }
    return {
      classification: 'ACTIVE_PRODUCER',
      reason: 'On production, no active DNs',
      affectingDNs: []
    };
  }

  if (well.isPotentiallyLocked()) {
    if (hasActiveDNs) {
      return {
        classification: 'LOCKED_BY_WORKFLOW',
        reason: `Locked with ${affectingDNs.length} active DN(s)`,
        affectingDNs
      };
    }
    return {
      classification: 'LOCKED_OTHER',
      reason: 'Locked without active DNs (other constraint)',
      affectingDNs: []
    };
  }

  const status = String(well.production_status || '').toLowerCase();

  if (status === 'testing') {
    return {
      classification: 'TESTING',
      reason: 'In testing phase',
      affectingDNs
    };
  }

  if (status === 'standby') {
    return {
      classification: 'STANDBY_CAPABLE',
      reason: 'Standby capable',
      affectingDNs
    };
  }

  if (status === 'shut-in' || status === 'shut in') {
    return {
      classification: 'INACTIVE_SHUT_IN',
      reason: 'Shut-in',
      affectingDNs
    };
  }

  if (status === 'mothball') {
    return {
      classification: 'INACTIVE_MOTHBALL',
      reason: 'Mothballed',
      affectingDNs: []
    };
  }

  return {
    classification: null,
    reason: `Unknown status: ${well.production_status}`,
    affectingDNs
  };
}

function getActiveWellDNs(well, dnMap) {
  if (!well || !dnMap) return [];

  const activeDNs = [];
  for (const [, dn] of dnMap.entries()) {
    if (String(dn.well_id).trim() === String(well.well_id).trim() && dn.isActive()) {
      activeDNs.push(dn);
    }
  }

  return activeDNs.sort((a, b) => {
    const priorityOrder = { High: 0, Medium: 1, Low: 2 };
    return (priorityOrder[a.priority] || 999) - (priorityOrder[b.priority] || 999);
  });
}

function detectBlockedProduction(well, dnMap) {
  if (!well) {
    return { status: 'NO_BLOCK', lostRate: 0, affectingDNIds: [] };
  }

  const activeDNs = getActiveWellDNs(well, dnMap);
  const affectingDNIds = activeDNs.map((dn) => dn.dn_id);
  const hasActiveDNs = activeDNs.length > 0;

  if (!hasActiveDNs) {
    return { status: 'NO_BLOCK', lostRate: 0, affectingDNIds: [] };
  }

  if (well.isPotentiallyLocked()) {
    return {
      status: 'LOCKED_BY_DN',
      lostRate: well.oil_rate_bopd,
      affectingDNIds
    };
  }

  if (well.isProducing()) {
    return {
      status: 'BLOCKED_BUT_PRODUCING',
      lostRate: 0,
      affectingDNIds
    };
  }

  return {
    status: 'PRODUCTION_STOPPED',
    lostRate: well.oil_rate_bopd,
    affectingDNIds
  };
}

function detectWorkflowBottlenecks(dnMap, stuckThresholdDays = 14) {
  const bottlenecks = {
    stuckDNs: [],
    byPhase: {},
    byPriority: {}
  };

  if (!dnMap) return bottlenecks;

  for (const [, dn] of dnMap.entries()) {
    if (!dn.isActive()) continue;

    if (dn.isDaysStuck(stuckThresholdDays)) {
      bottlenecks.stuckDNs.push({
        dnId: dn.dn_id,
        wellId: dn.well_id,
        phase: dn.getPhase(),
        daysStuck: dn.getDaysSinceLastUpdate(),
        priority: dn.priority,
        owner: dn.dn_owner
      });
    }

    const phase = dn.getPhase();
    if (!bottlenecks.byPhase[phase]) {
      bottlenecks.byPhase[phase] = [];
    }
    bottlenecks.byPhase[phase].push(dn.dn_id);

    const priority = dn.priority;
    if (!bottlenecks.byPriority[priority]) {
      bottlenecks.byPriority[priority] = [];
    }
    bottlenecks.byPriority[priority].push(dn.dn_id);
  }

  return bottlenecks;
}

module.exports = {
  classifyWell,
  getActiveWellDNs,
  detectBlockedProduction,
  detectWorkflowBottlenecks
};