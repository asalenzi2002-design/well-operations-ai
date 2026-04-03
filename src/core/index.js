// src/core/index.js
// Core engine initialization and exports

const domain = require('./domain');
const normalizer = require('./normalizer');
const stateEngine = require('./stateEngine');

function initializeCoreEngine(wellsArray, dnLogsArray, dnMasterArray) {
  try {
    const { wellIndex, nameIndex } = normalizer.buildWellIndex(wellsArray);
    const dnIndexByid = normalizer.buildDNLatestStateMap(dnLogsArray, dnMasterArray);

    const wellClassifications = new Map();
    for (const [wellId, well] of wellIndex.entries()) {
      const classification = stateEngine.classifyWell(well, dnIndexByid);
      wellClassifications.set(wellId, classification);
    }

    const blockedProduction = new Map();
    for (const [wellId, well] of wellIndex.entries()) {
      const blocked = stateEngine.detectBlockedProduction(well, dnIndexByid);
      blockedProduction.set(wellId, blocked);
    }

    const bottlenecks = stateEngine.detectWorkflowBottlenecks(dnIndexByid);

    console.log('[CoreEngine] Initialization complete');
    console.log(`[CoreEngine] ${wellIndex.size} wells indexed`);
    console.log(`[CoreEngine] ${dnIndexByid.size} DNs indexed`);
    console.log(`[CoreEngine] ${wellClassifications.size} wells classified`);
    console.log(`[CoreEngine] Bottlenecks: ${bottlenecks.stuckDNs.length} stuck DNs`);

    return {
      wellIndex,
      nameIndex,
      dnIndexByid,
      wellClassifications,
      blockedProduction,
      bottlenecks,
      getWell: (wellId) => wellIndex.get(String(wellId)),
      getDN: (dnId) => dnIndexByid.get(String(dnId)),
      getDNsForWell: (wellId) => normalizer.getDNsForWell(wellId, dnIndexByid),
      getWellClassification: (wellId) => wellClassifications.get(String(wellId)),
      getBlockedProduction: (wellId) => blockedProduction.get(String(wellId))
    };
  } catch (error) {
    console.error('[CoreEngine] Initialization failed:', error);
    return null;
  }
}

module.exports = {
  domain,
  normalizer,
  stateEngine,
  initializeCoreEngine
};