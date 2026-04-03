// src/core/normalizer.js
// Enhanced normalization using domain entities (parallel, non-destructive)

const { Well, DN } = require('./domain');

function createWellFromRaw(rawData) {
  try {
    if (!rawData) return null;
    return new Well(rawData);
  } catch (error) {
    console.error('[Normalizer] Failed to create Well instance:', error);
    return null;
  }
}

function createDNFromMerged(masterData, logData) {
  try {
    if (!masterData || !logData) return null;
    const merged = {
      dn_id: masterData.dn_id || logData.dn_id,
      well_id: masterData.well_id,
      dn_type: masterData.dn_type,
      dn_type_id: masterData.dn_type_id,
      type_group: masterData.type_group,
      priority: masterData.priority,
      created_date: masterData.created_date,
      progress_percent: masterData.progress_percent,
      dn_status: logData.status_update || masterData.dn_status,
      dn_owner: logData.updated_by,
      update_date: logData.update_date,
      ...masterData,
      ...logData
    };
    return new DN(merged);
  } catch (error) {
    console.error('[Normalizer] Failed to create DN instance:', error);
    return null;
  }
}

function buildDNLatestStateMap(dnLogsArray, dnMasterArray) {
  const dnMap = new Map();
  const latestMap = {};

  for (const row of dnLogsArray) {
    const dnId = row.dn_id;
    if (!dnId) continue;

    if (!latestMap[dnId]) {
      latestMap[dnId] = row;
      continue;
    }

    const currentDate = new Date(row.update_date || 0);
    const savedDate = new Date(latestMap[dnId].update_date || 0);

    if (currentDate > savedDate) {
      latestMap[dnId] = row;
    }
  }

  const latestLogs = Object.values(latestMap);

  for (const log of latestLogs) {
    const meta = dnMasterArray.find((d) => String(d.dn_id) === String(log.dn_id));
    if (!meta) continue;

    const dnInstance = createDNFromMerged(meta, log);
    if (dnInstance) {
      dnMap.set(String(dnInstance.dn_id), dnInstance);
    }
  }

  return dnMap;
}

function buildWellIndex(wellsArray) {
  const wellIndex = new Map();
  const nameIndex = new Map();

  for (const rawWell of wellsArray) {
    const well = createWellFromRaw(rawWell);
    if (!well) continue;

    wellIndex.set(String(well.well_id), well);

    const normalizedName = normalizeTextForSearch(well.well_name);
    if (normalizedName) {
      nameIndex.set(normalizedName, well.well_id);
    }
  }

  return { wellIndex, nameIndex };
}

function normalizeTextForSearch(value) {
  const text = String(value || '')
    .toLowerCase()
    .replace(/[_\s]+/g, '-')
    .replace(/[^a-z0-9-]/g, '')
    .trim();
  return text || null;
}

function getDNsForWell(wellId, dnMap) {
  const dns = [];
  for (const [, dn] of dnMap.entries()) {
    if (String(dn.well_id).trim() === String(wellId).trim()) {
      dns.push(dn);
    }
  }
  return dns.sort((a, b) => {
    const priorityOrder = { 'High': 0, 'Medium': 1, 'Low': 2 };
    return (priorityOrder[a.priority] || 999) - (priorityOrder[b.priority] || 999);
  });
}

module.exports = {
  createWellFromRaw,
  createDNFromMerged,
  buildDNLatestStateMap,
  buildWellIndex,
  getDNsForWell,
  normalizeTextForSearch
};