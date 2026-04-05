require("dotenv").config();

const express = require("express");
const fs = require("fs");
const path = require("path");
const CoreEngine = require("./src/core");
const {
  normalizeFieldCode,
  normalizeProductionStatus: normalizeDomainProductionStatus,
  isProducingStatus,
  interpretDNWorkflowStatus,
  interpretDNCurrentStep,
  isDNResolvedStatus,
  compareDNLogRows
} = require("./src/core/domain");
const {
  calculateWellRisk,
  calculateDNRisk,
  buildRiskDashboard
} = require("./src/logic/riskEngine");
const { buildSystemIntelligence } = require("./src/logic/intelligenceBuilders");
const dashboardBuilders = require("./src/logic/dashboardBuilders");
const { generateRecommendations } = require("./src/logic/recommendationEngine");
const {
  calculateDNImpact: calculateOperationalDNImpact
} = require("./src/logic/dnImpactEngine");
const {
  buildFormationOperationalSummary
} = require("./src/logic/formationLineEngine");
const { buildExecutionPlan } = require("./src/logic/executionEngine");

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3002;

let dnMaster = [];
let wells = [];
let dnLogs = [];
let formationProjects = [];
let formationTasks = [];
let coreEngine = null;

let normalizedWellsCache = null;
let latestDNMapCache = null;
let mergedLatestDNsCache = null;
let latestDNsByWellCache = null;
let formationSummaryCache = null;
let globalDashboardContextCache = null;

const productionHistory = [
  { timestamp: "2026-04-02T00:00:00Z", bopd: 980 },
  { timestamp: "2026-04-02T01:00:00Z", bopd: 1020 },
  { timestamp: "2026-04-02T02:00:00Z", bopd: 995 },
  { timestamp: "2026-04-02T03:00:00Z", bopd: 1010 },
  { timestamp: "2026-04-02T04:00:00Z", bopd: 970 },
  { timestamp: "2026-04-02T05:00:00Z", bopd: 990 },
  { timestamp: "2026-04-02T06:00:00Z", bopd: 1005 },
  { timestamp: "2026-04-02T07:00:00Z", bopd: 1030 },
  { timestamp: "2026-04-02T08:00:00Z", bopd: 1060 },
  { timestamp: "2026-04-02T09:00:00Z", bopd: 1045 },
  { timestamp: "2026-04-02T10:00:00Z", bopd: 1080 },
  { timestamp: "2026-04-02T11:00:00Z", bopd: 1075 }
];

/* =========================
   GENERAL HELPERS
========================= */
function toSafeString(value) {
  return String(value ?? "").trim();
}

function toSafeLower(value) {
  return toSafeString(value).toLowerCase();
}

function toNumber(value, fallback = 0) {
  if (value === null || value === undefined) return fallback;

  const cleaned = String(value).replace(/,/g, "").replace(/%/g, "").trim();
  const parsed = Number(cleaned);

  return Number.isFinite(parsed) ? parsed : fallback;
}

function normalizeText(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[_\s]+/g, "-")
    .replace(/[^a-z0-9-]/g, "")
    .trim();
}

function invalidateDataCaches() {
  normalizedWellsCache = null;
  latestDNMapCache = null;
  mergedLatestDNsCache = null;
  latestDNsByWellCache = null;
  formationSummaryCache = null;
  globalDashboardContextCache = null;
}

function parseCSVLine(line) {
  const cells = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];
    const nextChar = line[i + 1];

    if (char === '"') {
      if (inQuotes && nextChar === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === "," && !inQuotes) {
      cells.push(current);
      current = "";
      continue;
    }

    current += char;
  }

  cells.push(current);
  return cells;
}

function parseCSV(filePath) {
  const raw = fs.readFileSync(filePath, "utf8");
  if (!raw || !raw.trim()) return [];

  const normalizedRaw = raw.replace(/^\uFEFF/, "");
  const lines = [];
  let currentLine = "";
  let inQuotes = false;

  for (let i = 0; i < normalizedRaw.length; i += 1) {
    const char = normalizedRaw[i];
    const nextChar = normalizedRaw[i + 1];

    currentLine += char;

    if (char === '"') {
      if (inQuotes && nextChar === '"') {
        currentLine += nextChar;
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
    }

    if ((char === "\n" || char === "\r") && !inQuotes) {
      if (char === "\r" && nextChar === "\n") {
        i += 1;
      }
      const trimmedLine = currentLine.replace(/[\r\n]+$/, "");
      if (trimmedLine.length > 0) {
        lines.push(trimmedLine);
      }
      currentLine = "";
    }
  }

  if (currentLine.trim().length > 0) {
    lines.push(currentLine);
  }

  if (lines.length === 0) return [];

  const headers = parseCSVLine(lines[0]).map((header) => toSafeString(header));

  return lines.slice(1).map((line) => {
    const values = parseCSVLine(line);
    const row = {};

    headers.forEach((header, index) => {
      row[header] = values[index] !== undefined ? toSafeString(values[index]) : "";
    });

    return row;
  });
}

function getCoreEngineDNMap(engine) {
  if (!engine) return null;
  if (engine.dnIndexById instanceof Map) return engine.dnIndexById;
  if (engine.dnIndexByid instanceof Map) return engine.dnIndexByid;
  if (engine.dnIndex instanceof Map) return engine.dnIndex;
  return null;
}

/* =========================
   WELL NORMALIZATION
========================= */
function extractFieldCode(wellName, fieldName) {
  return normalizeFieldCode("", wellName, fieldName);
}

function normalizeProductionStatus(status) {
  return normalizeDomainProductionStatus(status);
}

function normalizeOilRate(well) {
  const raw =
    well?.oil_rate_bopd ??
    well?.oil_rate ??
    well?.oilrate ??
    well?.rate ??
    0;

  return toNumber(raw, 0);
}

function normalizeWell(well) {
  const safeWell = well || {};
  const wellName = toSafeString(safeWell.well_name || safeWell.name);
  const productionStatus = normalizeProductionStatus(safeWell.production_status);
  const oilRate = normalizeOilRate(safeWell);
  const fieldCode = normalizeFieldCode(
    safeWell.field_code,
    wellName,
    safeWell.field
  );

  return {
    ...safeWell,
    well_id: toSafeString(safeWell.well_id || safeWell.id),
    well_name: wellName,
    field_code: fieldCode,
    production_status: productionStatus,
    oil_rate_bopd: oilRate,
    is_active: normalizeProductionStatus(productionStatus) !== "Shut-in",
    last_updated: toSafeString(safeWell.last_updated)
  };
}

function getNormalizedWells() {
  if (normalizedWellsCache) {
    return normalizedWellsCache;
  }

  normalizedWellsCache = Array.isArray(wells) ? wells.map(normalizeWell) : [];
  return normalizedWellsCache;
}

/* =========================
   DN NORMALIZATION
========================= */
function normalizeWorkflowStatus(statusText) {
  return interpretDNWorkflowStatus(statusText);
}

function normalizeCurrentStep(statusText) {
  return interpretDNCurrentStep(statusText);
}

function normalizeProgressValue(progressValue) {
  const parsed = toNumber(progressValue, 0);
  if (parsed < 0) return 0;
  if (parsed > 100) return 100;
  return parsed;
}

function normalizeDN(item) {
  const safeItem = item || {};

  const latestStatus = toSafeString(
    safeItem.dn_status ||
      safeItem.status_update ||
      safeItem.status ||
      safeItem.latest_update
  );

  const workflowStatus = normalizeWorkflowStatus(latestStatus);
  const progressPercent = normalizeProgressValue(
    safeItem.progress_percent ?? safeItem.progress ?? 0
  );

  const ownerName = toSafeString(
    safeItem.current_owner_name ||
      safeItem.owner ||
      safeItem.dn_owner ||
      safeItem.updated_by
  );

  const progressString =
    typeof safeItem.progress === "string" && toSafeString(safeItem.progress)
      ? toSafeString(safeItem.progress)
      : `${progressPercent}%`;

  return {
    ...safeItem,
    dn_id: toSafeString(safeItem.dn_id),
    well_id: toSafeString(safeItem.well_id),
    dn_type: toSafeString(safeItem.dn_type || safeItem.type || "Unknown"),
    priority: toSafeString(safeItem.priority || "Unknown"),
    created_date: toSafeString(safeItem.created_date),
    update_date: toSafeString(safeItem.update_date || safeItem.last_updated),
    last_updated: toSafeString(safeItem.last_updated || safeItem.update_date),
    dn_status: latestStatus || "Unknown",
    status: latestStatus || "Unknown",
    owner: ownerName || "Unknown",
    dn_owner: ownerName || "Unknown",
    current_owner_name: ownerName || "Unknown",
    progress_percent: progressPercent,
    progress: progressString,
    workflow_status: workflowStatus,
    current_step: normalizeCurrentStep(latestStatus),
    is_closed: isDNResolvedStatus(latestStatus)
  };
}

function getLatestDNMap() {
  if (latestDNMapCache) {
    return latestDNMapCache;
  }

  const latestMap = new Map();

  for (const row of Array.isArray(dnLogs) ? dnLogs : []) {
    const dnId = toSafeString(row?.dn_id);
    if (!dnId) continue;

    const existing = latestMap.get(dnId);
    if (!existing) {
      latestMap.set(dnId, row);
      continue;
    }

    if (compareDNLogRows(row, existing) > 0) {
      latestMap.set(dnId, row);
    }
  }

  latestDNMapCache = latestMap;
  return latestDNMapCache;
}

function getMergedLatestDNs() {
  if (mergedLatestDNsCache) {
    return mergedLatestDNsCache;
  }

  const latestLogMap = getLatestDNMap();

  mergedLatestDNsCache = (Array.isArray(dnMaster) ? dnMaster : [])
    .map((meta) => {
      const dnId = toSafeString(meta?.dn_id);
      if (!dnId) return null;

      const latestLog = latestLogMap.get(dnId);

      const merged = {
        ...meta,
        dn_id: dnId,
        well_id: toSafeString(meta?.well_id),
        dn_type: toSafeString(meta?.dn_type),
        priority: toSafeString(meta?.priority),
        created_date: toSafeString(meta?.created_date),
        progress_percent: normalizeProgressValue(
          meta?.progress_percent ?? meta?.progress ?? 0
        ),
        dn_status: toSafeString(latestLog?.status_update || meta?.status || ""),
        status_update: toSafeString(latestLog?.status_update || ""),
        update_date: toSafeString(latestLog?.update_date || meta?.update_date || ""),
        last_updated: toSafeString(latestLog?.update_date || meta?.update_date || ""),
        dn_owner: toSafeString(latestLog?.updated_by || meta?.owner || ""),
        owner: toSafeString(latestLog?.updated_by || meta?.owner || ""),
        current_owner_name: toSafeString(latestLog?.updated_by || meta?.owner || "")
      };

      return normalizeDN(merged);
    })
    .filter(Boolean);

  return mergedLatestDNsCache;
}

function getLatestDNsByWellMap() {
  if (latestDNsByWellCache) {
    return latestDNsByWellCache;
  }

  const byWell = new Map();

  for (const dn of getMergedLatestDNs()) {
    const wellId = toSafeString(dn?.well_id);
    if (!wellId) continue;

    if (!byWell.has(wellId)) {
      byWell.set(wellId, []);
    }

    byWell.get(wellId).push(dn);
  }

  latestDNsByWellCache = byWell;
  return latestDNsByWellCache;
}

function getLatestDNsForWell(wellId) {
  const targetWellId = toSafeString(wellId);
  if (!targetWellId) return [];

  const byWell = getLatestDNsByWellMap();
  return byWell.get(targetWellId) || [];
}

function getConsistentWells() {
  if (coreEngine?.wellIndex instanceof Map) {
    return Array.from(coreEngine.wellIndex.values());
  }
  return getNormalizedWells();
}

function getConsistentDNs() {
  const dnMap = getCoreEngineDNMap(coreEngine);
  if (dnMap instanceof Map) {
    return Array.from(dnMap.values());
  }
  return getMergedLatestDNs();
}

function getConsistentDNsByWellMap() {
  const byWell = new Map();

  for (const dn of getConsistentDNs()) {
    const wellId = toSafeString(dn?.well_id);
    if (!wellId) continue;

    if (!byWell.has(wellId)) {
      byWell.set(wellId, []);
    }

    byWell.get(wellId).push(dn);
  }

  return byWell;
}

function getConsistentDNsForWell(wellId) {
  const targetWellId = toSafeString(wellId);
  if (!targetWellId) return [];

  if (coreEngine && typeof coreEngine.getDNsForWell === "function") {
    return coreEngine.getDNsForWell(targetWellId) || [];
  }

  return getLatestDNsForWell(targetWellId);
}

function getFormationSummary() {
  if (formationSummaryCache) {
    return formationSummaryCache;
  }

  formationSummaryCache = buildFormationOperationalSummary(
    formationProjects,
    formationTasks
  );
  return formationSummaryCache;
}

function buildSafeRiskData(wellsData, dnsData) {
  try {
    return buildRiskDashboard(wellsData, dnsData);
  } catch (_error) {
    return { top_risk_wells: [], top_risk_dns: [] };
  }
}

function getGlobalDashboardContext() {
  if (globalDashboardContextCache) {
    return globalDashboardContextCache;
  }

  const wellsData = getConsistentWells();
  const dnsData = getConsistentDNs();
  const dnsByWell = getConsistentDNsByWellMap();
  const riskData = buildSafeRiskData(wellsData, dnsData);
  const intelligence = buildSystemIntelligence({
    wells: wellsData,
    dns: dnsData,
    formationProjects,
    formationTasks,
    riskData,
    riskItems: Array.isArray(riskData?.top_risk_wells) ? riskData.top_risk_wells : []
  });
  const recommendations = generateRecommendations({
    wells: wellsData,
    dns: dnsData,
    formationProjects,
    formationTasks,
    risk: riskData,
    intelligence
  });
  const totalRate = calculateTotalRate(wellsData);
  const dnImpact = calculateDNImpact(wellsData, dnsByWell);
  const formationSummary = getFormationSummary();

  globalDashboardContextCache = {
    wells: wellsData,
    dns: dnsData,
    dnsByWell,
    riskData,
    intelligence,
    recommendations,
    totalRate,
    dnImpact,
    formationSummary
  };

  return globalDashboardContextCache;
}

/* =========================
   PRODUCTION HELPERS
========================= */
function calculateTotalRate(wellsData) {
  return (Array.isArray(wellsData) ? wellsData : [])
    .map(normalizeWell)
    .filter((well) => isProducingStatus(well.production_status))
    .reduce((sum, well) => sum + toNumber(well.oil_rate_bopd, 0), 0);
}

function calculateHourlyAverage(history) {
  if (!Array.isArray(history) || history.length === 0) return 0;

  const latest = history[history.length - 1];
  return toNumber(latest?.bopd, 0);
}

function calculateDailyAverage(history) {
  if (!Array.isArray(history) || history.length === 0) return 0;

  const total = history.reduce((sum, item) => sum + toNumber(item?.bopd, 0), 0);
  return Math.round(total / history.length);
}

function calculateProductionDrop(history) {
  if (!Array.isArray(history) || history.length < 2) {
    return {
      latest: 0,
      previous: 0,
      delta: 0,
      percent: 0,
      direction: "stable"
    };
  }

  const sorted = [...history].sort(
    (a, b) => new Date(b.timestamp) - new Date(a.timestamp)
  );

  const latest = toNumber(sorted[0]?.bopd, 0);
  const previous = toNumber(sorted[1]?.bopd, 0);
  const delta = latest - previous;
  const percent = previous > 0 ? Number(((delta / previous) * 100).toFixed(1)) : 0;

  let direction = "stable";
  if (delta > 0) direction = "up";
  if (delta < 0) direction = "down";

  return {
    latest,
    previous,
    delta,
    percent,
    direction
  };
}

function calculateDNImpact(normalizedWells, latestDNsByWell) {
  const impact = calculateOperationalDNImpact(
    Array.isArray(normalizedWells) ? normalizedWells : [],
    latestDNsByWell
  );

  return {
    lost_production: toNumber(impact?.total_estimated_loss_bopd, 0),
    affected_wells: toNumber(impact?.impacted_wells_count, 0)
  };
}

function getTopAndLowWells(normalizedWells) {
  const producing = (Array.isArray(normalizedWells) ? normalizedWells : []).filter(
    (w) => normalizeProductionStatus(w.production_status) === "On Production"
  );

  const sorted = [...producing].sort(
    (a, b) => toNumber(b.oil_rate_bopd, 0) - toNumber(a.oil_rate_bopd, 0)
  );

  return {
    top: sorted.slice(0, 3),
    low: sorted.slice(-3)
  };
}

/* =========================
   DATA LOAD
========================= */
function loadData() {
  const wellsPath = path.join(__dirname, "data", "wells.csv");
  const dnLogsPath = path.join(__dirname, "data", "dn_logs.csv");
  const dnMasterPath = path.join(__dirname, "data", "dn_master.csv");
  const formationProjectsPath = path.join(__dirname, "data", "formation_projects.csv");
  const formationTasksPath = path.join(__dirname, "data", "formation_tasks.csv");

  wells = parseCSV(wellsPath);
  dnLogs = parseCSV(dnLogsPath);
  dnMaster = parseCSV(dnMasterPath);
  formationProjects = parseCSV(formationProjectsPath);
  formationTasks = parseCSV(formationTasksPath);

  invalidateDataCaches();

  console.log("Data loaded successfully");
  console.log(`Wells loaded: ${wells.length}`);
  console.log(`DN logs loaded: ${dnLogs.length}`);
console.log(`DN master loaded: ${dnMaster.length}`);
console.log(`Formation projects loaded: ${formationProjects.length}`);
console.log(`Formation tasks loaded: ${formationTasks.length}`);
}

/* =========================
   FIND WELL / INTENT
========================= */
function findWell(question) {
  const q = toSafeLower(question);
  const normalizedWells = getConsistentWells();

  const match = q.match(
    /\b(?:well[-\s]?(\d+)|((?:andr|abqq)[-\s]?\d{3,4}))\b/i
  );

  if (match) {
    const numericPart = match[1];
    const fullCode = match[2];

    if (fullCode) {
      const normalizedTarget = normalizeText(fullCode.replace(/\s+/g, "-"));

      const found = normalizedWells.find((w) => {
        return normalizeText(w.well_name) === normalizedTarget;
      });

      if (found) return found;
    }

    if (numericPart) {
      const candidates = [
        `well-${numericPart}`,
        `well ${numericPart}`,
        `well${numericPart}`,
        `andr-${numericPart}`,
        `abqq-${numericPart}`
      ];

      const found = normalizedWells.find((w) => {
        const wellName = normalizeText(w.well_name);
        return candidates.some(
          (candidate) => wellName === normalizeText(candidate)
        );
      });

      if (found) return found;
    }
  }

  const normalizedQuestion = normalizeText(q);

  const directFound = normalizedWells.find((w) => {
    const wellName = normalizeText(w.well_name);
    return (
      normalizedQuestion.includes(wellName) ||
      wellName.includes(normalizedQuestion)
    );
  });

  return directFound || null;
}

function detectIntent(question) {
  const q = toSafeLower(question);

  if (
    q.includes("status") ||
    q.includes("check") ||
    q.includes("summary") ||
    q.includes("overview")
  ) {
    return "summary";
  }

  if (
    q.includes("oil rate") ||
    q.includes("bopd") ||
    q.includes("production rate")
  ) {
    return "oil_rate";
  }

  if (
    q.includes("owner") ||
    q.includes("responsible") ||
    q.includes("who is handling") ||
    q.includes("who handles")
  ) {
    return "owner";
  }

  if (
    q.includes("progress") ||
    q.includes("completed") ||
    q.includes("%")
  ) {
    return "progress";
  }

  if (
    q.includes("priority") ||
    q.includes("high priority") ||
    q.includes("medium priority") ||
    q.includes("low priority")
  ) {
    return "priority";
  }

  if (
    q.includes("issue") ||
    q.includes("issues") ||
    q.includes("dn") ||
    q.includes("dns") ||
    q.includes("problem") ||
    q.includes("problems") ||
    q.includes("leak") ||
    q.includes("sand encroachment")
  ) {
    return "issues";
  }

  if (
    q.includes("production") ||
    q.includes("producing") ||
    q.includes("on production")
  ) {
    return "production_status";
  }

  return "unknown";
}

function detectExecutiveAskIntent(question) {
  const q = toSafeLower(question);

  if (q.includes("focus now") || q.includes("where should i focus") || q.includes("what should i focus")) {
    return "focus_now";
  }

  if (q.includes("biggest issue") || q.includes("main issue") || q.includes("biggest problem")) {
    return "biggest_issue";
  }

  if (q.includes("which field needs attention") || q.includes("field needs attention") || q.includes("which field needs the most attention")) {
    return "field_attention";
  }

  if (q.includes("highest gain") || q.includes("highest upside") || q.includes("what gives the highest gain")) {
    return "highest_gain";
  }

  return "";
}

/* =========================
   ROUTE HELPERS
========================= */
function formatIssueForAsk(dn) {
  const normalized = normalizeDN(dn);

  return {
    ...normalized,
    type: normalized.dn_type || "Unknown"
  };
}

function buildExecutiveAskResponse(question) {
  const executiveIntent = detectExecutiveAskIntent(question);
  if (!executiveIntent) return null;

  const context = getGlobalDashboardContext();
  const executiveCommand = context.intelligence?.executive_command || null;
  const summary = context.intelligence?.summary || "";
  const topAction = executiveCommand?.top_action || null;
  const fieldCommand = executiveCommand?.field_command || {};
  const mode = executiveCommand?.mode || {};

  let message = summary || "No executive summary available.";

  if (executiveIntent === "focus_now") {
    message = topAction?.title
      ? `${topAction.title} in ${topAction.field_code} should be acted on now. ${topAction.reasoning || ""}`.trim()
      : summary || "No immediate executive action identified.";
  } else if (executiveIntent === "biggest_issue") {
    const pressureField = context.intelligence?.enhancements?.executive_signals?.biggest_operational_pressure_field;
    message = pressureField
      ? `${pressureField} is carrying the biggest operational pressure. ${mode.explanation || summary}`.trim()
      : summary || "No dominant issue identified.";
  } else if (executiveIntent === "field_attention") {
    message = fieldCommand?.immediate_action_field
      ? `${fieldCommand.immediate_action_field} needs the most executive attention right now, while ${fieldCommand.monitoring_field || "the other field"} can be monitored more closely.`
      : summary || "No field attention signal identified.";
  } else if (executiveIntent === "highest_gain") {
    const gainField = fieldCommand?.highest_gain_opportunity_field;
    const gainBopd = context.intelligence?.enhancements?.gain_vs_loss?.formation_gain_bopd;
    message = gainField
      ? `${gainField} carries the strongest gain opportunity, with about ${toNumber(gainBopd, 0)} BOPD of current formation upside.`
      : summary || "No gain opportunity identified.";
  }

  return {
    message,
    summary,
    executive_command: executiveCommand,
    field_command: fieldCommand,
    mode
  };
}

/* =========================
   MAIN ROUTE
========================= */
app.post("/ask", (req, res) => {
  try {
    const question = toSafeString(req.body?.question);

    if (!question) {
      return res.status(400).json({ error: "Question is required" });
    }

    const executiveResponse = buildExecutiveAskResponse(question);
    if (executiveResponse) {
      return res.json(executiveResponse);
    }

    const normalizedWell = findWell(question);

    if (!normalizedWell) {
      return res.json({ message: "Well not found" });
    }

    const intent = detectIntent(question);
    const latestDNs = getConsistentDNsForWell(normalizedWell.well_id || normalizedWell.id);
    const normalizedIssues = latestDNs.map(formatIssueForAsk);

    if (intent === "summary") {
      return res.json({
        well: normalizedWell.well_name,
        field_code: normalizedWell.field_code,
        production_status: normalizedWell.production_status,
        oil_rate: normalizedWell.oil_rate_bopd,
        issue_count: normalizedIssues.length,
        issues: normalizedIssues
      });
    }

    if (intent === "production_status") {
      return res.json({
        well: normalizedWell.well_name,
        field_code: normalizedWell.field_code,
        production_status: normalizedWell.production_status
      });
    }

    if (intent === "oil_rate") {
      return res.json({
        well: normalizedWell.well_name,
        field_code: normalizedWell.field_code,
        oil_rate: normalizedWell.oil_rate_bopd
      });
    }

    if (intent === "issues") {
      return res.json({
        well: normalizedWell.well_name,
        field_code: normalizedWell.field_code,
        issue_count: normalizedIssues.length,
        issues: normalizedIssues
      });
    }

    if (intent === "owner") {
      return res.json({
        well: normalizedWell.well_name,
        field_code: normalizedWell.field_code,
        issue_count: normalizedIssues.length,
        issues: normalizedIssues.map((item) => ({
          dn_id: item.dn_id,
          type: item.type,
          owner: item.current_owner_name,
          workflow_status: item.workflow_status,
          current_step: item.current_step,
          progress: item.progress,
          priority: item.priority,
          created_date: item.created_date,
          last_updated: item.last_updated
        }))
      });
    }

    if (intent === "progress") {
      return res.json({
        well: normalizedWell.well_name,
        field_code: normalizedWell.field_code,
        issue_count: normalizedIssues.length,
        issues: normalizedIssues.map((item) => ({
          dn_id: item.dn_id,
          type: item.type,
          progress: item.progress,
          progress_percent: item.progress_percent,
          workflow_status: item.workflow_status,
          current_step: item.current_step,
          owner: item.current_owner_name,
          priority: item.priority,
          created_date: item.created_date,
          last_updated: item.last_updated
        }))
      });
    }

    if (intent === "priority") {
      return res.json({
        well: normalizedWell.well_name,
        field_code: normalizedWell.field_code,
        issue_count: normalizedIssues.length,
        issues: normalizedIssues.map((item) => ({
          dn_id: item.dn_id,
          type: item.type,
          priority: item.priority,
          workflow_status: item.workflow_status,
          current_step: item.current_step,
          owner: item.current_owner_name,
          progress: item.progress,
          created_date: item.created_date,
          last_updated: item.last_updated
        }))
      });
    }

    return res.json({
      well: normalizedWell.well_name,
      field_code: normalizedWell.field_code,
      production_status: normalizedWell.production_status,
      oil_rate: normalizedWell.oil_rate_bopd,
      issue_count: normalizedIssues.length,
      issues: normalizedIssues
    });
  } catch (error) {
    console.error("Error in /ask:", error);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================
   DASHBOARD SUMMARY
========================= */
app.get("/dashboard/summary", (req, res) => {
  try {
    const context = getGlobalDashboardContext();
    const totalRate = context.totalRate;
    const hourlyAverage = calculateHourlyAverage(productionHistory);
    const dailyAverage = calculateDailyAverage(productionHistory);

    return res.json({
      total_rate: totalRate,
      hourly_average: hourlyAverage,
      daily_average: dailyAverage,
      unit: "BOPD"
    });
  } catch (error) {
    console.error("Error in /dashboard/summary:", error.message);
    return res.status(500).json({ error: "Failed to load dashboard summary" });
  }
});

/* =========================
   DASHBOARD OVERVIEW
========================= */
app.get("/dashboard/overview", (req, res) => {
  try {
    const context = getGlobalDashboardContext();
    const normalizedWells = context.wells;
    const latestDNs = context.dns;
    const latestDNsByWell = context.dnsByWell;

    const totalRate = context.totalRate;
    const hourlyAverage = calculateHourlyAverage(productionHistory);
    const dailyAverage = calculateDailyAverage(productionHistory);

    const monthlyTarget = Number(process.env.MONTHLY_TARGET) || 25000;
    const targetGap = totalRate - monthlyTarget;

    const activeDNCount = latestDNs.filter((dn) => !dn.is_closed).length;

    const statusCounts = {
      on_production: 0,
      testing: 0,
      standby: 0,
      mothball: 0,
      shut_in: 0,
      locked_potential: 0
    };

    normalizedWells.forEach((w) => {
      const rawStatus = normalizeProductionStatus(w.production_status);

      if (rawStatus === "Locked Potential") {
        statusCounts.locked_potential += 1;
      } else if (rawStatus === "On Production") {
        statusCounts.on_production += 1;
      } else if (rawStatus === "Testing") {
        statusCounts.testing += 1;
      } else if (rawStatus === "Standby") {
        statusCounts.standby += 1;
      } else if (rawStatus === "Mothball") {
        statusCounts.mothball += 1;
      } else if (rawStatus === "Shut-in") {
        statusCounts.shut_in += 1;
      }
    });

    let ainDarRate = 0;
    let abqaiqRate = 0;

    normalizedWells.forEach((w) => {
      const rate = toNumber(w.oil_rate_bopd, 0);
      const field = normalizeFieldCode(w.field_code, w.well_name, w.field);
      const includeInProduction = isProducingStatus(w.production_status);

      if (!includeInProduction) return;

      if (field === "ANDR") {
        ainDarRate += rate;
      } else if (field === "ABQQ") {
        abqaiqRate += rate;
      }
    });

    const totalFieldRate = ainDarRate + abqaiqRate;

    const ainDarPercent =
      totalFieldRate > 0
        ? Number(((ainDarRate / totalFieldRate) * 100).toFixed(1))
        : 0;

    const abqaiqPercent =
      totalFieldRate > 0
        ? Number(((abqaiqRate / totalFieldRate) * 100).toFixed(1))
        : 0;

    const insights = [];

    if (targetGap < 0) {
      insights.push(`Production is below target by ${Math.abs(targetGap)} BOPD.`);
    } else if (targetGap > 0) {
      insights.push(`Production is above target by ${targetGap} BOPD.`);
    } else {
      insights.push("Production is exactly on target.");
    }

    if (ainDarPercent > abqaiqPercent) {
      insights.push("Ain Dar field is the main contributor.");
    } else if (abqaiqPercent > ainDarPercent) {
      insights.push("Abqaiq field is the main contributor.");
    } else {
      insights.push("Field contributions are balanced.");
    }

    if (statusCounts.locked_potential > 0) {
      insights.push(`${statusCounts.locked_potential} wells have locked potential.`);
    }

    const productionTrend = calculateProductionDrop(productionHistory);
    const dnImpact = context.dnImpact;
    const performance = getTopAndLowWells(normalizedWells);

    return res.json({
      kpis: {
        total_rate: totalRate,
        hourly_average: hourlyAverage,
        daily_average: dailyAverage,
        monthly_target: monthlyTarget,
        target_gap: targetGap,
        active_dn_count: activeDNCount
      },
      well_status: {
        on_production: statusCounts.on_production,
        testing: statusCounts.testing,
        standby: statusCounts.standby,
        mothball: statusCounts.mothball,
        shut_in: statusCounts.shut_in,
        locked_potential: statusCounts.locked_potential
      },
      field_contribution: {
        ain_dar_rate: ainDarRate,
        abqaiq_rate: abqaiqRate,
        ain_dar_percent: ainDarPercent,
        abqaiq_percent: abqaiqPercent
      },
      insights,
      production_trend: productionTrend,
      dn_impact: dnImpact,
      performance
    });
  } catch (error) {
    console.error("Error in /dashboard/overview:", error);
    return res.status(500).json({ error: "Failed to load dashboard overview" });
  }
});

/* =========================
   DASHBOARD RISK
========================= */
app.get("/dashboard/risk", (req, res) => {
  try {
    if (!coreEngine) {
      const fallbackRisk = buildRiskDashboard(getConsistentWells(), getConsistentDNs());
      return res.json({
        top_risk_wells: Array.isArray(fallbackRisk?.top_risk_wells)
          ? fallbackRisk.top_risk_wells.slice(0, 10)
          : [],
        top_risk_dns: Array.isArray(fallbackRisk?.top_risk_dns)
          ? fallbackRisk.top_risk_dns.slice(0, 10)
          : []
      });
    }

    const topRiskWells = [];

    if (coreEngine.wellIndex instanceof Map) {
      for (const [wellId, well] of coreEngine.wellIndex.entries()) {
        const dns =
          typeof coreEngine.getDNsForWell === "function"
            ? coreEngine.getDNsForWell(wellId)
            : [];
        const risk = calculateWellRisk(well, dns);

        topRiskWells.push({
          well_id: well.well_id,
          well_name: well.well_name,
          field_code: well.field_code,
          production_status: well.production_status,
          oil_rate_bopd: well.oil_rate_bopd,
          risk_score: risk.score,
          risk_level: risk.level,
          reasons: risk.reasons
        });
      }
    }

    topRiskWells.sort((a, b) => b.risk_score - a.risk_score);

    const topRiskDNs = [];
    const dnMap = getCoreEngineDNMap(coreEngine);

    if (dnMap instanceof Map) {
      for (const [, dn] of dnMap.entries()) {
        const risk = calculateDNRisk(dn);

        topRiskDNs.push({
          dn_id: dn.dn_id,
          well_id: dn.well_id,
          dn_type: dn.dn_type,
          priority: dn.priority,
          workflow_status: dn.workflow_status,
          current_step: dn.current_step,
          risk_score: risk.score,
          risk_level: risk.level,
          reasons: risk.reasons
        });
      }
    }

    topRiskDNs.sort((a, b) => b.risk_score - a.risk_score);

    return res.json({
      top_risk_wells: topRiskWells.slice(0, 10),
      top_risk_dns: topRiskDNs.slice(0, 10)
    });
  } catch (error) {
    console.error("Error in /dashboard/risk:", error);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================
   DASHBOARD INTELLIGENCE
========================= */
app.get("/dashboard/intelligence", (req, res) => {
  try {
    const context = getGlobalDashboardContext();
    const normalizedWells = context.wells;
    const latestDNsByWell = context.dnsByWell;
    const executionData = buildExecutionPlan(normalizedWells, latestDNsByWell, { topN: 20 });

    return res.json({
  success: true,

  intelligence: context.intelligence,

  execution: {
    summary: executionData.summary,

    fields: {
      ANDR: executionData.by_field?.ANDR || [],
      ABQQ: executionData.by_field?.ABQQ || []
    },

    top_actions: executionData.execution_plan || []
  }
});
  } catch (error) {
    console.error("Error in /dashboard/intelligence:", error);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================
   DASHBOARD SYSTEM
========================= */
app.get("/dashboard/system", (req, res) => {
  try {
    const context = getGlobalDashboardContext();
    const normalizedWells = context.wells;
    const latestDNs = context.dns;
    const latestDNsByWell = context.dnsByWell;

    const overviewSection = dashboardBuilders.buildOverviewSection(
      normalizedWells,
      latestDNs,
      productionHistory,
      process.env.MONTHLY_TARGET
    );

    const productionSection = dashboardBuilders.buildProductionSection(
      normalizedWells,
      latestDNs,
      productionHistory
    );

    const dnSection = dashboardBuilders.buildDNSection(
      latestDNs,
      normalizedWells,
      latestDNsByWell
    );

    let riskSection = dashboardBuilders.buildRiskSection(null);

    if (coreEngine) {
      try {
        riskSection = dashboardBuilders.buildRiskSectionFromEngine(coreEngine);
      } catch (riskError) {
        console.error("Risk section build failed:", riskError.message);
      }
    }

    const intelligenceSection = dashboardBuilders.buildIntelligenceSection(
      context.intelligence
    );

    return res.json({
      generated_at: new Date().toISOString(),
      overview: overviewSection,
      production: productionSection,
      dn: dnSection,
      risk: riskSection,
      intelligence: intelligenceSection,
      formation: context.formationSummary
    });
  } catch (error) {
    console.error("Error in /dashboard/system:", error);
    return res.status(500).json({ error: "Failed to load dashboard system" });
  }
});

/* =========================
   DASHBOARD OPERATIONS
========================= */
app.get("/dashboard/operations", (req, res) => {
  try {
    const normalizedWells = getConsistentWells();
    const latestDNs = getConsistentDNs();
    const latestDNsByWell = getConsistentDNsByWellMap();

    const statusCounts = {
      on_production: 0,
      testing: 0,
      standby: 0,
      mothball: 0,
      shut_in: 0,
      locked_potential: 0
    };

    normalizedWells.forEach((w) => {
      const rawStatus = normalizeProductionStatus(w.production_status);

      if (rawStatus === "Locked Potential") {
        statusCounts.locked_potential += 1;
      } else if (rawStatus === "On Production") {
        statusCounts.on_production += 1;
      } else if (rawStatus === "Testing") {
        statusCounts.testing += 1;
      } else if (rawStatus === "Standby") {
        statusCounts.standby += 1;
      } else if (rawStatus === "Mothball") {
        statusCounts.mothball += 1;
      } else if (rawStatus === "Shut-in") {
        statusCounts.shut_in += 1;
      }
    });

    const activeDNCount = latestDNs.filter((dn) => !dn.is_closed).length;
    const closedDNCount = latestDNs.filter((dn) => dn.is_closed).length;

    const dnByStatus = {
      open: 0,
      in_progress: 0,
      completed: 0,
      closed: 0,
      waiting: 0
    };

    latestDNs.forEach((dn) => {
      const ws = toSafeLower(dn.workflow_status || "");
      if (ws === "closed") dnByStatus.closed += 1;
      else if (ws === "completed") dnByStatus.completed += 1;
      else if (ws === "in progress") dnByStatus.in_progress += 1;
      else if (ws === "waiting") dnByStatus.waiting += 1;
      else dnByStatus.open += 1;
    });

    const dnByOwner = {};
    latestDNs.forEach((dn) => {
      const owner = toSafeString(dn.current_owner_name || "Unassigned");
      if (!dnByOwner[owner]) {
        dnByOwner[owner] = 0;
      }
      dnByOwner[owner] += 1;
    });

    const ownerList = Object.entries(dnByOwner)
      .map(([name, count]) => ({ name, dn_count: count }))
      .sort((a, b) => b.dn_count - a.dn_count);

    const dnImpact = calculateDNImpact(normalizedWells, latestDNsByWell);

    const totalWells = normalizedWells.length;
    const producingWellsPercent = Math.round(
      totalWells > 0 ? (statusCounts.on_production / totalWells) * 100 : 0
    ) || 0;

    const bottlenecks = [];

    if (statusCounts.locked_potential > 0) {
      bottlenecks.push({
        type: "locked_potential",
        count: statusCounts.locked_potential,
        message: `${statusCounts.locked_potential} wells with locked production potential`
      });
    }

    if (dnImpact.affected_wells > 0) {
      bottlenecks.push({
        type: "dn_affected",
        count: dnImpact.affected_wells,
        lost_production: dnImpact.lost_production,
        message: `${dnImpact.affected_wells} wells affected by active DNs, ${dnImpact.lost_production} BOPD lost`
      });
    }

    const shutInPercent = Math.round(
      totalWells > 0 ? (statusCounts.shut_in / totalWells) * 100 : 0
    ) || 0;

    if (shutInPercent > 15) {
      bottlenecks.push({
        type: "high_shut_in",
        percentage: shutInPercent,
        count: statusCounts.shut_in,
        message: `${shutInPercent}% of wells shut-in (${statusCounts.shut_in} wells)`
      });
    }

    return res.json({
      generated_at: new Date().toISOString(),
      well_status: statusCounts,
      well_metrics: {
        total_wells: totalWells,
        producing_percent: producingWellsPercent,
        shut_in_percent: shutInPercent
      },
      dn_operational_load: {
        active_dn_count: activeDNCount,
        closed_dn_count: closedDNCount,
        total_dn_count: latestDNs.length,
        dn_by_status: dnByStatus
      },
      dn_ownership: {
        assigned_owners: ownerList.length,
        top_owners: ownerList.slice(0, 5)
      },
      operational_alerts: {
        bottleneck_count: bottlenecks.length,
        bottlenecks
      },
      production_impact: dnImpact
    });
  } catch (error) {
    console.error("Error in /dashboard/operations:", error);
    return res.status(500).json({ error: "Failed to load operations dashboard" });
  }
});

/* =========================
   DASHBOARD RECOMMENDATIONS
========================= */
app.get("/dashboard/recommendations", (req, res) => {
  try {
    return res.json(getGlobalDashboardContext().recommendations);
  } catch (error) {
    console.error("Error in /dashboard/recommendations:", error);
    return res.status(500).json({ error: "Failed to generate recommendations" });
  }
});

/* =========================
   DASHBOARD EXECUTIVE
========================= */
app.get("/dashboard/executive", (req, res) => {
  try {
    const context = getGlobalDashboardContext();
    const normalizedWells = getConsistentWells();
    const latestDNs = getConsistentDNs();

    const executiveDashboard = dashboardBuilders.buildExecutiveDashboard({
      wells: normalizedWells,
      dns: latestDNs,
      formationProjects,
      formationTasks,
      productionHistory,
      monthlyTarget: process.env.MONTHLY_TARGET,
      coreEngine
    });

    return res.json({
      ...executiveDashboard,
      summary: context.intelligence?.summary || executiveDashboard.summary || "",
      executive_command:
        context.intelligence?.executive_command || executiveDashboard.executive_command || null
    });
  } catch (error) {
    console.error("Error in /dashboard/executive:", error);
    return res.status(500).json({ error: "Failed to load executive dashboard" });
  }
});

/* =========================
   DASHBOARD FIELD
========================= */
app.get("/dashboard/field/:field_code", (req, res) => {
  try {
    const fieldCode = toSafeString(req.params.field_code).toUpperCase();
    const normalizedWells = getConsistentWells();
    const latestDNs = getConsistentDNs();

    const fieldDashboard = dashboardBuilders.buildFieldDashboard(fieldCode, {
      wells: normalizedWells,
      dns: latestDNs,
      formationProjects,
      formationTasks,
      productionHistory,
      monthlyTarget: process.env.MONTHLY_TARGET,
      coreEngine
    });

    if (!fieldDashboard) {
      return res.status(404).json({ error: "Field not found" });
    }

    return res.json(fieldDashboard);
  } catch (error) {
    console.error(`Error in /dashboard/well/${req.params.well_id}:`, error);
    return res.status(500).json({ error: "Failed to load field dashboard" });
  }
});

/* =========================
   DASHBOARD WELL
========================= */
app.get("/dashboard/well/:well_id", (req, res) => {
  try {
    const wellId = toSafeString(req.params.well_id);
    const normalizedWells = getConsistentWells();
    const latestDNs = getConsistentDNs();

    const wellDashboard = dashboardBuilders.buildWellDashboard(wellId, {
      wells: normalizedWells,
      dns: latestDNs,
      formationProjects,
      formationTasks,
      productionHistory,
      monthlyTarget: process.env.MONTHLY_TARGET,
      coreEngine
    });

    if (!wellDashboard) {
      return res.status(404).json({ error: "Well not found" });
    }

    return res.json(wellDashboard);
  } catch (error) {
    console.error(`Error in /dashboard/well/${req.params.well_id}:`, error);
    return res.status(500).json({ error: "Failed to load well dashboard" });
  }
});

app.get("/dashboard/formation", (_req, res) => {
  try {
    return res.json(getFormationSummary());
  } catch (error) {
    console.error("Error in /dashboard/formation:", error);
    return res.status(500).json({ error: "Failed to load formation dashboard" });
  }
});

/* =========================
   START
========================= */
function initializeCoreEngineData() {
  try {
    coreEngine = CoreEngine.initializeCoreEngine(wells, dnLogs, dnMaster);

    if (!coreEngine) {
      console.warn("[CoreEngine] Initialization returned null, advanced features disabled");
    }
  } catch (error) {
    console.error("[CoreEngine] Failed to initialize:", error);
  }
}

loadData();
initializeCoreEngineData();

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
