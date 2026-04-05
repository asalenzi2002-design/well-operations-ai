"use strict";

function toSafeString(value) {
  return String(value ?? "").trim();
}

function toSafeLower(value) {
  return toSafeString(value).toLowerCase();
}

function toNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function getArray(value) {
  return Array.isArray(value) ? value : [];
}

function uniqueStrings(values = []) {
  return Array.from(
    new Set(
      getArray(values)
        .map((value) => toSafeString(value))
        .filter(Boolean)
    )
  );
}

const WORKSTREAM_WEIGHTS = {
  ON_PLOT: 35,
  OFF_PLOT: 35,
  TIE_IN_PREP: 20,
  PERMITS_SECURITY: 10
};

function normalizeProject(project) {
  const facility = toSafeString(project?.facility);
  const projectType = toSafeString(project?.project_type);
  const location = toSafeLower(project?.location) === "off_plot"
    ? "off_plot"
    : facility.toUpperCase().includes("NORTH") || projectType.toLowerCase().includes("loop")
      ? "off_plot"
      : "on_plot";

  return {
    ...project,
    project_id: toSafeString(project?.project_id),
    project_name: toSafeString(project?.project_name),
    field_code: toSafeString(project?.field_code).toUpperCase(),
    location,
    facility: toSafeString(project?.facility),
    project_type: toSafeString(project?.project_type),
    status: toSafeString(project?.status),
    workflow_status: toSafeString(project?.workflow_status || project?.status),
    overall_progress_percent: toNumber(project?.overall_progress_percent ?? project?.progress_percent, 0),
    progress_percent: toNumber(project?.overall_progress_percent ?? project?.progress_percent, 0),
    readiness_state: toSafeString(project?.readiness_state),
    estimated_gain_bopd: toNumber(project?.estimated_gain_bopd ?? project?.expected_gain_bopd, 0),
    expected_gain_bopd: toNumber(project?.estimated_gain_bopd ?? project?.expected_gain_bopd, 0),
    priority: toSafeString(project?.priority),
    owner: toSafeString(project?.owner),
    start_date: toSafeString(project?.start_date),
    target_date: toSafeString(project?.target_date),
    last_update: toSafeString(project?.last_update)
  };
}

function normalizeTask(task) {
  return {
    ...task,
    task_id: toSafeString(task?.task_id),
    project_id: toSafeString(task?.project_id),
    workstream: toSafeString(task?.workstream).toUpperCase(),
    task_name: toSafeString(task?.task_name),
    task_category: toSafeString(task?.task_category),
    status: toSafeString(task?.status),
    progress_percent: toNumber(task?.progress_percent, 0),
    remaining_count: toNumber(task?.remaining_count, 0),
    dependency_type: toSafeString(task?.dependency_type),
    blocker_reason: toSafeString(task?.blocker_reason),
    owner: toSafeString(task?.owner),
    last_update: toSafeString(task?.last_update)
  };
}

function averageProgress(tasks) {
  const safeTasks = getArray(tasks);
  if (safeTasks.length === 0) return 0;
  const total = safeTasks.reduce((sum, task) => sum + toNumber(task?.progress_percent, 0), 0);
  return Math.round(total / safeTasks.length);
}

function buildFormationProjectIndex(projects = [], tasks = []) {
  const projectIndex = new Map();
  const tasksByProject = new Map();

  getArray(projects).map(normalizeProject).forEach((project) => {
    if (!project.project_id) return;
    projectIndex.set(project.project_id, project);
  });

  getArray(tasks).map(normalizeTask).forEach((task) => {
    if (!task.project_id) return;
    if (!tasksByProject.has(task.project_id)) {
      tasksByProject.set(task.project_id, []);
    }
    tasksByProject.get(task.project_id).push(task);
  });

  return { projectIndex, tasksByProject };
}

function calculateFormationProjectProgress(project = {}, tasks = []) {
  const progressByWorkstream = {
    ON_PLOT: averageProgress(getArray(tasks).filter((task) => task.workstream === "ON_PLOT")),
    OFF_PLOT: averageProgress(getArray(tasks).filter((task) => task.workstream === "OFF_PLOT")),
    TIE_IN_PREP: averageProgress(getArray(tasks).filter((task) => task.workstream === "TIE_IN_PREP")),
    PERMITS_SECURITY: averageProgress(getArray(tasks).filter((task) => task.workstream === "PERMITS_SECURITY"))
  };

  const weightedProgress = Math.round(
    Object.entries(WORKSTREAM_WEIGHTS).reduce((sum, [workstream, weight]) => {
      return sum + ((progressByWorkstream[workstream] || 0) * weight) / 100;
    }, 0)
  );

  return {
    overall_progress_percent: weightedProgress,
    workstreams: progressByWorkstream
  };
}

function calculateFormationReadiness(project = {}, tasks = [], progress = null) {
  const safeTasks = getArray(tasks);
  const computedProgress = progress || calculateFormationProjectProgress(project, safeTasks);
  const permitTasks = safeTasks.filter((task) => task.workstream === "PERMITS_SECURITY");
  const tieInTasks = safeTasks.filter((task) => task.workstream === "TIE_IN_PREP");
  const permitBlocked = permitTasks.some(
    (task) => task.blocker_reason && toSafeLower(task.dependency_type).includes("permit")
  );
  const executionBlocked = safeTasks.some(
    (task) =>
      task.blocker_reason &&
      !toSafeLower(task.dependency_type).includes("permit") &&
      toSafeLower(task.status) !== "completed"
  );

  if (toSafeLower(project?.status) === "completed" || computedProgress.overall_progress_percent >= 100) {
    return "completed";
  }

  if (permitBlocked) return "permit_blocked";
  if (executionBlocked) return "execution_blocked";

  const onPlotReady = (computedProgress.workstreams.ON_PLOT || 0) >= 80;
  const offPlotReady = (computedProgress.workstreams.OFF_PLOT || 0) >= 80;
  const tieInReady = tieInTasks.length > 0 && tieInTasks.every((task) => task.progress_percent >= 90);
  const permitsReady = permitTasks.length === 0 || permitTasks.every((task) => task.progress_percent >= 90);
  const anyConstruction = (computedProgress.workstreams.ON_PLOT || 0) > 0 || (computedProgress.workstreams.OFF_PLOT || 0) > 0;
  const anyProgress = computedProgress.overall_progress_percent > 0;

  if (onPlotReady && offPlotReady && tieInReady && permitsReady) return "tie_in_ready";
  if (anyConstruction) return "construction_in_progress";
  if (anyProgress && (computedProgress.workstreams.TIE_IN_PREP > 0 || computedProgress.workstreams.PERMITS_SECURITY > 0)) {
    return "partial_ready";
  }
  if (permitsReady && computedProgress.workstreams.TIE_IN_PREP >= 20) return "engineering_ready";
  if (anyProgress) return "partial_ready";

  return "not_started";
}

function buildProjectRecord(project, tasks) {
  const normalizedProject = normalizeProject(project);
  const normalizedTasks = getArray(tasks).map(normalizeTask);
  const progress = calculateFormationProjectProgress(normalizedProject, normalizedTasks);
  const readiness = calculateFormationReadiness(normalizedProject, normalizedTasks, progress);
  const blockers = normalizedTasks.filter((task) => task.blocker_reason).map((task) => ({
    task_id: task.task_id,
    workstream: task.workstream,
    blocker_reason: task.blocker_reason,
    dependency_type: task.dependency_type,
    owner: task.owner
  }));
  const dependencies = uniqueStrings(
    normalizedTasks.map((task) => task.dependency_type).filter((type) => type && toSafeLower(type) !== "none")
  );
  const blockingItems = blockers.map((blocker) => ({
    task_id: blocker.task_id,
    workstream: blocker.workstream,
    reason: blocker.blocker_reason,
    dependency_type: blocker.dependency_type,
    owner: blocker.owner
  }));

  return {
    ...normalizedProject,
    workflow_status: normalizedProject.workflow_status || normalizedProject.status || readiness,
    overall_progress_percent: progress.overall_progress_percent,
    progress_percent: progress.overall_progress_percent,
    readiness_state: readiness,
    dependencies,
    blocking_items: blockingItems,
    workstreams: progress.workstreams,
    blockers,
    tasks: normalizedTasks
  };
}

function calculateFormationFieldSummary(projects = [], tasks = [], fieldCode = "") {
  const targetField = toSafeString(fieldCode).toUpperCase();
  const index = buildFormationProjectIndex(projects, tasks);
  const records = Array.from(index.projectIndex.values())
    .filter((project) => !targetField || project.field_code === targetField)
    .map((project) => buildProjectRecord(project, index.tasksByProject.get(project.project_id) || []));

  const activeProjects = records.filter((project) => project.readiness_state !== "completed");
  const tieInReadyProjects = activeProjects.filter((project) => project.readiness_state === "tie_in_ready");
  const permitBlockedProjects = activeProjects.filter((project) => project.readiness_state === "permit_blocked");
  const blockedProjects = activeProjects.filter((project) =>
    project.readiness_state === "permit_blocked" || project.readiness_state === "execution_blocked"
  );
  const constructionProjects = activeProjects.filter((project) => project.readiness_state === "construction_in_progress");
  const topProject = [...activeProjects].sort(
    (a, b) => b.expected_gain_bopd - a.expected_gain_bopd || b.overall_progress_percent - a.overall_progress_percent
  )[0] || null;

  return {
    summary: activeProjects.length
      ? `${activeProjects.length} active formation project(s) with ${activeProjects.reduce((sum, item) => sum + item.estimated_gain_bopd, 0)} BOPD upside.`
      : "No active formation projects.",
    active_projects: activeProjects.length,
    tie_in_ready_count: tieInReadyProjects.length,
    blocked_projects_count: blockedProjects.length,
    construction_in_progress_count: constructionProjects.length,
    permit_blocked_count: permitBlockedProjects.length,
    expected_gain_bopd: activeProjects.reduce((sum, item) => sum + item.estimated_gain_bopd, 0),
    top_project: topProject
      ? {
          project_id: topProject.project_id,
          project_name: topProject.project_name,
          field_code: topProject.field_code,
          location: topProject.location,
          workflow_status: topProject.workflow_status,
          estimated_gain_bopd: topProject.estimated_gain_bopd,
          overall_progress_percent: topProject.overall_progress_percent,
          readiness_state: topProject.readiness_state
        }
      : null,
    top_risks: blockedProjects.slice(0, 3).map((project) => ({
      project_id: project.project_id,
      project_name: project.project_name,
      readiness_state: project.readiness_state,
      blocker_reason: project.blockers[0]?.blocker_reason || "",
      estimated_gain_bopd: project.estimated_gain_bopd
    })),
    opportunities: tieInReadyProjects.slice(0, 3).map((project) => ({
      project_id: project.project_id,
      project_name: project.project_name,
      estimated_gain_bopd: project.estimated_gain_bopd,
      readiness_state: project.readiness_state
    })),
    projects: activeProjects.map((project) => ({
      project_id: project.project_id,
      project_name: project.project_name,
      field_code: project.field_code,
      location: project.location,
      facility: project.facility,
      status: project.status,
      workflow_status: project.workflow_status,
      overall_progress_percent: project.overall_progress_percent,
      progress_percent: project.progress_percent,
      readiness_state: project.readiness_state,
      estimated_gain_bopd: project.estimated_gain_bopd,
      priority: project.priority,
      owner: project.owner,
      target_date: project.target_date,
      last_update: project.last_update,
      dependencies: project.dependencies,
      blocking_items: project.blocking_items,
      blockers: project.blockers,
      workstreams: project.workstreams
    }))
  };
}

function buildFormationOperationalSummary(projects = [], tasks = []) {
  const global = calculateFormationFieldSummary(projects, tasks);
  const andr = calculateFormationFieldSummary(projects, tasks, "ANDR");
  const abqq = calculateFormationFieldSummary(projects, tasks, "ABQQ");
  const allProjects = global.projects || [];

  const topBlocked = [...allProjects]
    .filter((project) => getArray(project.blocking_items).length > 0)
    .sort((a, b) => b.estimated_gain_bopd - a.estimated_gain_bopd)
    .slice(0, 5);

  const topGains = [...allProjects]
    .sort((a, b) => b.estimated_gain_bopd - a.estimated_gain_bopd || b.progress_percent - a.progress_percent)
    .slice(0, 5);

  const moreProgressField =
    (andr.active_projects ? andr.projects.reduce((sum, item) => sum + item.progress_percent, 0) / Math.max(andr.active_projects, 1) : 0) >=
    (abqq.active_projects ? abqq.projects.reduce((sum, item) => sum + item.progress_percent, 0) / Math.max(abqq.active_projects, 1) : 0)
      ? "ANDR"
      : "ABQQ";
  const topGainField = andr.expected_gain_bopd >= abqq.expected_gain_bopd ? "ANDR" : "ABQQ";

  return {
    summary: global.summary,
    global: {
      active_projects: global.active_projects,
      blocked_projects_count: global.blocked_projects_count,
      tie_in_ready_count: global.tie_in_ready_count,
      expected_gain_bopd: global.expected_gain_bopd,
      projects: global.projects
    },
    by_field: {
      ANDR: andr,
      ABQQ: abqq
    },
    intelligence: {
      field_with_more_progress: moreProgressField,
      field_with_more_gain: topGainField,
      blocked_projects: topBlocked.map((project) => ({
        project_id: project.project_id,
        project_name: project.project_name,
        field_code: project.field_code,
        readiness_state: project.readiness_state,
        blocking_items: project.blocking_items,
        estimated_gain_bopd: project.estimated_gain_bopd
      })),
      top_gain_projects: topGains.map((project) => ({
        project_id: project.project_id,
        project_name: project.project_name,
        field_code: project.field_code,
        readiness_state: project.readiness_state,
        progress_percent: project.progress_percent,
        estimated_gain_bopd: project.estimated_gain_bopd
      })),
      insight:
        global.active_projects > 0
          ? `${topGainField} carries the larger formation-line gain opportunity while ${moreProgressField} is further ahead on execution progress.`
          : "No active formation-line projects."
    }
  };
}

function buildFormationExecutiveActions(projects = [], tasks = [], fieldCode = "") {
  const summary = calculateFormationFieldSummary(projects, tasks, fieldCode);
  const targetField = toSafeString(fieldCode).toUpperCase();
  const actions = [];

  summary.projects.forEach((project) => {
    if (project.readiness_state === "permit_blocked") {
      actions.push({
        field: targetField || project.field_code,
        dn_id: project.project_id,
        project_id: project.project_id,
        project_name: project.project_name,
        well_id: "",
        action: `Clear permit blocker for ${project.project_name}${project.blockers[0]?.blocker_reason ? `: ${project.blockers[0].blocker_reason}` : ""}.`,
        impact_bopd: project.expected_gain_bopd,
        priority: "high",
        source: "formation_line",
        readiness_state: project.readiness_state,
        progress_percent: project.progress_percent,
        dependencies: project.dependencies,
        blocking_items: project.blocking_items,
        owner: project.owner,
        workflow_status: project.workflow_status
      });
    } else if (project.readiness_state === "execution_blocked") {
      actions.push({
        field: targetField || project.field_code,
        dn_id: project.project_id,
        project_id: project.project_id,
        project_name: project.project_name,
        well_id: "",
        action: `Remove execution blocker and recover construction momentum on ${project.project_name}.`,
        impact_bopd: project.expected_gain_bopd,
        priority: "high",
        source: "formation_line",
        readiness_state: project.readiness_state,
        progress_percent: project.progress_percent,
        dependencies: project.dependencies,
        blocking_items: project.blocking_items,
        owner: project.owner,
        workflow_status: project.workflow_status
      });
    } else if (project.readiness_state === "tie_in_ready") {
      actions.push({
        field: targetField || project.field_code,
        dn_id: project.project_id,
        project_id: project.project_id,
        project_name: project.project_name,
        well_id: "",
        action: `Secure tie-in window and execute ${project.project_name}.`,
        impact_bopd: project.expected_gain_bopd,
        priority: "critical",
        source: "formation_line",
        readiness_state: project.readiness_state,
        progress_percent: project.progress_percent,
        dependencies: project.dependencies,
        blocking_items: project.blocking_items,
        owner: project.owner,
        workflow_status: project.workflow_status
      });
    } else if (project.readiness_state === "construction_in_progress" && project.overall_progress_percent >= 50) {
      actions.push({
        field: targetField || project.field_code,
        dn_id: project.project_id,
        project_id: project.project_id,
        project_name: project.project_name,
        well_id: "",
        action: `Accelerate construction closeout on ${project.project_name} to pull forward production gain.`,
        impact_bopd: project.expected_gain_bopd,
        priority: "medium",
        source: "formation_line",
        readiness_state: project.readiness_state,
        progress_percent: project.progress_percent,
        dependencies: project.dependencies,
        blocking_items: project.blocking_items,
        owner: project.owner,
        workflow_status: project.workflow_status
      });
    }
  });

  return actions.sort((a, b) => {
    const order = { critical: 0, high: 1, medium: 2, low: 3 };
    const priorityDiff = (order[a.priority] ?? 4) - (order[b.priority] ?? 4);
    if (priorityDiff !== 0) return priorityDiff;
    return b.impact_bopd - a.impact_bopd;
  });
}

function buildFormationIntelligence(projects = [], tasks = [], fieldCode = "") {
  if (toSafeString(fieldCode)) {
    return calculateFormationFieldSummary(projects, tasks, fieldCode);
  }
  return buildFormationOperationalSummary(projects, tasks);
}

module.exports = {
  buildFormationProjectIndex,
  calculateFormationProjectProgress,
  calculateFormationReadiness,
  calculateFormationFieldSummary,
  buildFormationOperationalSummary,
  buildFormationIntelligence,
  buildFormationExecutiveActions
};
