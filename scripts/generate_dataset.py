"""
generate_dataset.py  ·  Dataset Realism V2 + Campaign Orchestration (500 Wells)

Generates / updates:
  data/wells.csv      - 500 wells  + 15 new optional columns (backward-compatible)
  data/dn_master.csv  - ~204 DNs   +  4 new optional columns (dn_id preserved → dn_logs safe)
  data/campaigns.csv  - 6 campaign metadata rows (optional reference file)

Hard constraints:
  · data/dn_logs.csv is NOT touched (historical read-only)
  · All new columns are optional (empty string = unset)
  · server_working.js is NOT touched
  · Existing column names/order are preserved; new columns appended
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

TODAY = datetime(2026, 4, 4)
DATA_DIR = Path(__file__).parent.parent / "data"

# ---------------------------------------------------------------------------
# Field personality parameters
# ---------------------------------------------------------------------------
FIELD_PARAMS = {
    "Ain Dar": {
        "code": "ANDR",
        "base_lo": 600,  "base_hi": 1400,
        "decline_lo": 1.0, "decline_hi": 3.0,
        "stab_lo": 6, "stab_hi": 9,
        "res_lo": 6,  "res_hi": 9,
        "age_lo": 500, "age_hi": 7000,
    },
    "Abqaiq": {
        "code": "ABQQ",
        "base_lo": 400,  "base_hi": 1100,
        "decline_lo": 2.0, "decline_hi": 5.0,
        "stab_lo": 3, "stab_hi": 7,
        "res_lo": 4,  "res_hi": 7,
        "age_lo": 1000, "age_hi": 8000,
    },
}

# ---------------------------------------------------------------------------
# Well archetypes  (weight = % of total wells)
# ---------------------------------------------------------------------------
ARCHETYPES = {
    "Workhorse":     {"rate_lo": 0.85, "rate_hi": 1.00, "stab_d": (1,  2), "age_lo": 3000, "age_hi": 7000, "w": 40},
    "Young Star":    {"rate_lo": 1.10, "rate_hi": 1.30, "stab_d": (2,  3), "age_lo":  500, "age_hi": 1500, "w": 20},
    "Mature":        {"rate_lo": 0.60, "rate_hi": 0.80, "stab_d": (-1, 1), "age_lo": 5000, "age_hi": 8000, "w": 25},
    "Problem Child": {"rate_lo": 0.40, "rate_hi": 0.60, "stab_d": (-3,-1), "age_lo": 4000, "age_hi": 8000, "w": 15},
}

# ---------------------------------------------------------------------------
# Campaign definitions  (200 wells total across 6 campaigns)
# ---------------------------------------------------------------------------
CAMPAIGNS = [
    {
        "id": "CAM-001", "type": "Preventive Maintenance", "scope": "Sector North",
        "n": 60, "teams": ["Team-A", "Team-B", "Team-C"],
        "sgs": ["SG-001", "SG-002"], "status": "Active",
        "start": "2026-03-15", "end": "2026-05-15", "dn_pct": 0.58,
    },
    {
        "id": "CAM-002", "type": "Network Optimization", "scope": "Cluster 3-5",
        "n": 50, "teams": ["Team-D", "Team-E", "Team-F"],
        "sgs": ["SG-003"], "status": "In Progress",
        "start": "2026-03-20", "end": "2026-05-20", "dn_pct": 0.64,
    },
    {
        "id": "CAM-003", "type": "Pressure Management", "scope": "ABQQ Field",
        "n": 40, "teams": ["Team-G", "Team-H"],
        "sgs": ["SG-004", "SG-005"], "status": "Planning",
        "start": "2026-04-10", "end": "2026-06-10", "dn_pct": 0.45,
    },
    {
        "id": "CAM-004", "type": "Emergency Response", "scope": "Critical Wells",
        "n": 20, "teams": ["Team-I"],
        "sgs": ["SG-006"], "status": "Urgent",
        "start": "2026-04-01", "end": "2026-04-30", "dn_pct": 0.90,
    },
    {
        "id": "CAM-005", "type": "Rig Mobilization", "scope": "High-Value Wells",
        "n": 15, "teams": ["Team-J"],
        "sgs": ["SG-007"], "status": "Planning",
        "start": "2026-04-15", "end": "2026-06-15", "dn_pct": 0.60,
    },
    {
        "id": "CAM-006", "type": "Workover Campaign", "scope": "Mature Wells",
        "n": 15, "teams": ["Team-K"],
        "sgs": ["SG-008", "SG-009"], "status": "Active",
        "start": "2026-03-01", "end": "2026-05-01", "dn_pct": 0.47,
    },
]

# ---------------------------------------------------------------------------
# DN impact mapping
# ---------------------------------------------------------------------------
IMPACT_LEVELS  = ["none", "partial", "restriction", "critical", "shutin"]
IMPACT_WEIGHTS = [30,     20,        20,            20,         10]
IMPACT_TYPES   = {
    "none":        "monitoring",
    "partial":     "production_loss",
    "restriction": "production_loss",
    "critical":    "equipment_damage",
    "shutin":      "safety",
}

# Impact level → (loss_lo, loss_hi) as fraction of base_rate
IMPACT_LOSS = {
    "none":        (0.00, 0.00),
    "partial":     (0.10, 0.30),
    "restriction": (0.20, 0.40),
    "critical":    (0.40, 0.70),
    "shutin":      (1.00, 1.00),
}

# Production multiplier applied to effective rate when a DN is active
IMPACT_RATE_MULT = {
    "none":        (1.00, 1.00),
    "partial":     (0.70, 0.90),
    "restriction": (0.60, 0.80),
    "critical":    (0.30, 0.60),
    "shutin":      (0.00, 0.00),
}

# ---------------------------------------------------------------------------
# Legacy constants kept for backward-compatibility with build_dns()
# ---------------------------------------------------------------------------
DN_TYPES = [
    {"dn_type_id": 1, "dn_type": "Sand Encroachment",  "type_group": "CFC"},
    {"dn_type_id": 2, "dn_type": "Instrument Failure",  "type_group": "CFC"},
    {"dn_type_id": 3, "dn_type": "Valve Failure",       "type_group": "CFC"},
    {"dn_type_id": 4, "dn_type": "Tubing Issue",        "type_group": "CFC"},
    {"dn_type_id": 5, "dn_type": "Pinhole Leak",        "type_group": "CRD"},
    {"dn_type_id": 6, "dn_type": "Flowline Corrosion",  "type_group": "CRD"},
    {"dn_type_id": 7, "dn_type": "Tie-in Modification", "type_group": "CRD"},
    {"dn_type_id": 8, "dn_type": "Line Blockage",       "type_group": "CRD"},
]

STATUS_FLOWS = {
    "CFC": [
        ("DN not issuing",           "FOEU"),
        ("FOEU not issuing package", "FOEU"),
        ("Under Engineering Review", "FOEU"),
        ("Package Ready",            "FOEU"),
        ("Waiting CFC Execution",    "Maintenance (CFC)"),
        ("Execution started",        "Maintenance (CFC)"),
        ("Under Inspection",         "Inspection"),
        ("Completed",                "Inspection"),
        ("Closed",                   "Field Operations"),
    ],
    "CRD": [
        ("DN not issuing",           "FOEU"),
        ("Under Engineering Review", "FOEU"),
        ("Package Ready",            "FOEU"),
        ("Waiting CRD Execution",    "Maintenance Planner (CRD)"),
        ("Execution started",        "Maintenance Planner (CRD)"),
        ("Under Inspection",         "Inspection"),
        ("Completed",                "Inspection"),
        ("Closed",                   "Field Operations"),
    ],
}

PRIORITIES = ["Low", "Medium", "High"]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ri(lo: int, hi: int) -> int:
    return random.randint(lo, hi)


def rf(lo: float, hi: float) -> float:
    return random.uniform(lo, hi)


def random_date_within(days_back: int) -> datetime:
    return TODAY - timedelta(days=ri(0, days_back))


def format_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def generate_well_name(field_code: str, used_names: set) -> str:
    while True:
        if random.random() < 0.55:
            num = ri(100, 999)
        else:
            num = ri(1000, 1999)
        name = f"{field_code}-{num:03d}" if num < 1000 else f"{field_code}-{num}"
        if name not in used_names:
            used_names.add(name)
            return name


def oil_rate_for_status(status: str) -> int:
    if status == "On Production":
        return ri(450, 1650)
    if status == "Testing":
        return ri(80, 500)
    return 0


def priority_for_status(status: str) -> str:
    if status in ("Shut-in", "Locked Potential"):
        return random.choices(PRIORITIES, weights=[10, 25, 65])[0]
    if status == "On Production":
        return random.choices(PRIORITIES, weights=[20, 60, 20])[0]
    return random.choices(PRIORITIES, weights=[25, 55, 20])[0]


def progress_from_stage(stage_index: int, total_stages: int) -> int:
    if stage_index >= total_stages - 2:
        return ri(85, 100)
    if stage_index >= total_stages // 2:
        return ri(35, 80)
    return ri(0, 40)


def write_csv(path: Path, fieldnames: list, rows: list) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

# ---------------------------------------------------------------------------
# STEP 1 – Build base well rows  (same logic as original for reproducibility)
# ---------------------------------------------------------------------------

WELL_STATUSES = (
    ["On Production"]   * 330
    + ["Testing"]       * 50
    + ["Shut-in"]       * 55
    + ["Standby"]       * 25
    + ["Locked Potential"] * 20
    + ["Mothball"]      * 20
)


def build_base_wells() -> list:
    statuses = WELL_STATUSES[:]
    random.shuffle(statuses)

    wells = []
    used_names: set = set()
    well_id = 1

    for _ in range(300):   # Ain Dar
        status = statuses.pop()
        wells.append({
            "well_id": str(well_id),
            "well_name": generate_well_name("ANDR", used_names),
            "field": "Ain Dar",
            "production_status": status,
            "oil_rate_bopd": oil_rate_for_status(status),
            "last_updated": format_date(random_date_within(30)),
        })
        well_id += 1

    for _ in range(200):   # Abqaiq
        status = statuses.pop()
        wells.append({
            "well_id": str(well_id),
            "well_name": generate_well_name("ABQQ", used_names),
            "field": "Abqaiq",
            "production_status": status,
            "oil_rate_bopd": oil_rate_for_status(status),
            "last_updated": format_date(random_date_within(30)),
        })
        well_id += 1

    return wells

# ---------------------------------------------------------------------------
# STEP 2 – Assign well archetypes
# ---------------------------------------------------------------------------

def assign_archetypes(wells: list) -> None:
    archetype_pool: list = []
    for name, params in ARCHETYPES.items():
        archetype_pool.extend([name] * params["w"])
    random.shuffle(archetype_pool)
    for i, w in enumerate(wells):
        w["_archetype"] = archetype_pool[i % len(archetype_pool)]

# ---------------------------------------------------------------------------
# STEP 3 – Compute field-personality metrics
# ---------------------------------------------------------------------------

def set_personality(wells: list) -> None:
    for w in wells:
        fp   = FIELD_PARAMS[w["field"]]
        arch = ARCHETYPES[w["_archetype"]]

        # base_rate_bopd
        field_mid = (fp["base_lo"] + fp["base_hi"]) / 2
        base_rate = int(field_mid * rf(arch["rate_lo"], arch["rate_hi"]))
        base_rate = max(int(fp["base_lo"] * arch["rate_lo"]),
                        min(int(fp["base_hi"] * arch["rate_hi"]), base_rate))
        w["base_rate_bopd"] = base_rate

        # decline_rate  (% per year)
        w["decline_rate"] = round(rf(fp["decline_lo"], fp["decline_hi"]), 2)

        # well_age_days
        w["well_age_days"] = ri(arch["age_lo"], arch["age_hi"])

        # stability_factor  (1-9)
        stab_base = ri(fp["stab_lo"], fp["stab_hi"])
        stab_delta_lo, stab_delta_hi = arch["stab_d"]
        stab = stab_base + ri(stab_delta_lo, stab_delta_hi)
        w["stability_factor"] = max(1, min(9, stab))

        # reservoir_quality  (field range, ±1 for variance)
        w["reservoir_quality"] = max(1, min(10, ri(fp["res_lo"], fp["res_hi"])))

# ---------------------------------------------------------------------------
# STEP 4 – Cluster assignment  (~90 clusters, 3-6 wells, ~340 wells total)
# ---------------------------------------------------------------------------

def assign_clusters(wells: list) -> None:
    for w in wells:
        w["cluster_id"] = ""

    idle_statuses = {"Shut-in", "Standby", "Locked Potential", "Mothball"}
    active  = [w for w in wells if w["production_status"] not in idle_statuses]
    idle    = [w for w in wells if w["production_status"] in     idle_statuses]

    # Small fraction of idle wells can still be in a cluster (rig-nearby)
    idle_sample = random.sample(idle, min(20, len(idle)))
    pool = active + idle_sample
    random.shuffle(pool)

    TARGET_CLUSTERED = 340
    MAX_CLUSTERS     = 90

    assigned = 0
    cnum     = 1
    idx      = 0

    while assigned < TARGET_CLUSTERED and cnum <= MAX_CLUSTERS and idx < len(pool):
        remaining_wells    = TARGET_CLUSTERED - assigned
        remaining_clusters = MAX_CLUSTERS - cnum + 1
        # Prefer size 3-4 to reach closer to 90 clusters from 340 wells
        size = random.choices([3, 4, 5, 6], weights=[30, 50, 15, 5])[0]
        size = max(3, min(size, remaining_wells, 6))
        if size < 3:
            break

        cid = f"CL-{cnum:03d}"
        for _ in range(size):
            if idx >= len(pool):
                break
            pool[idx]["cluster_id"] = cid
            idx      += 1
            assigned += 1

        cnum += 1

# ---------------------------------------------------------------------------
# STEP 5 – Campaign & team assignment  (200 wells, 11 teams, 9 SGs)
# ---------------------------------------------------------------------------

def assign_campaigns(wells: list, dn_well_ids: set) -> None:
    """Assign 200 wells to 6 campaigns with DN-correlation bias."""
    for w in wells:
        w["campaign_id"]       = ""
        w["assigned_team"]     = ""
        w["event_type"]        = ""
        w["event_scope"]       = ""
        w["shutdown_group_id"] = ""

    excluded   = {"Mothball", "Locked Potential"}
    assigned   = set()   # well_ids already placed in a campaign

    def _score(w: dict, prefer_dn: bool, prefer_field: str,
               prefer_arch: str, prefer_cluster: bool) -> float:
        if w["well_id"] in assigned:
            return 0.0
        if w["production_status"] in excluded:
            return 0.0
        s = 1.0
        if prefer_dn and w["well_id"] in dn_well_ids:
            s += 3.0
        if prefer_arch and w.get("_archetype") == prefer_arch:
            s += 2.0
        if prefer_field and w["field"] == prefer_field:
            s += 2.0
        if prefer_cluster and w.get("cluster_id"):
            s += 1.0
        # Problem Child and Mature wells more likely in troubled campaigns
        if w.get("_archetype") in ("Mature", "Problem Child"):
            s += 0.5
        return s

    def pick(n: int, prefer_dn=False, prefer_field="", prefer_arch="",
             prefer_cluster=False) -> list:
        scores = [
            (_score(w, prefer_dn, prefer_field, prefer_arch, prefer_cluster), w)
            for w in wells
        ]
        eligible = [(s, w) for s, w in scores if s > 0.0]
        if not eligible:
            return []
        total = sum(s for s, _ in eligible)
        probs = [s / total for s, _ in eligible]
        k = min(n, len(eligible))
        chosen = random.choices(eligible, weights=probs, k=k * 5)  # oversample
        seen: set = set()
        result: list = []
        for _, w in chosen:
            if w["well_id"] not in seen and w["well_id"] not in assigned:
                seen.add(w["well_id"])
                result.append(w)
            if len(result) >= n:
                break
        return result

    for cam in CAMPAIGNS:
        cid   = cam["id"]
        n     = cam["n"]
        teams = cam["teams"]
        sgs   = cam["sgs"]

        if cid == "CAM-001":   # Broad preventive maintenance
            chosen = pick(n, prefer_dn=True,  prefer_cluster=True)
        elif cid == "CAM-002": # Network / cluster-based
            chosen = pick(n, prefer_dn=True,  prefer_cluster=True)
        elif cid == "CAM-003": # ABQQ field focus
            chosen = pick(n, prefer_dn=True,  prefer_field="Abqaiq")
        elif cid == "CAM-004": # Emergency – highest DN priority
            chosen = pick(n, prefer_dn=True,  prefer_arch="Problem Child")
        elif cid == "CAM-005": # Rig mobilization – critical Problem Child
            chosen = pick(n, prefer_dn=True,  prefer_arch="Problem Child", prefer_cluster=True)
        elif cid == "CAM-006": # Workover – Mature wells
            chosen = pick(n, prefer_dn=True,  prefer_arch="Mature")
        else:
            chosen = pick(n, prefer_dn=True)

        # Assign fields with uneven team distribution
        team_counter: dict = {t: 0 for t in teams}
        for i, w in enumerate(chosen):
            assigned.add(w["well_id"])

            # Uneven team load: first team gets ~35%, subsequent split the rest
            if len(teams) == 1:
                team = teams[0]
            else:
                # Weighted toward first team for realism
                weights = [35] + [int(65 / (len(teams) - 1))] * (len(teams) - 1)
                team = random.choices(teams, weights=weights)[0]

            sg = sgs[i % len(sgs)]   # cycle through shutdown groups

            w["campaign_id"]       = cid
            w["assigned_team"]     = team
            w["event_type"]        = cam["type"]
            w["event_scope"]       = cam["scope"]
            w["shutdown_group_id"] = sg

# ---------------------------------------------------------------------------
# STEP 6 – Rig / workover / stripping flags
# ---------------------------------------------------------------------------

def assign_rig_flags(wells: list) -> None:
    for w in wells:
        w["rig_required"]                   = ""
        w["workover_flag"]                  = ""
        w["stripping_required"]             = ""
        w["rig_shutdown_duration_category"] = ""

    # ── CAM-005 : rig mobilization targets ──────────────────────────────────
    cam005 = [w for w in wells if w.get("campaign_id") == "CAM-005"]
    for w in cam005:
        w["rig_required"]       = "yes"
        w["workover_flag"]      = "yes"
        w["stripping_required"] = "yes"
        w["production_status"]  = "Shut-in"
        w["oil_rate_bopd"]      = 0

    # ── Nearby cluster wells (collateral from CAM-005) ───────────────────────
    cam005_clusters = {w["cluster_id"] for w in cam005 if w.get("cluster_id")}
    collateral_rig = [
        w for w in wells
        if w.get("cluster_id") in cam005_clusters
        and w.get("campaign_id") != "CAM-005"
        and w.get("cluster_id")
    ]
    for w in collateral_rig[:50]:
        w["rig_shutdown_duration_category"] = random.choice(["short", "medium"])
        w["production_status"]  = "Shut-in"
        w["oil_rate_bopd"]      = 0

    # ── CAM-006 : workover targets ───────────────────────────────────────────
    cam006 = [w for w in wells if w.get("campaign_id") == "CAM-006"]
    for w in cam006[:10]:   # 8-10 explicit workover targets
        w["workover_flag"]      = "yes"
        w["stripping_required"] = random.choice(["yes", "no"])

    # ── Nearby cluster wells (CAM-006 collateral, extended shutdown) ─────────
    cam006_clusters = {w["cluster_id"] for w in cam006 if w.get("cluster_id")}
    collateral_wo = [
        w for w in wells
        if w.get("cluster_id") in cam006_clusters
        and w.get("campaign_id") != "CAM-006"
        and w.get("cluster_id")
        and not w.get("rig_shutdown_duration_category")
    ]
    for w in collateral_wo[:20]:
        w["rig_shutdown_duration_category"] = "extended"

# ---------------------------------------------------------------------------
# STEP 7 – Enhance existing DN master with 4 new optional columns
#          dn_ids are PRESERVED so dn_logs.csv references remain valid.
# ---------------------------------------------------------------------------

def load_dn_master() -> list:
    """Load the existing dn_master.csv preserving all original columns."""
    with open(DATA_DIR / "dn_master.csv", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def enhance_dn_master(dn_master: list, wells: list) -> list:
    """
    Add impact_level, impact_type, estimated_loss_bopd, rig_required to
    existing DN rows.  All dn_ids and well_ids are unchanged.

    Impact distribution target:
      30% none  · 20% partial  · 20% restriction  · 20% critical  · 10% shutin
    """
    well_map = {w["well_id"]: w for w in wells}

    # Build a deterministic pool that matches the target distribution
    n = len(dn_master)
    pool: list = []
    for level, weight in zip(IMPACT_LEVELS, IMPACT_WEIGHTS):
        pool.extend([level] * round(n * weight / 100))
    # Top up / trim to exactly n
    while len(pool) < n:
        pool.append("none")
    pool = pool[:n]
    random.shuffle(pool)

    for i, dn in enumerate(dn_master):
        wid  = dn["well_id"]
        well = well_map.get(wid, {})
        arch = well.get("_archetype", "")

        # Pick from pool, then allow archetype-based reroll
        impact_level = pool[i]
        if arch == "Problem Child" and random.random() < 0.45:
            impact_level = random.choices(
                ["restriction", "critical", "shutin"], weights=[40, 40, 20])[0]
        elif arch == "Mature" and random.random() < 0.30:
            impact_level = random.choices(
                ["partial", "restriction", "critical"], weights=[40, 40, 20])[0]

        impact_type   = IMPACT_TYPES[impact_level]
        base_rate     = int(well.get("base_rate_bopd", 800) or 800)
        loss_lo, loss_hi = IMPACT_LOSS[impact_level]
        estimated_loss   = int(base_rate * rf(loss_lo, loss_hi))

        prio = dn.get("priority", "")
        rig_req = (
            "yes"
            if impact_level in ("critical", "shutin") and prio == "High"
            else "no"
        )

        dn["impact_level"]        = impact_level
        dn["impact_type"]         = impact_type
        dn["estimated_loss_bopd"] = estimated_loss
        dn["rig_required"]        = rig_req

    return dn_master

# ---------------------------------------------------------------------------
# STEP 8 – Calculate effective oil_rate_bopd
# ---------------------------------------------------------------------------

def calculate_production(wells: list, dn_master: list) -> None:
    """
    Overwrite oil_rate_bopd on each well with a value that reflects
    field personality, age-decline, DN impact, and campaign impact.
    production_status is also updated to reflect operational reality.
    """
    # Build well_id → worst DN impact level
    impact_order = {lv: i for i, lv in enumerate(IMPACT_LEVELS)}
    worst_impact: dict = {}
    for dn in dn_master:
        wid = dn["well_id"]
        lv  = dn.get("impact_level", "none")
        if wid not in worst_impact or impact_order[lv] > impact_order[worst_impact[wid]]:
            worst_impact[wid] = lv

    for w in wells:
        wid    = w["well_id"]
        status = w["production_status"]

        # Already hard-shut-in by rig flags (set in step 6)
        if w.get("rig_required") == "yes" or w.get("rig_shutdown_duration_category"):
            w["production_status"] = "Shut-in"
            w["oil_rate_bopd"]     = 0
            continue

        # CAM-004 Emergency Response – always shut-in regardless of prior status
        if w.get("campaign_id") == "CAM-004":
            w["production_status"] = "Shut-in"
            w["oil_rate_bopd"]     = 0
            continue

        # Non-producing status → zero rate (no further calculation needed)
        if status in ("Shut-in", "Standby", "Locked Potential", "Mothball"):
            w["oil_rate_bopd"] = 0
            continue

        base_rate   = int(w.get("base_rate_bopd", 800))
        age_days    = int(w.get("well_age_days", 2000))
        decline_pct = float(w.get("decline_rate", 2.0))

        # Age-decline factor  (floor at 30% of base)
        years          = age_days / 365.0
        decline_factor = max(0.30, 1.0 - (decline_pct / 100.0) * years)

        # DN impact
        impact_level = worst_impact.get(wid, "none")
        dn_lo, dn_hi = IMPACT_RATE_MULT[impact_level]
        dn_mult      = rf(dn_lo, dn_hi)

        if impact_level == "shutin":
            w["production_status"] = "Shut-in"
            w["oil_rate_bopd"]     = 0
            continue

        # Campaign production impact
        camp_id   = w.get("campaign_id", "")
        camp_mult = 1.0
        if camp_id == "CAM-001":           # Preventive Maintenance – slight
            camp_mult = rf(0.85, 1.00)
        elif camp_id == "CAM-002":         # Network Optimization – moderate
            camp_mult = rf(0.80, 0.95)
        elif camp_id == "CAM-003":         # Pressure Management – moderate
            camp_mult = rf(0.70, 0.90)
        elif camp_id == "CAM-006":         # Workover – significant reduction
            camp_mult = rf(0.55, 0.85)

        effective = int(base_rate * decline_factor * dn_mult * camp_mult)
        effective = max(0, effective)

        w["oil_rate_bopd"] = effective
        if effective == 0:
            w["production_status"] = "Shut-in"
        elif status == "On Production" and effective < 80:
            w["production_status"] = "Testing"

# ---------------------------------------------------------------------------
# STEP 9 – Write campaigns.csv  (optional metadata reference)
# ---------------------------------------------------------------------------

def write_campaigns_csv() -> None:
    rows = []
    for cam in CAMPAIGNS:
        rows.append({
            "campaign_id":      cam["id"],
            "event_type":       cam["type"],
            "event_scope":      cam["scope"],
            "well_count":       cam["n"],
            "team_count":       len(cam["teams"]),
            "status":           cam["status"],
            "start_date":       cam["start"],
            "end_date":         cam["end"],
            "shutdown_groups":  "|".join(cam["sgs"]),
        })
    write_csv(
        DATA_DIR / "campaigns.csv",
        ["campaign_id", "event_type", "event_scope", "well_count", "team_count",
         "status", "start_date", "end_date", "shutdown_groups"],
        rows,
    )

# ---------------------------------------------------------------------------
# STEP 10 – Verification report
# ---------------------------------------------------------------------------

def print_report(wells: list, dn_master: list) -> None:
    from collections import Counter

    print("\n" + "=" * 60)
    print("DATASET REALISM V2 — VERIFICATION REPORT")
    print("=" * 60)

    print(f"\n  Total wells  : {len(wells)}")

    field_dist = Counter(w["field"] for w in wells)
    for field, n in sorted(field_dist.items()):
        print(f"    {field}: {n}")

    status_dist = Counter(w["production_status"] for w in wells)
    print("\n  Production status:")
    for s, n in sorted(status_dist.items()):
        print(f"    {s}: {n}")

    arch_dist = Counter(w.get("_archetype", "") for w in wells)
    print("\n  Archetypes:")
    for a, n in sorted(arch_dist.items()):
        print(f"    {a}: {n}")

    clustered = sum(1 for w in wells if w.get("cluster_id"))
    cluster_ids = {w["cluster_id"] for w in wells if w.get("cluster_id")}
    print(f"\n  Clustered wells : {clustered}  ({len(cluster_ids)} clusters)")

    camp_dist = Counter(w.get("campaign_id", "") for w in wells)
    campaign_total = sum(v for k, v in camp_dist.items() if k)
    print(f"\n  Campaign wells  : {campaign_total}")
    for c, n in sorted(camp_dist.items()):
        if c:
            print(f"    {c}: {n}")

    dn_wells = {dn["well_id"] for dn in dn_master}
    print(f"\n  DN master rows  : {len(dn_master)}")
    print(f"  Wells with DNs  : {len(dn_wells)}")

    impact_dist = Counter(dn.get("impact_level", "") for dn in dn_master)
    print("\n  DN impact levels:")
    for lv, n in sorted(impact_dist.items()):
        print(f"    {lv}: {n}")

    rig_yes = sum(1 for w in wells if w.get("rig_required") == "yes")
    wo_yes  = sum(1 for w in wells if w.get("workover_flag") == "yes")
    shut_dur = Counter(w.get("rig_shutdown_duration_category", "") for w in wells)
    print(f"\n  rig_required=yes : {rig_yes}")
    print(f"  workover_flag=yes: {wo_yes}")
    print("  rig_shutdown_duration_category:")
    for k, v in sorted(shut_dur.items()):
        if k:
            print(f"    {k}: {v}")

    # Overlap matrix
    camp_ids = {w["well_id"] for w in wells if w.get("campaign_id")}
    cl_ids   = {w["well_id"] for w in wells if w.get("cluster_id")}
    idle_ids = {w["well_id"] for w in wells
                if w["production_status"] in ("Shut-in", "Standby", "Locked Potential", "Mothball")}

    def ov(has_dn: bool, has_cam: bool, has_cl: bool) -> int:
        """Return count of non-idle wells matching all three context flags."""
        result = set(w["well_id"] for w in wells)
        result = (result & dn_wells)  if has_dn  else (result - dn_wells)
        result = (result & camp_ids)  if has_cam else (result - camp_ids)
        result = (result & cl_ids)    if has_cl  else (result - cl_ids)
        return len(result - idle_ids)

    print("\n  Overlap matrix (excl. idle):")
    print(f"    DN+Campaign+Cluster : {ov(True,  True,  True)}")
    print(f"    DN+Campaign         : {ov(True,  True,  False)}")
    print(f"    DN+Cluster          : {ov(True,  False, True)}")
    print(f"    DN only             : {ov(True,  False, False)}")
    print(f"    Campaign+Cluster    : {ov(False, True,  True)}")
    print(f"    Campaign only       : {ov(False, True,  False)}")
    print(f"    Cluster only        : {ov(False, False, True)}")
    print(f"    Baseline (none)     : {ov(False, False, False)}")
    print(f"    Idle/Mothball/etc.  : {len(idle_ids)}")
    print("=" * 60 + "\n")

# ---------------------------------------------------------------------------
# Legacy helpers kept for reference (used only in comments / future calls)
# ---------------------------------------------------------------------------

def pick_dn_count_for_well(status: str) -> int:
    if status == "Shut-in":
        return random.choices([0, 1, 2], weights=[10, 60, 30])[0]
    if status == "Locked Potential":
        return random.choices([0, 1, 2], weights=[20, 60, 20])[0]
    if status == "Standby":
        return random.choices([0, 1], weights=[75, 25])[0]
    if status == "Mothball":
        return random.choices([0, 1], weights=[85, 15])[0]
    if status == "Testing":
        return random.choices([0, 1], weights=[75, 25])[0]
    if status == "On Production":
        return random.choices([0, 1], weights=[70, 30])[0]
    return 0


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:
    # ── Build wells ──────────────────────────────────────────────────────────
    wells = build_base_wells()
    assign_archetypes(wells)
    set_personality(wells)
    assign_clusters(wells)

    # ── Load and enhance existing DN master (preserves dn_ids → dn_logs safe) ─
    dn_master = load_dn_master()
    dn_well_ids = {dn["well_id"] for dn in dn_master}

    # ── Campaign + team assignment ───────────────────────────────────────────
    assign_campaigns(wells, dn_well_ids)

    # ── Rig / workover / stripping flags ────────────────────────────────────
    assign_rig_flags(wells)

    # ── Enhance DN master with 4 new optional columns ───────────────────────
    #    Must happen BEFORE calculate_production so impact_level is available.
    enhance_dn_master(dn_master, wells)

    # ── Recalculate oil_rate_bopd with full context ──────────────────────────
    calculate_production(wells, dn_master)

    # ── Write output files ───────────────────────────────────────────────────
    WELLS_FIELDS = [
        # Original columns (unchanged order + names)
        "well_id", "well_name", "field", "production_status",
        "oil_rate_bopd", "last_updated",
        # New optional columns
        "base_rate_bopd", "decline_rate", "well_age_days",
        "reservoir_quality", "stability_factor",
        "cluster_id",
        "shutdown_group_id", "campaign_id", "assigned_team",
        "event_type", "event_scope",
        "rig_required", "workover_flag", "stripping_required",
        "rig_shutdown_duration_category",
    ]
    write_csv(DATA_DIR / "wells.csv", WELLS_FIELDS, wells)

    DN_FIELDS = [
        # Original columns (unchanged)
        "dn_id", "well_id", "dn_type", "dn_type_id", "type_group",
        "priority", "created_date", "progress_percent",
        # New optional columns
        "impact_level", "impact_type", "estimated_loss_bopd", "rig_required",
    ]
    write_csv(DATA_DIR / "dn_master.csv", DN_FIELDS, dn_master)

    # dn_logs.csv is NOT written here (historical read-only)

    write_campaigns_csv()

    print_report(wells, dn_master)
    print(f"Wells written    : {len(wells)}")
    print(f"DN master rows   : {len(dn_master)}")
    print(f"DN log rows kept : (unchanged — data/dn_logs.csv not modified)")


if __name__ == "__main__":
    main()