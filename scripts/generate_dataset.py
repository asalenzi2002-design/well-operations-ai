import csv
import random
from datetime import datetime, timedelta

random.seed(42)

TOTAL_WELLS = 500
AIN_DAR_COUNT = 300
ABQAIQ_COUNT = 200

WELL_STATUSES = (
    ["On Production"] * 330
    + ["Testing"] * 50
    + ["Shut-in"] * 55
    + ["Standby"] * 25
    + ["Locked Potential"] * 20
    + ["Mothball"] * 20
)

DN_TYPES = [
    {"dn_type_id": 1, "dn_type": "Sand Encroachment", "type_group": "CFC"},
    {"dn_type_id": 2, "dn_type": "Instrument Failure", "type_group": "CFC"},
    {"dn_type_id": 3, "dn_type": "Valve Failure", "type_group": "CFC"},
    {"dn_type_id": 4, "dn_type": "Tubing Issue", "type_group": "CFC"},
    {"dn_type_id": 5, "dn_type": "Pinhole Leak", "type_group": "CRD"},
    {"dn_type_id": 6, "dn_type": "Flowline Corrosion", "type_group": "CRD"},
    {"dn_type_id": 7, "dn_type": "Tie-in Modification", "type_group": "CRD"},
    {"dn_type_id": 8, "dn_type": "Line Blockage", "type_group": "CRD"},
]

STATUS_FLOWS = {
    "CFC": [
        ("DN not issuing", "FOEU"),
        ("FOEU not issuing package", "FOEU"),
        ("Under Engineering Review", "FOEU"),
        ("Package Ready", "FOEU"),
        ("Waiting CFC Execution", "Maintenance (CFC)"),
        ("Execution started", "Maintenance (CFC)"),
        ("Under Inspection", "Inspection"),
        ("Completed", "Inspection"),
        ("Closed", "Field Operations"),
    ],
    "CRD": [
        ("DN not issuing", "FOEU"),
        ("Under Engineering Review", "FOEU"),
        ("Package Ready", "FOEU"),
        ("Waiting CRD Execution", "Maintenance Planner (CRD)"),
        ("Execution started", "Maintenance Planner (CRD)"),
        ("Under Inspection", "Inspection"),
        ("Completed", "Inspection"),
        ("Closed", "Field Operations"),
    ],
}

PRIORITIES = ["Low", "Medium", "High"]
TODAY = datetime(2026, 4, 4)


def random_date_within(days_back: int) -> datetime:
    return TODAY - timedelta(days=random.randint(0, days_back))


def format_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def generate_well_name(field_code: str, used_names: set) -> str:
    while True:
        if random.random() < 0.55:
            num = random.randint(100, 999)
        else:
            num = random.randint(1000, 1999)

        name = f"{field_code}-{num:03d}" if num < 1000 else f"{field_code}-{num}"
        if name not in used_names:
            used_names.add(name)
            return name


def oil_rate_for_status(status: str) -> int:
    if status == "On Production":
        return random.randint(450, 1650)
    if status == "Testing":
        return random.randint(80, 500)
    return 0


def build_wells():
    statuses = WELL_STATUSES[:]
    random.shuffle(statuses)

    wells = []
    used_names = set()
    well_id = 1

    for _ in range(AIN_DAR_COUNT):
        status = statuses.pop()
        wells.append({
            "well_id": well_id,
            "well_name": generate_well_name("ANDR", used_names),
            "field": "Ain Dar",
            "production_status": status,
            "oil_rate_bopd": oil_rate_for_status(status),
            "last_updated": format_date(random_date_within(30)),
        })
        well_id += 1

    for _ in range(ABQAIQ_COUNT):
        status = statuses.pop()
        wells.append({
            "well_id": well_id,
            "well_name": generate_well_name("ABQQ", used_names),
            "field": "Abqaiq",
            "production_status": status,
            "oil_rate_bopd": oil_rate_for_status(status),
            "last_updated": format_date(random_date_within(30)),
        })
        well_id += 1

    return wells


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


def priority_for_status(status: str) -> str:
    if status in ("Shut-in", "Locked Potential"):
        return random.choices(PRIORITIES, weights=[10, 25, 65])[0]
    if status == "On Production":
        return random.choices(PRIORITIES, weights=[20, 60, 20])[0]
    return random.choices(PRIORITIES, weights=[25, 55, 20])[0]


def progress_from_stage(stage_index: int, total_stages: int) -> int:
    if stage_index >= total_stages - 2:
        return random.randint(85, 100)
    if stage_index >= total_stages // 2:
        return random.randint(35, 80)
    return random.randint(0, 40)


def build_dns(wells):
    dn_master = []
    dn_logs = []
    dn_id = 1

    for well in wells:
        dn_count = pick_dn_count_for_well(well["production_status"])
        for _ in range(dn_count):
            dn_type_obj = random.choice(DN_TYPES)
            created_date = random_date_within(180)
            flow = STATUS_FLOWS[dn_type_obj["type_group"]]

            max_stage = len(flow) - 1
            if well["production_status"] in ("Shut-in", "Locked Potential"):
                current_stage = random.randint(0, max_stage - 1)
            else:
                current_stage = random.randint(2, max_stage)

            progress = progress_from_stage(current_stage, len(flow))

            dn_master.append({
                "dn_id": dn_id,
                "well_id": well["well_id"],
                "dn_type": dn_type_obj["dn_type"],
                "dn_type_id": dn_type_obj["dn_type_id"],
                "type_group": dn_type_obj["type_group"],
                "priority": priority_for_status(well["production_status"]),
                "created_date": format_date(created_date),
                "progress_percent": progress,
            })

            log_date = created_date
            for stage_idx in range(current_stage + 1):
                status_update, updated_by = flow[stage_idx]
                log_date += timedelta(days=random.randint(3, 20))
                if log_date > TODAY:
                    log_date = TODAY

                dn_logs.append({
                    "dn_id": dn_id,
                    "status_update": status_update,
                    "updated_by": updated_by,
                    "update_date": format_date(log_date),
                })

            dn_id += 1

    return dn_master, dn_logs


def write_csv(path, fieldnames, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    wells = build_wells()
    dn_master, dn_logs = build_dns(wells)

    write_csv(
        "data/wells.csv",
        ["well_id", "well_name", "field", "production_status", "oil_rate_bopd", "last_updated"],
        wells,
    )

    write_csv(
        "data/dn_master.csv",
        ["dn_id", "well_id", "dn_type", "dn_type_id", "type_group", "priority", "created_date", "progress_percent"],
        dn_master,
    )

    write_csv(
        "data/dn_logs.csv",
        ["dn_id", "status_update", "updated_by", "update_date"],
        dn_logs,
    )

    print(f"Wells generated: {len(wells)}")
    print(f"DN master rows: {len(dn_master)}")
    print(f"DN log rows: {len(dn_logs)}")


if __name__ == "__main__":
    main()