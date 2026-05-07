#!/usr/bin/env python3
"""
Comprehensive analysis and benchmark selection for engineering diagram dataset.
Classifies figures by diagram type and subsystem, computes complexity metrics,
and selects 500 diverse, complex benchmark figures.
"""

import os
import json
import csv
import math
from collections import defaultdict, Counter
from statistics import mean, median

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
RESULTS_DIR = "/opt/predii/isha/enginuity_source_data/results"
MANIFEST_PATH = os.path.join(RESULTS_DIR, "dataset_manifest_global.tsv")
BENCHMARK_OUT = os.path.join(RESULTS_DIR, "benchmark_subset_500.tsv")
DATA_DIRS = [f"data-{i}" for i in range(1, 11)]
BENCHMARK_SIZE = 500


# ─────────────────────────────────────────────
# Classification helpers
# ─────────────────────────────────────────────

def classify_type(text: str) -> str:
    t = text.lower()
    if "wiring harness" in t:
        return "Wiring Harness Diagram"
    if "wiring" in t:
        return "Wiring Diagram"
    if "schematic" in t or "diagram" in t:
        return "Schematic/Circuit Diagram"
    if "assembly" in t and "wiring" not in t:
        return "Assembly/Exploded Parts View"
    if "cross section" in t or "section" in t:
        return "Cross-Section View"
    if "special tools" in t or "tool kit" in t:
        return "Special Tools Reference"
    if "hydraulic" in t or "fluid lines" in t or "supply and return lines" in t:
        return "Hydraulic/Fluid Diagram"
    if "kit" in t:
        return "Kit/Equipment Assembly"
    return "Parts/Assembly Diagram"


def classify_subsystem(text: str) -> str:
    t = text.lower()

    # Powertrain - Engine
    if any(kw in t for kw in ["engine", "exhaust", "crankcase", "valve", "intake", "turbo", "cylinder", "piston"]):
        return "Powertrain - Engine"

    # Powertrain - Transmission/Drivetrain
    if any(kw in t for kw in ["transmission", "clutch", "planetary", "gear", "torque", "drive shaft", "transfer case"]):
        return "Powertrain - Transmission/Drivetrain"

    # Fuel System
    if any(kw in t for kw in ["fuel tank", "fuel pump", "fuel line", "fuel filter", "air cleaner", "carburetor", "injector"]):
        return "Fuel System"

    # Chassis - Brakes
    if any(kw in t for kw in ["brake", "master cylinder", "parking brake", "wheel cylinder"]):
        return "Chassis - Brakes"

    # Chassis - Suspension/Steering
    if any(kw in t for kw in ["suspension", "spring", "axle", "shock", "steering", "wheel", "tire"]):
        return "Chassis - Suspension/Steering"

    # Chassis - Frame/Body Mount
    if any(kw in t for kw in ["frame", "crossmember", "body mount"]):
        return "Chassis - Frame/Body Mount"

    # Electrical - Wiring/Harness
    if any(kw in t for kw in ["wiring harness", "wiring", "electrical", "wire"]):
        return "Electrical - Wiring/Harness"

    # Electrical - Components
    if any(kw in t for kw in ["light", "lamp", "horn", "switch", "relay", "contactor", "fuse", "battery", "buss bar", "solenoid"]):
        return "Electrical - Components"

    # HVAC/Climate Control
    if any(kw in t for kw in ["air conditioner", "heater", "blower", "hvac", "defroster", "duct", "arctic kit", "ventilat"]):
        return "HVAC/Climate Control"

    # Hydraulic/Fluid System
    if any(kw in t for kw in ["hydraulic", "pump", "hose", "valve"]):
        return "Hydraulic/Fluid System"

    # Body/Interior
    if any(kw in t for kw in ["door", "window", "windshield", "cab", "body", "panel", "interior", "seat", "floor"]):
        return "Body/Interior"

    # Fire Suppression/Water Systems
    if any(kw in t for kw in ["fire", "water", "boiler"]):
        return "Fire Suppression/Water Systems"

    # Material Handling/Winch
    if any(kw in t for kw in ["winch", "crane", "hoist", "boom"]):
        return "Material Handling/Winch"

    # Special Tools
    if any(kw in t for kw in ["special tools", "tool"]):
        return "Special Tools"

    # Medical/Ambulance Equipment
    if any(kw in t for kw in ["oxygen", "medical", "ambulance", "patient", "litter"]):
        return "Medical/Ambulance Equipment"

    # Armor/Protection
    if any(kw in t for kw in ["armor", "protection"]):
        return "Armor/Protection"

    # Communications
    if any(kw in t for kw in ["intercom", "radio", "communication"]):
        return "Communications"

    return "Other/Auxiliary Systems"


# ─────────────────────────────────────────────
# Step 1: Load figures_tables_pages.json for all data dirs
# ─────────────────────────────────────────────

print("=" * 70)
print("STEP 1: Loading figures_tables_pages.json for all data directories")
print("=" * 70)

# page_info[(data_dir, page_num)] = {description, label_count, figure_number}
page_info = {}

for data_dir in DATA_DIRS:
    json_path = os.path.join(RESULTS_DIR, data_dir, "figures_tables_pages.json")
    if not os.path.exists(json_path):
        print(f"  WARNING: {json_path} not found, skipping")
        continue

    with open(json_path) as f:
        data = json.load(f)

    pairing = data.get("pairing", {})
    figures_loaded = 0

    for fig_num_str, fig_info in pairing.items():
        figure_pages = fig_info.get("figure_pages", [])
        table_pages = fig_info.get("table_pages", [])
        figure_texts = fig_info.get("figure_texts", [])

        # Get description from figure_texts
        description = " ".join(figure_texts).strip() if figure_texts else ""

        # Count TSV rows for label_count (sum over all table_pages)
        label_count = 0
        for tp in table_pages:
            tsv_path = os.path.join(RESULTS_DIR, data_dir, "table_data", f"page_{tp}.tsv")
            if os.path.exists(tsv_path):
                try:
                    with open(tsv_path, newline='', encoding='utf-8', errors='replace') as tsv_f:
                        reader = csv.reader(tsv_f, delimiter='\t')
                        rows = list(reader)
                        # Subtract header row if present
                        row_count = max(0, len(rows) - 1) if rows else 0
                        label_count += row_count
                except Exception as e:
                    pass  # skip unreadable TSVs

        # Map each figure_page to this figure's info
        for fp in figure_pages:
            key = (data_dir, int(fp))
            page_info[key] = {
                "description": description,
                "label_count": label_count,
                "figure_number": fig_num_str,
            }
        figures_loaded += 1

    print(f"  {data_dir}: {figures_loaded} figures loaded from pairing dict")

print(f"\nTotal page_info entries: {len(page_info)}")


# ─────────────────────────────────────────────
# Step 2: Load dataset_manifest_global.tsv
# ─────────────────────────────────────────────

print("\n" + "=" * 70)
print("STEP 2: Loading dataset_manifest_global.tsv")
print("=" * 70)

manifest_rows = []
with open(MANIFEST_PATH, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f, delimiter='\t')
    for row in reader:
        manifest_rows.append(dict(row))

print(f"  Total manifest rows: {len(manifest_rows)}")
unique_figure_paths = set(r['figure_path'] for r in manifest_rows)
print(f"  Unique figure_paths: {len(unique_figure_paths)}")


# ─────────────────────────────────────────────
# Step 3 & 4: Join manifest with figure info, classify
# ─────────────────────────────────────────────

print("\n" + "=" * 70)
print("STEP 3 & 4: Joining manifest with figure info and classifying")
print("=" * 70)

enriched_rows = []
no_description_count = 0
has_description_count = 0

for row in manifest_rows:
    figure_path = row['figure_path']
    # Parse data_dir and page_num from figure_path
    # e.g. "data-1/figures/page_660.png" -> data_dir="data-1", page_num=660
    parts = figure_path.split('/')
    data_dir = parts[0]
    filename = parts[-1]  # page_660.png
    page_num_str = filename.replace('page_', '').replace('.png', '')
    try:
        page_num = int(page_num_str)
    except ValueError:
        page_num = -1

    key = (data_dir, page_num)
    info = page_info.get(key)

    if info and info.get('description'):
        description = info['description']
        label_count = info['label_count']
        has_description_count += 1
    else:
        # No description: try to get label_count from TSV rows anyway
        description = ""
        label_count = 0
        if info:
            label_count = info.get('label_count', 0)
        # Fallback: compute label_count from tsv_path
        if label_count == 0 and row.get('tsv_path'):
            tsv_path = os.path.join(RESULTS_DIR, row['tsv_path'])
            if os.path.exists(tsv_path):
                try:
                    with open(tsv_path, newline='', encoding='utf-8', errors='replace') as tsv_f:
                        reader = csv.reader(tsv_f, delimiter='\t')
                        rows_data = list(reader)
                        label_count = max(0, len(rows_data) - 1) if rows_data else 0
                except Exception:
                    pass
        no_description_count += 1

    # Classify
    if description:
        diagram_type = classify_type(description)
        subsystem = classify_subsystem(description)
    else:
        diagram_type = "Parts/Assembly Diagram"
        subsystem = "Other/Auxiliary Systems"

    enriched_rows.append({
        **row,
        'description': description,
        'diagram_type': diagram_type,
        'subsystem': subsystem,
        'label_count': label_count,
    })

print(f"  Rows with description: {has_description_count}")
print(f"  Rows without description: {no_description_count}")


# ─────────────────────────────────────────────
# Deduplicate by figure_path (keep max label_count per figure_path)
# ─────────────────────────────────────────────

# Build per figure_path aggregated info
figure_map = {}
for row in enriched_rows:
    fp = row['figure_path']
    if fp not in figure_map:
        figure_map[fp] = row.copy()
    else:
        # Keep entry with higher label_count
        if row['label_count'] > figure_map[fp]['label_count']:
            figure_map[fp] = row.copy()

unique_figures = list(figure_map.values())
print(f"\nUnique figures after deduplication: {len(unique_figures)}")


# ─────────────────────────────────────────────
# Step 5: Compute overall stats
# ─────────────────────────────────────────────

print("\n" + "=" * 70)
print("STEP 5: Overall Statistics")
print("=" * 70)

type_counts = Counter(f['diagram_type'] for f in unique_figures)
subsystem_counts = Counter(f['subsystem'] for f in unique_figures)

print("\n--- Unique Figures per Diagram Type ---")
print(f"{'Diagram Type':<40} {'Count':>8} {'%':>7}")
print("-" * 58)
total_unique = len(unique_figures)
for dtype, count in sorted(type_counts.items(), key=lambda x: -x[1]):
    pct = 100.0 * count / total_unique
    print(f"  {dtype:<38} {count:>8,}  {pct:>6.1f}%")
print(f"  {'TOTAL':<38} {total_unique:>8,}  100.0%")

print("\n--- Unique Figures per Subsystem ---")
print(f"{'Subsystem':<45} {'Count':>8} {'%':>7}")
print("-" * 63)
for subsys, count in sorted(subsystem_counts.items(), key=lambda x: -x[1]):
    pct = 100.0 * count / total_unique
    print(f"  {subsys:<43} {count:>8,}  {pct:>6.1f}%")
print(f"  {'TOTAL':<43} {total_unique:>8,}  100.0%")

# Label count stats overall
all_label_counts = [f['label_count'] for f in unique_figures]
print(f"\n--- Label Count Distribution (all unique figures) ---")
print(f"  Min:    {min(all_label_counts)}")
print(f"  Max:    {max(all_label_counts)}")
print(f"  Mean:   {mean(all_label_counts):.1f}")
print(f"  Median: {median(all_label_counts):.1f}")

# Count by pdf_id
pdf_id_counts = Counter(f['pdf_id'] for f in unique_figures)
print(f"\n--- Unique Figures per pdf_id (data-N) ---")
print(f"{'pdf_id':<12} {'Count':>8}")
print("-" * 22)
for pdf_id, count in sorted(pdf_id_counts.items()):
    print(f"  {pdf_id:<10} {count:>8,}")


# ─────────────────────────────────────────────
# Step 6: Select 500 benchmark figures
# ─────────────────────────────────────────────

print("\n" + "=" * 70)
print("STEP 6: Selecting 500 Benchmark Figures")
print("=" * 70)

# Exclude Special Tools Reference
candidates = [f for f in unique_figures if f['diagram_type'] != "Special Tools Reference"]
print(f"  Candidates after excluding 'Special Tools Reference': {len(candidates)}")

# Sort by label_count descending
candidates_sorted = sorted(candidates, key=lambda x: -x['label_count'])

selected_fps = set()

# ── Stratified sampling by type ──────────────────────────────────────────
# Min 3 per type, total proportional to type frequency
type_groups = defaultdict(list)
for f in candidates_sorted:
    type_groups[f['diagram_type']].append(f)

total_candidates = len(candidates)
type_alloc = {}
for dtype, figs in type_groups.items():
    # Proportional to frequency, min 3
    alloc = max(3, round(BENCHMARK_SIZE * len(figs) / total_candidates))
    type_alloc[dtype] = alloc

print(f"\n  Type allocations (before adjustment):")
for dtype, alloc in sorted(type_alloc.items(), key=lambda x: -x[1]):
    print(f"    {dtype:<40} -> {alloc}")

# Take top N by label_count for each type
for dtype, figs in type_groups.items():
    alloc = type_alloc[dtype]
    for f in figs[:alloc]:
        selected_fps.add(f['figure_path'])

print(f"\n  Selected after type stratification: {len(selected_fps)}")

# ── Ensure min 5 per subsystem ────────────────────────────────────────────
subsystem_groups = defaultdict(list)
for f in candidates_sorted:
    subsystem_groups[f['subsystem']].append(f)

for subsys, figs in subsystem_groups.items():
    count_in_selection = sum(1 for f in figs if f['figure_path'] in selected_fps)
    needed = max(0, 5 - count_in_selection)
    added = 0
    for f in figs:
        if needed == 0:
            break
        if f['figure_path'] not in selected_fps:
            selected_fps.add(f['figure_path'])
            added += 1
            needed -= 1

print(f"  Selected after subsystem min-5 guarantee: {len(selected_fps)}")

# ── Fill remaining slots with highest label_count ─────────────────────────
remaining_needed = BENCHMARK_SIZE - len(selected_fps)
if remaining_needed > 0:
    for f in candidates_sorted:
        if remaining_needed == 0:
            break
        if f['figure_path'] not in selected_fps:
            selected_fps.add(f['figure_path'])
            remaining_needed -= 1
    print(f"  Filled remaining slots. Total selected: {len(selected_fps)}")
elif len(selected_fps) > BENCHMARK_SIZE:
    # Trim back to 500: remove lowest label_count ones
    # Build list of (fp, label_count)
    selected_list = [(fp, figure_map[fp]['label_count']) for fp in selected_fps]
    selected_list.sort(key=lambda x: -x[1])
    selected_fps = set(fp for fp, _ in selected_list[:BENCHMARK_SIZE])
    print(f"  Trimmed to {len(selected_fps)} (removed lowest label_count)")

print(f"\n  Final benchmark size: {len(selected_fps)} unique figure_paths")


# ─────────────────────────────────────────────
# Step 7: Write benchmark_subset_500.tsv
# ─────────────────────────────────────────────

print("\n" + "=" * 70)
print("STEP 7: Writing benchmark_subset_500.tsv")
print("=" * 70)

# Filter original manifest rows to benchmark figure_paths
# Keep ALL manifest rows (multiple rows per figure_path for multiple tables)
benchmark_rows = [row for row in enriched_rows if row['figure_path'] in selected_fps]
print(f"  Benchmark rows (including multiple table rows per figure): {len(benchmark_rows)}")

fieldnames = ['figure_path', 'table_path', 'tsv_path', 'figure', 'pdf_id',
              'description', 'diagram_type', 'subsystem', 'label_count']

with open(BENCHMARK_OUT, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='\t', extrasaction='ignore')
    writer.writeheader()
    writer.writerows(benchmark_rows)

print(f"  Written to: {BENCHMARK_OUT}")


# ─────────────────────────────────────────────
# Step 8: Benchmark subset stats
# ─────────────────────────────────────────────

print("\n" + "=" * 70)
print("STEP 8: Benchmark Subset Statistics")
print("=" * 70)

# Use deduplicated figures for stats
benchmark_unique = [figure_map[fp] for fp in selected_fps]

bench_type_counts = Counter(f['diagram_type'] for f in benchmark_unique)
bench_subsystem_counts = Counter(f['subsystem'] for f in benchmark_unique)
bench_pdf_counts = Counter(f['pdf_id'] for f in benchmark_unique)
bench_label_counts = [f['label_count'] for f in benchmark_unique]

print(f"\n--- Benchmark: Count per Diagram Type ---")
print(f"{'Diagram Type':<40} {'Count':>8} {'%':>7}")
print("-" * 58)
for dtype, count in sorted(bench_type_counts.items(), key=lambda x: -x[1]):
    pct = 100.0 * count / BENCHMARK_SIZE
    print(f"  {dtype:<38} {count:>8,}  {pct:>6.1f}%")
print(f"  {'TOTAL':<38} {BENCHMARK_SIZE:>8,}  100.0%")

print(f"\n--- Benchmark: Count per Subsystem ---")
print(f"{'Subsystem':<45} {'Count':>8} {'%':>7}")
print("-" * 63)
for subsys, count in sorted(bench_subsystem_counts.items(), key=lambda x: -x[1]):
    pct = 100.0 * count / BENCHMARK_SIZE
    print(f"  {subsys:<43} {count:>8,}  {pct:>6.1f}%")
print(f"  {'TOTAL':<43} {BENCHMARK_SIZE:>8,}  100.0%")

print(f"\n--- Benchmark: Label Count Distribution ---")
print(f"  Min:    {min(bench_label_counts)}")
print(f"  Max:    {max(bench_label_counts)}")
print(f"  Mean:   {mean(bench_label_counts):.1f}")
print(f"  Median: {median(bench_label_counts):.1f}")

# Histogram buckets
buckets = [0, 5, 10, 20, 50, 100, 200, 500, float('inf')]
bucket_labels = ["0-4", "5-9", "10-19", "20-49", "50-99", "100-199", "200-499", "500+"]
bucket_counts = [0] * len(bucket_labels)
for lc in bench_label_counts:
    for i in range(len(buckets) - 1):
        if buckets[i] <= lc < buckets[i + 1]:
            bucket_counts[i] += 1
            break

print(f"\n  Label Count Histogram:")
print(f"  {'Range':<10} {'Count':>8} {'Bar'}")
print("  " + "-" * 45)
for label, count in zip(bucket_labels, bucket_counts):
    bar = "█" * (count // 2)
    print(f"  {label:<10} {count:>8}  {bar}")

print(f"\n--- Benchmark: Count per pdf_id (data-N) ---")
print(f"{'pdf_id':<12} {'Count':>8}")
print("-" * 22)
for pdf_id, count in sorted(bench_pdf_counts.items()):
    print(f"  {pdf_id:<10} {count:>8,}")

print("\n" + "=" * 70)
print("ANALYSIS COMPLETE")
print(f"  Benchmark TSV written to: {BENCHMARK_OUT}")
print("=" * 70)
