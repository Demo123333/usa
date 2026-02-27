import json
from copy import deepcopy

FILE_PATH = r"E:\BOXOFFICETRACKER\TrackingBOXOffice\usa\USA Data\243198_2026-02-27.json"

# ===============================
# LOAD DATA
# ===============================
with open(FILE_PATH, encoding="utf-8") as f:
    original_data = json.load(f)

corrected_data = deepcopy(original_data)


# ===============================
# CALCULATE BEFORE TOTAL
# ===============================
before_total_gross = round(
    sum(s.get("grossRevenueUSD", 0) for s in original_data), 2
)

before_total_with_fee = round(
    sum(s.get("totalRevenueWithFee", 0) for s in original_data), 2
)


# ===============================
# GROUP FOR CORRECTION
# ===============================
grouped = {}

for entry in corrected_data:
    key = (
        entry.get("theater_name"),
        entry.get("date"),
        entry.get("language")
    )

    if key not in grouped:
        grouped[key] = {"standard": None, "d-box": None}

    fmt = entry.get("format", "").lower()

    if fmt == "standard":
        grouped[key]["standard"] = entry
    elif fmt == "d-box":
        grouped[key]["d-box"] = entry


# ===============================
# APPLY D-BOX CORRECTION
# ===============================
for group in grouped.values():

    standard = group.get("standard")
    dbox = group.get("d-box")

    if standard and dbox:

        standard["totalSeatCount"] -= dbox["totalSeatCount"]

        removed = min(standard["totalSeatSold"], dbox["totalSeatSold"])
        standard["totalSeatSold"] -= removed

        price = standard.get("adultTicketPrice", 0)
        fee = standard.get("fee", 0)

        standard["grossRevenueUSD"] -= removed * price
        standard["totalRevenueWithFee"] -= removed * (price + fee)

        if standard["totalSeatCount"] > 0:
            standard["occupancy"] = round(
                (standard["totalSeatSold"] / standard["totalSeatCount"]) * 100,
                2
            )
        else:
            standard["occupancy"] = 0.0


# ===============================
# CALCULATE AFTER TOTAL
# ===============================
after_total_gross = round(
    sum(s.get("grossRevenueUSD", 0) for s in corrected_data), 2
)

after_total_with_fee = round(
    sum(s.get("totalRevenueWithFee", 0) for s in corrected_data), 2
)


# ===============================
# PRINT OVERALL VALIDATION
# ===============================
print("\n==============================")
print("🎯 OVERALL RECONCILIATION")
print("==============================")

print(f"{'Before Gross':<25}{before_total_gross:>15,.2f}")
print(f"{'After Gross':<25}{after_total_gross:>15,.2f}")
print(f"{'Difference':<25}{(after_total_gross-before_total_gross):>15,.2f}")

print("\nWith Fee")
print(f"{'Before Gross (Fee)':<25}{before_total_with_fee:>15,.2f}")
print(f"{'After Gross (Fee)':<25}{after_total_with_fee:>15,.2f}")
print(f"{'Difference':<25}{(after_total_with_fee-before_total_with_fee):>15,.2f}")


# ===============================
# BUILD BEFORE / AFTER MAPS
# ===============================
grouped_before = {}
grouped_after = {}

def build_map(data, container):
    for s in data:
        key = (
            s.get("theater_name"),
            s.get("date"),
            s.get("language")
        )
        if key not in container:
            container[key] = {"standard": None, "d-box": None}

        fmt = s.get("format", "").lower()
        if fmt == "standard":
            container[key]["standard"] = s
        elif fmt == "d-box":
            container[key]["d-box"] = s

build_map(original_data, grouped_before)
build_map(corrected_data, grouped_after)


# ===============================
# MATCHED SHOW GRAND TOTAL
# ===============================
grand_std_before = 0
grand_std_after = 0
grand_dbox = 0
grand_combined_before = 0
grand_combined_after = 0

for key in grouped_before:

    before_std = grouped_before[key].get("standard")
    before_dbox = grouped_before[key].get("d-box")

    after_std = grouped_after.get(key, {}).get("standard")

    if before_std and before_dbox:

        std_before = before_std.get("grossRevenueUSD", 0)
        std_after = after_std.get("grossRevenueUSD", 0)
        dbox_gross = before_dbox.get("grossRevenueUSD", 0)

        grand_std_before += std_before
        grand_std_after += std_after
        grand_dbox += dbox_gross

        grand_combined_before += (std_before + dbox_gross)
        grand_combined_after += (std_after + dbox_gross)


# ===============================
# PRINT GRAND TOTAL (MATCHED)
# ===============================
print("\n==============================")
print("🎬 GRAND TOTAL (MATCHED SHOWS ONLY)")
print("==============================")

print(f"{'Standard Gross BEFORE':<35}{grand_std_before:>15,.2f}")
print(f"{'Standard Gross AFTER':<35}{grand_std_after:>15,.2f}")
print(f"{'D-BOX Gross':<35}{grand_dbox:>15,.2f}")
print("-" * 55)
print(f"{'COMBINED BEFORE':<35}{grand_combined_before:>15,.2f}")
print(f"{'COMBINED AFTER':<35}{grand_combined_after:>15,.2f}")
print(f"{'TOTAL DIFFERENCE':<35}{(grand_combined_after-grand_combined_before):>15,.2f}")

print("\n✅ Done.")