import sys
from typing import Dict, Any

from description_parsing import parse_description_structured


def run_case(name: str, text: str, expected: Dict[str, Any]) -> None:
    print(f"\n=== CASE: {name} ===")
    result = parse_description_structured(text)
    print("Extracted:")
    for k in sorted(result.keys()):
        if k == 'bullets':
            continue
        print(f"  {k}: {result[k]}")
    failures = []
    for k, v in expected.items():
        rv = result.get(k)
        if rv != v:
            failures.append((k, v, rv))
    if failures:
        print("-- FAILURES --")
        for k, exp, got in failures:
            print(f"  {k}: expected={exp!r}, got={got!r}")
        raise AssertionError(f"Case '{name}' failed with {len(failures)} mismatches")
    print("OK")


def main() -> None:
    # Note: item_contents folder appears empty; using synthetic hard cases that simulate real-world errors.
    cases = [
        (
            "RAM vs Storage with negation, VRAM nearby",
            """
            === ITEM DESCRIPTION ===
            No SSD / HDD included. 16GB DDR4 memory installed.
            GPU: GeForce GTX1060 with 6GB GDDR5 VRAM.
            (2 x 8GB) modules. PC3-12800
            """,
            {
                "ram_size": "16GB",
                "ram_config": "2x8GB",
                "ram_type": "DDR4",
                "ram_speed_grade": "PC3-12800",
                "gpu": "GTX1060",
                "gpu_memory_type": "GDDR5",
                "gpu_spec": "6GB",
            },
        ),
        (
            "Storage dual capacity slash, type mapping",
            """
            === ITEM DESCRIPTION ===
            Storage: 256GB/1TB, SSD + HDD combo. 2.5in bay.
            """,
            {
                "storage_capacity": "256GB",
                "storage_capacity2": "1TB",
                "storage_drive_size": "2.5in",
            },
        ),
        (
            "Network carriers and status (Wi fi, unlocked variants)",
            """
            === ITEM DESCRIPTION ===
            Network / Carrier: Wi fi only
            Un-Locked (net unlocked). VZW supported.
            """,
            {
                "network_status": "Network Unlocked",
                "network_carrier": "Verizon",
            },
        ),
        (
            "CPU complex naming",
            """
            === ITEM DESCRIPTION ===
            Intel Core Ultra 7 165H at 4.8GHz boost.
            """,
            {
                "cpu_family": "Core Ultra 7",
                "cpu_model": "Core Ultra 7 165H",
                "cpu_speed": "4.80GHz",
            },
        ),
        (
            "Screen mixed formats",
            """
            === ITEM DESCRIPTION ===
            15.6 inch FHD (1920 x 1080) 144 Hz, IPS panel, Non-touch
            """,
            {
                "screen_size": "15.6in",
                "screen_resolution": "1920x1080",
                "screen_hertz": "144Hz",
                "screen_panel_type": "IPS",
                "screen_touch": "Non-Touch",
            },
        ),
        (
            "OS detection",
            """
            === ITEM DESCRIPTION ===
            Windows 11 Pro freshly installed and activated.
            """,
            {
                "os_type": "Windows",
                "os_edition": "Pro",
                "os_version": "11",
                "os_status": "Installed",
            },
        ),
        (
            "HDD specific fields",
            """
            === ITEM DESCRIPTION ===
            3.5in SATA drive 7200 RPM, transfer up to 6 Gbps. Model: ST1000DM010, P/N 1CH162.
            12000 hours on.
            """,
            {
                "hdd_form_factor": "3.5in",
                "hdd_interface": "SATA",
                "hdd_rpm": "7200",
                "hdd_transfer_rate": "6 Gbps",
                "hdd_model_number": "ST1000DM010",
                "hdd_part_number": "1CH162",
                "hdd_usage_hours": "12000",
            },
        ),
        (
            "Battery status and health",
            """
            === ITEM DESCRIPTION ===
            Battery included; Health 97%. Condition: Good.
            """,
            {
                "battery_presence": "Included",
                "battery_health": "97%",
                "battery_condition": "Good",
            },
        ),
        (
            "Lot + form factor + phone details",
            """
            === ITEM DESCRIPTION ===
            Lot of 9. Ultra Small Form Factor desktop.
            Apple iPad A1474 Space Gray, 32 GB storage. Wi-Fi.
            """,
            {
                "lot": "9",
                "form_factor": "USFF",
                "phone_model": "A1474",
                "color": "Space Gray",
                "storage_size": "32GB",
                "network_status": "WiFi Only",
            },
        ),
        (
            "Slash RAM list and ranges with storage present",
            """
            === ITEM DESCRIPTION ===
            4/8/16GB listed; includes 1TB HDD; 8 GB DDR4 installed.
            Range supported: 8-16GB.
            """,
            {
                "ram_size": "8GB",
                "ram_type": "DDR4",
                "ram_range": "8GB-16GB",
                "storage_capacity": "1TB",
                "storage_type": "HDD",
            },
        ),
        (
            "VRAM vs RAM disambiguation",
            """
            === ITEM DESCRIPTION ===
            16GB RAM, NVIDIA RTX 3080 10GB GDDR6.
            """,
            {
                "ram_size": "16GB",
                "gpu": "RTX3080",
                "gpu_spec": "10GB",
                "gpu_memory_type": "GDDR6",
            },
        ),
    ]

    failures = 0
    for name, text, expected in cases:
        try:
            run_case(name, text, expected)
        except AssertionError as e:
            failures += 1
            print(str(e))

    if failures:
        print(f"\nFAILED {failures} case(s)")
        sys.exit(1)
    print("\nAll cases passed.")


if __name__ == "__main__":
    main()


