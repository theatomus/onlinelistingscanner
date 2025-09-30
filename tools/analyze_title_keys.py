from __future__ import annotations

import json
from pathlib import Path


def main() -> int:
    base = Path.cwd()
    src = base / "tools" / "title_keys_preview.jsonl"
    if not src.exists():
        print(f"Missing {src}")
        return 1

    # Outputs
    out_dir = base / "tools" / "diagnostics"
    out_dir.mkdir(parents=True, exist_ok=True)
    f_storage_ram = out_dir / "storage_with_ram_sizes.txt"
    f_apple_brand = out_dir / "apple_brand_misassigned.txt"
    f_gpu_vram_as_ram = out_dir / "gpu_vram_as_system_ram.txt"

    storage_terms = {"SSD", "HDD", "NVME", "M.2", "EMMC"}
    ram_suspect_sizes = {f"{n}GB" for n in [2,3,4,6,8,12,16,20,24,32,48,64]}

    def is_intel_like(cpu_model: str) -> bool:
        cpu_model = (cpu_model or "").upper()
        return cpu_model.startswith(("I3-", "I5-", "I7-", "I9-", "XEON")) or "CORE " in cpu_model

    stor_lines = []
    apple_lines = []
    gpu_lines = []

    with src.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            title = rec.get("title", "")
            keys = rec.get("title_keys", {}) or {}

            # 1) Storage mapped to RAM sizes
            stype = keys.get("title_storage_type_key")
            scap = keys.get("title_storage_capacity1_key")
            if stype in storage_terms and scap in ram_suspect_sizes:
                stor_lines.append(line)

            # 2) Apple brand misassignment: Apple set but model looks Intel/Xeon
            brand = keys.get("title_cpu_brand_key")
            cpu_model = keys.get("title_cpu_model_key", "")
            if brand == "Apple" and is_intel_like(cpu_model):
                apple_lines.append(line)

            # 3) GPU VRAM as system RAM: when gpu_memory_type present and DDR3/5 present as RAM type/size
            gpu_mem = keys.get("title_gpu_memory_type_key")
            ram_type = keys.get("title_ram_type_key", "").upper()
            ram_size = keys.get("title_ram_size_key", "").upper()
            if gpu_mem and (ram_type.startswith("GDDR") or ram_size.endswith("GB") and "GRAPHICS" in title.upper()):
                gpu_lines.append(line)

    f_storage_ram.write_text("\n".join(stor_lines), encoding="utf-8")
    f_apple_brand.write_text("\n".join(apple_lines), encoding="utf-8")
    f_gpu_vram_as_ram.write_text("\n".join(gpu_lines), encoding="utf-8")

    print(f"Storage with RAM sizes: {len(stor_lines)} -> {f_storage_ram}")
    print(f"Apple misassigned: {len(apple_lines)} -> {f_apple_brand}")
    print(f"GPU VRAM as system RAM: {len(gpu_lines)} -> {f_gpu_vram_as_ram}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


