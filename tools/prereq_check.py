from __future__ import annotations

import argparse
import importlib
import subprocess
import sys
from dataclasses import dataclass
from typing import List, Optional, Tuple


@dataclass
class Requirement:
    import_name: str
    pip_name: str
    optional: bool = False


REQUIREMENTS: List[Requirement] = [
    # Core GUIs and utilities used by runit/flow
    Requirement("tkinterdnd2", "tkinterdnd2", optional=False),
    Requirement("pyperclip", "pyperclip", optional=False),
    # AI/training tooling
    Requirement("yaml", "pyyaml", optional=False),
    Requirement("requests", "requests", optional=False),
    # Parsing helpers
    Requirement("PIL", "Pillow", optional=True),  # scan_monitor uses PIL if present
    Requirement("bs4", "beautifulsoup4", optional=True),  # extract_specifics uses BeautifulSoup
]


def ensure_package(req: Requirement) -> Tuple[str, bool, Optional[str]]:
    """Attempt to import a package; if missing, try installing.

    Returns a tuple of (import_name, installed_ok, version_or_error).
    """
    try:
        mod = importlib.import_module(req.import_name)
        ver = getattr(mod, "__version__", None)
        if ver is None and req.import_name == "PIL":
            try:
                from PIL import Image  # type: ignore
                ver = getattr(Image, "PILLOW_VERSION", None) or getattr(Image, "__version__", None)
            except Exception:
                pass
        return (req.import_name, True, str(ver) if ver else None)
    except Exception as e:
        # Try install
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", req.pip_name])
        except Exception as inst_err:
            return (req.import_name, False, f"install failed: {inst_err}")
        # Re-import after install
        try:
            mod = importlib.import_module(req.import_name)
            ver = getattr(mod, "__version__", None)
            return (req.import_name, True, str(ver) if ver else None)
        except Exception as reimp_err:
            return (req.import_name, False, f"import after install failed: {reimp_err}")


def build_summary(results: List[Tuple[Requirement, Tuple[str, bool, Optional[str]]]]) -> str:
    ok: List[str] = []
    installed: List[str] = []
    failed_required: List[str] = []
    failed_optional: List[str] = []

    for req, (name, ok_flag, ver_or_err) in results:
        if ok_flag and ver_or_err and "install" not in (ver_or_err or ""):
            ok.append(f"{req.pip_name} ({name}) v{ver_or_err}")
        elif ok_flag:
            # Installed now or version unknown
            installed.append(f"{req.pip_name} ({name})")
        else:
            if req.optional:
                failed_optional.append(f"{req.pip_name} ({name}): {ver_or_err}")
            else:
                failed_required.append(f"{req.pip_name} ({name}): {ver_or_err}")

    lines: List[str] = []
    lines.append("ZScrape prerequisites check")
    lines.append("")
    if ok:
        lines.append("Already present:")
        for s in ok:
            lines.append(f"  - {s}")
        lines.append("")
    if installed:
        lines.append("Installed now:")
        for s in installed:
            lines.append(f"  - {s}")
        lines.append("")
    if failed_required:
        lines.append("Failed (required):")
        for s in failed_required:
            lines.append(f"  - {s}")
        lines.append("")
    if failed_optional:
        lines.append("Failed (optional):")
        for s in failed_optional:
            lines.append(f"  - {s}")
        lines.append("")

    if not any([installed, failed_required, failed_optional]):
        lines.append("All required packages are available.")

    if failed_required:
        lines.append("")
        lines.append("Some required packages failed to install. Core features may not work until resolved.")

    return "\n".join(lines).strip()


def show_popup(text: str, title: str = "ZScrape Prerequisites") -> None:
    try:
        import ctypes  # stdlib, Windows only
        MB_ICONINFORMATION = 0x40
        ctypes.windll.user32.MessageBoxW(0, text, title, MB_ICONINFORMATION)
    except Exception:
        # Fallback: print to console
        print(text)


def main() -> int:
    ap = argparse.ArgumentParser(description="Check and install required Python modules for ZScrape and AI training")
    ap.add_argument("--popup", action="store_true", help="Show a Windows popup with the summary")
    args = ap.parse_args()

    results: List[Tuple[Requirement, Tuple[str, bool, Optional[str]]]] = []
    for req in REQUIREMENTS:
        res = ensure_package(req)
        results.append((req, res))

    summary = build_summary(results)
    # Always print to stdout for logs, optionally show popup
    print(summary)
    if args.popup:
        show_popup(summary)
    # Never block the caller with a non-zero code
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


