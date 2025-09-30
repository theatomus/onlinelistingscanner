import threading
import tkinter as tk
from tkinter import ttk
from pathlib import Path
from typing import Optional

# Simple GUI wrapper for Watchdog TEST MODE
# - Shows ON/OFF status
# - Shows last violation reason and count
# - Buttons: Toggle, Start, Stop, Quit
# - Hotkey Ctrl+I+O still works via watchdog internals


def run_watchdog_in_thread(wd) -> threading.Thread:
    t = threading.Thread(target=wd.run, daemon=True)
    t.start()
    return t


def main():
    # Import works both as module and as script
    if __package__:
        from .watchdog import Watchdog  # type: ignore
    else:
        from watchdog import Watchdog  # type: ignore

    root = tk.Tk()
    root.title("Watchdog (TEST MODE)")
    root.geometry("420x260")

    status_var = tk.StringVar(value="OFF")
    running_status_var = tk.StringVar(value="Stopped (no hooks)")
    vio_count_var = tk.StringVar(value="0")
    last_reason_var = tk.StringVar(value="-")
    running_var = tk.BooleanVar(value=False)

    def on_state_change(is_enabled: bool) -> None:
        status_var.set("ON" if is_enabled else "OFF")

    def on_violation(reason: str) -> None:
        # Called from watchdog thread; marshal to Tk thread
        def _update():
            try:
                vio_count_var.set(str(wd.violation_count))
                last_reason_var.set(reason)
            except Exception:
                pass
        root.after(0, _update)

    # Defaults: monitor the repo root as critical (but TEST MODE protects it)
    repo_root = Path(__file__).resolve().parents[2]
    wd = Watchdog(
        critical_paths=[str(repo_root)],
        working_start_str="09:00",
        working_end_str="15:30",
        log_path=Path("logs")/"watchdog"/"security_watchdog_test.log",
        sentinel_path=Path("logs")/"watchdog"/"test_last_run_complete.txt",
        control_path=Path("state")/"watchdog_control.txt",
        status_path=Path("state")/"watchdog_status.txt",
        test_mode=True,
        on_violation=on_violation,
        on_state_change=on_state_change,
        block_unauthorized=True,
        block_keyboard_unauthorized=True,
    )

    thread_ref: Optional[threading.Thread] = None

    def start_watchdog():
        nonlocal thread_ref
        if running_var.get():
            return
        # Clear counters and last reason on fresh start for clarity
        try:
            wd.violation_count = 0
            last_reason_var.set("-")
            vio_count_var.set("0")
        except Exception:
            pass
        thread_ref = run_watchdog_in_thread(wd)
        running_var.set(True)
        running_status_var.set("Running (hooks active)")

    def stop_watchdog():
        nonlocal thread_ref
        if not running_var.get():
            return
        try:
            wd.request_stop()
        except Exception:
            pass
        thread_ref = None
        running_var.set(False)
        running_status_var.set("Stopped (no hooks)")

    def toggle_enabled():
        wd.enabled = not wd.enabled
        try:
            wd._write_control()
            wd._write_status()
        except Exception:
            pass
        on_state_change(wd.enabled)

    # Also ensure GUI reflects control file toggles made outside the GUI
    def poll_control_file():
        try:
            desired = wd._read_control()
            want = (desired == "ON")
            if want != wd.enabled:
                wd.enabled = want
                on_state_change(wd.enabled)
        except Exception:
            pass
        root.after(500, poll_control_file)

    frm = ttk.Frame(root, padding=12)
    frm.pack(fill=tk.BOTH, expand=True)

    ttk.Label(frm, text="Watchdog (TEST MODE)", font=("Segoe UI", 12, "bold")).pack(anchor=tk.W)

    status_row = ttk.Frame(frm)
    status_row.pack(fill=tk.X, pady=(8, 2))
    ttk.Label(status_row, text="Status:", width=12).pack(side=tk.LEFT)
    ttk.Label(status_row, textvariable=status_var, foreground="#0A7", font=("Segoe UI", 11, "bold")).pack(side=tk.LEFT)

    run_row = ttk.Frame(frm)
    run_row.pack(fill=tk.X, pady=(2, 2))
    ttk.Label(run_row, text="Hooks:", width=12).pack(side=tk.LEFT)
    ttk.Label(run_row, textvariable=running_status_var).pack(side=tk.LEFT)

    vio_row = ttk.Frame(frm)
    vio_row.pack(fill=tk.X, pady=(2, 2))
    ttk.Label(vio_row, text="Violations:", width=12).pack(side=tk.LEFT)
    ttk.Label(vio_row, textvariable=vio_count_var).pack(side=tk.LEFT)

    reason_row = ttk.Frame(frm)
    reason_row.pack(fill=tk.X, pady=(2, 12))
    ttk.Label(reason_row, text="Last reason:", width=12).pack(side=tk.LEFT)
    ttk.Label(reason_row, textvariable=last_reason_var, wraplength=300, justify=tk.LEFT).pack(side=tk.LEFT)

    btn_row = ttk.Frame(frm)
    btn_row.pack(fill=tk.X)
    ttk.Button(btn_row, text="Start", command=start_watchdog).pack(side=tk.LEFT, padx=4)
    ttk.Button(btn_row, text="Stop", command=stop_watchdog).pack(side=tk.LEFT, padx=4)
    ttk.Button(btn_row, text="Toggle ON/OFF", command=toggle_enabled).pack(side=tk.LEFT, padx=4)
    ttk.Button(btn_row, text="Quit", command=root.destroy).pack(side=tk.RIGHT, padx=4)

    # Advanced toggles
    adv_row = ttk.Frame(frm)
    adv_row.pack(fill=tk.X, pady=(8, 0))
    block_var = tk.BooleanVar(value=True)
    def apply_policy():
        wd.block_unauthorized = bool(block_var.get())
        wd.block_keyboard_unauthorized = bool(block_var.get())
    ttk.Checkbutton(adv_row, text="Block unauthorized input (mouse + keyboard)", variable=block_var, command=apply_policy).pack(side=tk.LEFT)

    # Start by default so the GUI shows activity without extra clicks
    start_watchdog()

    # Periodic refresh to reflect live counters even without new violations
    def refresh_ui():
        try:
            status_var.set("ON" if wd.enabled else "OFF")
            vio_count_var.set(str(wd.violation_count))
        except Exception:
            pass
        root.after(500, refresh_ui)

    refresh_ui()
    poll_control_file()

    root.mainloop()


if __name__ == "__main__":
    main()


