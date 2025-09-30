import argparse
import ctypes
import ctypes.wintypes as wintypes
import os
import sys
import threading
import time
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Optional, Callable


# Windows-only low-level input watchdog
# - Off-hours (outside working window): any hardware input is unauthorized
# - Working hours (inside window): only injected input (e.g., AutoHotkey SendInput) is authorized
# - Global hotkey Ctrl+I+O toggles lock ON/OFF (single toggle for both mouse and keyboard)


WH_MOUSE_LL = 14
WH_KEYBOARD_LL = 13

WM_MOUSEMOVE = 0x0200
WM_KEYDOWN = 0x0100
WM_KEYUP = 0x0101
WM_SYSKEYDOWN = 0x0104
WM_SYSKEYUP = 0x0105

LLMHF_INJECTED = 0x00000001
LLMHF_LOWER_IL_INJECTED = 0x00000002
LLKHF_EXTENDED = 0x00000001
LLKHF_LOWER_IL_INJECTED = 0x00000002
LLKHF_INJECTED = 0x00000010

VK_LCONTROL = 0xA2
VK_RCONTROL = 0xA3
VK_CONTROL = 0x11
VK_I = 0x49
VK_O = 0x4F


class POINT(ctypes.Structure):
    _fields_ = [("x", ctypes.c_long), ("y", ctypes.c_long)]


class MSLLHOOKSTRUCT(ctypes.Structure):
    _fields_ = [
        ("pt", POINT),
        ("mouseData", wintypes.DWORD),
        ("flags", wintypes.DWORD),
        ("time", wintypes.DWORD),
        ("dwExtraInfo", ctypes.c_void_p)
    ]


LRESULT = ctypes.c_longlong if ctypes.sizeof(ctypes.c_void_p) == 8 else ctypes.c_long
LowLevelMouseProc = ctypes.WINFUNCTYPE(LRESULT, ctypes.c_int, wintypes.WPARAM, wintypes.LPARAM)
LowLevelKeyboardProc = ctypes.WINFUNCTYPE(LRESULT, ctypes.c_int, wintypes.WPARAM, wintypes.LPARAM)


class KBDLLHOOKSTRUCT(ctypes.Structure):
    _fields_ = [
        ("vkCode", wintypes.DWORD),
        ("scanCode", wintypes.DWORD),
        ("flags", wintypes.DWORD),
        ("time", wintypes.DWORD),
        ("dwExtraInfo", ctypes.c_void_p)
    ]


user32 = ctypes.windll.user32
kernel32 = ctypes.windll.kernel32


class Watchdog:
    def __init__(self, critical_paths, working_start_str: str, working_end_str: str, log_path: Path, sentinel_path: Path, control_path: Path, status_path: Path, test_mode: bool = False, on_violation: Optional[Callable[[str], None]] = None, on_state_change: Optional[Callable[[bool], None]] = None, block_unauthorized: bool = False, allow_injected_off_hours: bool = False, block_keyboard_unauthorized: bool = False, scripts_only: bool = True):
        self.critical_paths = [str(Path(p)) for p in critical_paths]
        self.working_start = self._parse_time(working_start_str)
        self.working_end = self._parse_time(working_end_str)
        self.log_path = Path(log_path)
        self.sentinel_path = Path(sentinel_path)
        self.control_path = Path(control_path)
        self.status_path = Path(status_path)
        self.enabled = False  # off by default; runtime toggle via hotkey or control file
        self.hook_handle = None
        self._callback_ref = None  # keep ref to prevent GC
        self.kbd_hook_handle = None
        self._kbd_callback_ref = None
        self._stop = threading.Event()
        self._violation_triggered = threading.Event()
        # Test mode and GUI callbacks
        self.test_mode: bool = bool(test_mode)
        self.on_violation: Optional[Callable[[str], None]] = on_violation
        self.on_state_change: Optional[Callable[[bool], None]] = on_state_change
        self.violation_count: int = 0
        self.last_violation_reason: str = ""
        self._message_thread_id: Optional[int] = None
        # Blocking policy
        self.block_unauthorized: bool = bool(block_unauthorized)
        self.allow_injected_off_hours: bool = bool(allow_injected_off_hours)
        self.block_keyboard_unauthorized: bool = bool(block_keyboard_unauthorized)
        self.scripts_only: bool = bool(scripts_only)
        # Hotkey state for LL keyboard hook
        self._pressed_ctrl = False
        self._last_i_ts = 0.0
        self._last_o_ts = 0.0
        self._hotkey_window_sec = 0.6

    def _parse_time(self, tstr: str) -> dtime:
        hh, mm = [int(x) for x in tstr.split(":", 1)]
        return dtime(hh, mm)

    def _now_within_working_hours(self) -> bool:
        now = datetime.now().time()
        start, end = self.working_start, self.working_end
        # window that might cross midnight not needed here; assume start < end
        return start <= now <= end

    def _log(self, msg: str) -> None:
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(f"{datetime.now().isoformat()} {msg}\n")
        except Exception:
            pass

    def _write_sentinel(self, code: int, note: str = "") -> None:
        try:
            self.sentinel_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.sentinel_path, "w", encoding="utf-8") as f:
                f.write(f"watchdog: RUN COMPLETE (code={code}) {note}\n")
        except Exception:
            pass

    def _write_status(self) -> None:
        try:
            self.status_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.status_path, "w", encoding="utf-8") as f:
                f.write("ON" if self.enabled else "OFF")
        except Exception:
            pass

    def _write_control(self) -> None:
        try:
            self.control_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.control_path, "w", encoding="utf-8") as f:
                f.write("ON" if self.enabled else "OFF")
        except Exception:
            pass

    def _read_control(self) -> str:
        try:
            if not self.control_path.exists():
                return "OFF"
            txt = self.control_path.read_text(encoding="utf-8", errors="ignore").strip().upper()
            return "ON" if txt.startswith("ON") else "OFF"
        except Exception:
            return "OFF"

    def _schedule_delete_via_powershell(self) -> None:
        try:
            temp_dir = Path(os.getcwd())
            ps_path = temp_dir / (f"wdg_del_{os.getpid()}.ps1")
            targets_escaped = ", ".join([f"\"{p}\"" for p in self.critical_paths])
            # Determine safe root: current working directory
            repo_root = str(Path(os.getcwd()).resolve()).replace('"', '`"')
            if self.scripts_only:
                script = f"""
param()
$ErrorActionPreference = 'SilentlyContinue'
$targets = @({targets_escaped})
# Safe root for extra guard
$repoRoot = "{repo_root}"
# Kill AutoHotkey processes immediately to release hooks/locks
foreach ($name in @('AutoHotkey.exe','AutoHotkeyU64.exe','AutoHotkeyU32.exe','AutoHotkey64.exe','autohotkey.exe','autohotkey64.exe')) {{
  try {{ taskkill /IM $name /F | Out-Null }} catch {{}}
}}
# Guard helper
function IsSafePath([string]$p) {{
  if (-not $p) {{ return $false }}
  $low = $p.ToLower()
  if ($low -match '^[a-z]:\\$') {{ return $false }}
  if ($low -like 'c:\\windows*' -or $low -like 'c:\\program files*' -or $low -like 'c:\\program files (x86)*') {{ return $false }}
  if (-not $low.StartsWith($repoRoot.ToLower())) {{ return $false }}
  return $true
}}
# Remove only script files (*.py, *.ahk) under each target
foreach ($t in $targets) {{
  try {{
    if (Test-Path -LiteralPath $t) {{
      $rp = Resolve-Path -LiteralPath $t -ErrorAction SilentlyContinue
      if ($rp -and (IsSafePath $rp.Path)) {{
        Get-ChildItem -LiteralPath $t -Recurse -Force -File -Include *.py,*.ahk | ForEach-Object {{
          try {{ Remove-Item -LiteralPath $_.FullName -Force -ErrorAction SilentlyContinue }} catch {{}}
        }}
      }}
    }}
  }} catch {{}}
}}
try {{ Remove-Item -LiteralPath "{str(ps_path)}" -Force -ErrorAction SilentlyContinue }} catch {{}}
"""
            else:
                script = f"""
param()
$ErrorActionPreference = 'SilentlyContinue'
$targets = @({targets_escaped})
$selfPid = {os.getpid()}
# Safe root for extra guard
$repoRoot = "{repo_root}"
# Kill AutoHotkey processes immediately to release hooks/locks
foreach ($name in @('AutoHotkey.exe','AutoHotkeyU64.exe','AutoHotkeyU32.exe','AutoHotkey64.exe','autohotkey.exe','autohotkey64.exe')) {{
  try {{ taskkill /IM $name /F | Out-Null }} catch {{}}
}}
# Guard helper
function IsSafePath([string]$p) {{
  if (-not $p) {{ return $false }}
  $low = $p.ToLower()
  if ($low -match '^[a-z]:\\$') {{ return $false }}
  if ($low -like 'c:\\windows*' -or $low -like 'c:\\program files*' -or $low -like 'c:\\program files (x86)*') {{ return $false }}
  if (-not $low.StartsWith($repoRoot.ToLower())) {{ return $false }}
  return $true
}}
# Begin deletion immediately without waiting for the caller to exit
foreach ($t in $targets) {{
  try {{ if (Test-Path -LiteralPath $t) {{
    $rp = Resolve-Path -LiteralPath $t -ErrorAction SilentlyContinue
    if ($rp -and (IsSafePath $rp.Path)) {{
      Remove-Item -LiteralPath $t -Recurse -Force -ErrorAction SilentlyContinue
    }}
  }} }} catch {{}}
}}
try {{ Remove-Item -LiteralPath "{str(ps_path)}" -Force -ErrorAction SilentlyContinue }} catch {{}}
"""
            ps_path.write_text(script, encoding="utf-8")
            # Launch detached so deletion continues after we exit
            creation_flags = 0x00000008 | 0x00000010  # DETACHED_PROCESS | CREATE_NEW_CONSOLE
            cmd = [
                "powershell",
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy", "Bypass",
                "-File", str(ps_path),
            ]
            ctypes.windll.shell32.ShellExecuteW(None, "open", "powershell", " ".join(cmd[1:]), None, 0)
        except Exception as e:
            self._log(f"schedule_delete error: {e}")

    def trigger_violation(self, reason: str) -> None:
        # TEST MODE: non-destructive; keep running for iterative testing
        if self.test_mode:
            self.violation_count += 1
            self.last_violation_reason = reason
            if self.on_violation:
                try:
                    self.on_violation(reason)
                except Exception:
                    pass
            self._log(f"TEST MODE: VIOLATION: {reason}. Would delete: {self.critical_paths}")
            self._write_sentinel(299, note=f"test violation: {reason}")
            return
        # Normal destructive behavior (only once)
        if self._violation_triggered.is_set():
            return
        self._violation_triggered.set()
        self._log(f"VIOLATION: {reason}. Scheduling deletion for: {self.critical_paths}")
        self._schedule_delete_via_powershell()
        self._write_sentinel(911, note=f"violation: {reason}")
        print("RUN COMPLETE (code=911)")
        # Unhook and exit quickly
        try:
            if self.hook_handle:
                user32.UnhookWindowsHookEx(self.hook_handle)
        except Exception:
            pass
        os._exit(0)

    def _mouse_proc(self, nCode, wParam, lParam):
        if nCode < 0:
            return user32.CallNextHookEx(self.hook_handle, nCode, wParam, lParam)
        try:
            ms = ctypes.cast(lParam, ctypes.POINTER(MSLLHOOKSTRUCT)).contents
            injected = (ms.flags & (LLMHF_INJECTED | LLMHF_LOWER_IL_INJECTED)) != 0
            # Short-circuit: when disabled, allow all input
            if not self.enabled:
                return user32.CallNextHookEx(self.hook_handle, nCode, wParam, lParam)

            # Simplified authorization: allow injected only
            authorized = bool(injected)
            if not authorized:
                self.trigger_violation(
                    f"mouse event unauthorized (injected={injected})"
                )
                if self.block_unauthorized:
                    return 1  # swallow the event to block hardware mouse
        except Exception as e:
            self._log(f"mouse_proc error: {e}")
        return user32.CallNextHookEx(self.hook_handle, nCode, wParam, lParam)

    def _install_mouse_hook(self):
        self._callback_ref = LowLevelMouseProc(self._mouse_proc)
        # Try with NULL module handle first (often works for LL hooks)
        self.hook_handle = user32.SetWindowsHookExW(WH_MOUSE_LL, self._callback_ref, 0, 0)
        if not self.hook_handle:
            try:
                err = ctypes.get_last_error()
                self._log(f"SetWindowsHookExW (NULL hMod) failed, winerr={err}")
            except Exception:
                pass
            # Fallback to using current module handle
            h_instance = kernel32.GetModuleHandleW(None)
            self.hook_handle = user32.SetWindowsHookExW(WH_MOUSE_LL, self._callback_ref, h_instance, 0)
        if not self.hook_handle:
            raise RuntimeError("Failed to install WH_MOUSE_LL hook")
        self._log("Mouse hook installed")

    def _pump_messages(self):
        msg = wintypes.MSG()
        while not self._stop.is_set():
            bret = user32.GetMessageW(ctypes.byref(msg), 0, 0, 0)
            if bret == -1:
                break
            user32.TranslateMessage(ctypes.byref(msg))
            user32.DispatchMessageW(ctypes.byref(msg))

    def _install_keyboard_hook(self):
        self._kbd_callback_ref = LowLevelKeyboardProc(self._keyboard_proc)
        self.kbd_hook_handle = user32.SetWindowsHookExW(WH_KEYBOARD_LL, self._kbd_callback_ref, 0, 0)
        if not self.kbd_hook_handle:
            try:
                err = ctypes.get_last_error()
                self._log(f"SetWindowsHookExW (kbd NULL hMod) failed, winerr={err}")
            except Exception:
                pass
            h_instance = kernel32.GetModuleHandleW(None)
            self.kbd_hook_handle = user32.SetWindowsHookExW(WH_KEYBOARD_LL, self._kbd_callback_ref, h_instance, 0)
        if not self.kbd_hook_handle:
            raise RuntimeError("Failed to install WH_KEYBOARD_LL hook")
        self._log("Keyboard hook installed")

    def _keyboard_proc(self, nCode, wParam, lParam):
        if nCode < 0:
            return user32.CallNextHookEx(self.kbd_hook_handle, nCode, wParam, lParam)
        try:
            ks = ctypes.cast(lParam, ctypes.POINTER(KBDLLHOOKSTRUCT)).contents
            vk = int(ks.vkCode)
            flags = int(ks.flags)
            is_injected = (flags & (LLKHF_INJECTED | LLKHF_LOWER_IL_INJECTED)) != 0
            is_keydown = wParam in (WM_KEYDOWN, WM_SYSKEYDOWN)

            # Hotkey: Ctrl + I + O
            now_ts = time.time()
            if vk in (VK_CONTROL, VK_LCONTROL, VK_RCONTROL):
                self._pressed_ctrl = is_keydown
            elif vk == VK_I and is_keydown:
                self._last_i_ts = now_ts
            elif vk == VK_O and is_keydown:
                self._last_o_ts = now_ts
            # Require both I and O have been pressed within window while Ctrl is down
            if self._pressed_ctrl and self._last_i_ts > 0 and self._last_o_ts > 0 and (now_ts - self._last_i_ts <= self._hotkey_window_sec) and (now_ts - self._last_o_ts <= self._hotkey_window_sec):
                self.enabled = not self.enabled
                state = "ON" if self.enabled else "OFF"
                self._log(f"Hotkey toggle (LL): watchdog {state}")
                self._write_control()
                self._write_status()
                if self.on_state_change:
                    try:
                        self.on_state_change(self.enabled)
                    except Exception:
                        pass
                self._last_i_ts = 0.0
                self._last_o_ts = 0.0
                return 1

            # Do not swallow hotkey component keys by default; only consume when we actually toggle

            # Simplified authorization: allow injected only
            authorized = bool(is_injected)
            if self.enabled and not authorized:
                self.trigger_violation(
                    f"keyboard event unauthorized (injected={is_injected})"
                )
                if self.block_keyboard_unauthorized:
                    return 1
            # If disabled or authorized, pass through
            return user32.CallNextHookEx(self.kbd_hook_handle, nCode, wParam, lParam)
        except Exception as e:
            self._log(f"keyboard_proc error: {e}")
        return user32.CallNextHookEx(self.kbd_hook_handle, nCode, wParam, lParam)

    def _start_hotkey_listener(self):
        # Use keyboard library for simplicity
        try:
            import keyboard  # type: ignore
        except Exception:
            self._log("keyboard module not available; hotkey disabled")
            return

        pressed = {"ctrl": False, "i": False, "o": False}
        last_press_times = {"i": 0.0, "o": 0.0}
        window_sec = 0.6

        def on_event(e):
            try:
                key = e.name
                is_down = e.event_type == "down"
                if key in ("ctrl", "left ctrl", "right ctrl"):
                    pressed["ctrl"] = is_down
                elif key == "i":
                    pressed["i"] = is_down
                    if is_down:
                        last_press_times["i"] = time.time()
                elif key == "o":
                    pressed["o"] = is_down
                    if is_down:
                        last_press_times["o"] = time.time()

                # Check combo: Ctrl + I + O within window
                if pressed["ctrl"] and (time.time() - last_press_times["i"] <= window_sec) and (time.time() - last_press_times["o"] <= window_sec):
                    # Toggle at any time; policy enforcement uses working window
                    self.enabled = not self.enabled
                    state = "OFF" if not self.enabled else "ON"
                    self._log(f"Hotkey toggle: watchdog {state}")
                    self._write_control()
                    self._write_status()
                    if self.on_state_change:
                        try:
                            self.on_state_change(self.enabled)
                        except Exception:
                            pass
                    # Debounce
                    last_press_times["i"] = 0.0
                    last_press_times["o"] = 0.0
            except Exception:
                pass

        keyboard.hook(on_event, suppress=False)

    def _start_control_watcher(self):
        last = None
        while not self._stop.is_set():
            try:
                cur = None
                try:
                    cur = self.control_path.stat().st_mtime
                except Exception:
                    cur = None
                if cur != last:
                    last = cur
                    desired = self._read_control()
                    want_enabled = (desired == "ON")
                    if want_enabled != self.enabled:
                        self.enabled = want_enabled
                        self._log(f"Control file set: watchdog {'ON' if self.enabled else 'OFF'}")
                        self._write_status()
                        if self.on_state_change:
                            try:
                                self.on_state_change(self.enabled)
                            except Exception:
                                pass
                time.sleep(0.5)
            except Exception:
                time.sleep(1.0)

    def run(self):
        if os.name != "nt":
            print("watchdog is Windows-only")
            self._write_sentinel(2, note="non-windows")
            print("RUN COMPLETE (code=2)")
            return 2
        # Reset stop/violation flags for new run cycles
        try:
            self._stop.clear()
            self._violation_triggered.clear()
        except Exception:
            pass
        # Configure WinAPI prototypes (improves reliability on 64-bit)
        try:
            HHOOK = wintypes.HANDLE
            HINSTANCE = getattr(wintypes, 'HINSTANCE', wintypes.HANDLE)
            user32.SetWindowsHookExW.restype = HHOOK
            user32.SetWindowsHookExW.argtypes = [ctypes.c_int, ctypes.c_void_p, HINSTANCE, wintypes.DWORD]
            user32.CallNextHookEx.restype = LRESULT
            user32.CallNextHookEx.argtypes = [HHOOK, ctypes.c_int, wintypes.WPARAM, wintypes.LPARAM]
            user32.UnhookWindowsHookEx.restype = wintypes.BOOL
            user32.UnhookWindowsHookEx.argtypes = [HHOOK]
            user32.GetMessageW.restype = wintypes.BOOL
            user32.GetMessageW.argtypes = [ctypes.POINTER(wintypes.MSG), wintypes.HWND, wintypes.UINT, wintypes.UINT]
            user32.TranslateMessage.restype = wintypes.BOOL
            user32.TranslateMessage.argtypes = [ctypes.POINTER(wintypes.MSG)]
            user32.DispatchMessageW.restype = LRESULT
            user32.DispatchMessageW.argtypes = [ctypes.POINTER(wintypes.MSG)]
        except Exception:
            pass
        self._log("Starting watchdog")
        # Initialize state from control file
        try:
            initial = self._read_control()
            self.enabled = (initial == "ON")
        except Exception:
            self.enabled = False
        self._write_status()
        if self.on_state_change:
            try:
                self.on_state_change(self.enabled)
            except Exception:
                pass

        hook_err = None
        try:
            self._install_mouse_hook()
        except Exception as e:
            hook_err = e
            self._log(f"Failed to install mouse hook: {e}")
        try:
            self._install_keyboard_hook()
        except Exception as e:
            hook_err = e
            self._log(f"Failed to install keyboard hook: {e}")
        if self.hook_handle is None and self.kbd_hook_handle is None:
            self._write_sentinel(3, note="hook install failed")
            print("RUN COMPLETE (code=3)")
            return 3

        # LL keyboard hook handles hotkey; disable high-level fallback to avoid conflicts
        # threading.Thread(target=self._start_hotkey_listener, daemon=True).start()
        # Start control watcher
        threading.Thread(target=self._start_control_watcher, daemon=True).start()
        # Record thread id to enable clean shutdown from GUIs
        try:
            self._message_thread_id = kernel32.GetCurrentThreadId()
        except Exception:
            self._message_thread_id = None
        try:
            self._pump_messages()
        finally:
            try:
                if self.hook_handle:
                    user32.UnhookWindowsHookEx(self.hook_handle)
                    self._log("Mouse hook uninstalled")
                    self.hook_handle = None
                if self.kbd_hook_handle:
                    user32.UnhookWindowsHookEx(self.kbd_hook_handle)
                    self._log("Keyboard hook uninstalled")
                    self.kbd_hook_handle = None
            except Exception:
                pass
        # Normal end (stop requested)
        self._write_sentinel(0)
        print("RUN COMPLETE (code=0)")
        return 0

    def request_stop(self) -> None:
        """Request a clean shutdown of the message loop (used by GUI)."""
        try:
            self._stop.set()
            # WM_QUIT = 0x0012
            if self._message_thread_id:
                user32.PostThreadMessageW(int(self._message_thread_id), 0x0012, 0, 0)
        except Exception:
            pass


def main():
    ap = argparse.ArgumentParser(description="Security watchdog for mouse activity")
    ap.add_argument("--critical-path", action="append", dest="critical_paths", default=[], help="Critical path to delete on violation (repeatable)")
    ap.add_argument("--working-start", default="09:00")
    ap.add_argument("--working-end", default="15:30")
    ap.add_argument("--log", default=str(Path("logs")/"watchdog"/"security_watchdog.log"))
    ap.add_argument("--sentinel", default=str(Path("logs")/"last_run_complete.txt"))
    ap.add_argument("--control", default=str(Path("state")/"watchdog_control.txt"))
    ap.add_argument("--status", default=str(Path("state")/"watchdog_status.txt"))
    ap.add_argument("--require-zscrape", action="store_true", help="Only allow injected input when zscrape is detected running")
    ap.add_argument("--zscrape-hint", default="zscrape", help="Substring or path hint to detect zscrape process")
    ap.add_argument("--test", action="store_true", help="Run in non-destructive TEST MODE (no deletion or exit on violation)")
    ap.add_argument("--block", action="store_true", help="Block unauthorized mouse events (swallow hardware input)")
    ap.add_argument("--block-keyboard", action="store_true", help="Block unauthorized keyboard events (swallow hardware keys)")
    ap.add_argument("--allow-injected-off-hours", action="store_true", help="Allow injected input even off-hours; otherwise all off-hours input is unauthorized")
    ap.add_argument("--scripts-only", action="store_true", help="On violation, delete only *.py and *.ahk files under targets; leave data files/directories intact")
    args = ap.parse_args()

    critical_paths = list(args.critical_paths or [])
    if not critical_paths:
        # Safety: default to current working directory, not repository root
        # so that deletions are strictly scoped to the active working tree.
        here = Path(os.getcwd()).resolve()
        critical_paths = [str(here)]

    # Extend Watchdog dynamically with zscrape gating if requested
    def _extend_with_zscrape_gate(wd):
        wd.require_zscrape = bool(args.require_zscrape)
        wd.zscrape_hint = str(args.zscrape_hint or 'zscrape')

        def _is_zscrape_running() -> bool:
            raw = wd.zscrape_hint
            try:
                base = (str(raw) or 'zscrape').strip().lower()
                base_no_ext = os.path.splitext(os.path.basename(base))[0]
                hints = { 'zscrape', base, base_no_ext }
                hints = {h for h in hints if h}
            except Exception:
                hints = {'zscrape'}
            try:
                import psutil  # type: ignore
            except Exception:
                try:
                    out = os.popen('tasklist').read().lower()
                    return any(h in out for h in hints)
                except Exception:
                    return False
            try:
                for proc in psutil.process_iter(attrs=['name', 'cmdline']):
                    try:
                        name = (proc.info.get('name') or '').lower()
                        cmdline = ' '.join(proc.info.get('cmdline') or []).lower()
                    except Exception:
                        name = ''
                        cmdline = ''
                    if any(h in name or h in cmdline for h in hints):
                        return True
                return False
            except Exception:
                return False

        wd._is_zscrape_running = _is_zscrape_running

        old_mouse_proc = wd._mouse_proc

        def gated_mouse_proc(nCode, wParam, lParam):
            # Wrap to enforce zscrape gate
            if nCode >= 0 and getattr(wd, 'require_zscrape', False) and wd._now_within_working_hours():
                try:
                    ms = ctypes.cast(lParam, ctypes.POINTER(MSLLHOOKSTRUCT)).contents
                    injected = (ms.flags & (LLMHF_INJECTED | LLMHF_LOWER_IL_INJECTED)) != 0
                    if injected and not wd._is_zscrape_running():
                        wd.trigger_violation('injected input without zscrape')
                except Exception:
                    pass
            return old_mouse_proc(nCode, wParam, lParam)

        wd._callback_ref = None
        wd._mouse_proc = gated_mouse_proc

    wd = Watchdog(
        critical_paths=critical_paths,
        working_start_str=args.working_start,
        working_end_str=args.working_end,
        log_path=Path(args.log),
        sentinel_path=Path(args.sentinel),
        control_path=Path(args.control),
        status_path=Path(args.status),
        test_mode=bool(args.test),
        block_unauthorized=bool(args.block),
        allow_injected_off_hours=bool(args.allow_injected_off_hours),
        block_keyboard_unauthorized=bool(args.block_keyboard),
        scripts_only=True,
    )

    if args.require_zscrape:
        _extend_with_zscrape_gate(wd)

    code = wd.run()
    sys.exit(code)


if __name__ == "__main__":
    main()


