"""
双按钮启动器：运行主抓取流程（main_worker）与本地同步流程（uploader_worker），
实时显示子进程输出。仅依赖标准库 + tkinter。
"""

import os
import subprocess
import sys
import threading
from pathlib import Path

try:
    import tkinter as tk
    from tkinter import scrolledtext
    from tkinter import messagebox
except ImportError:
    tk = None  # type: ignore


def _pause_before_exit(message: str) -> None:
    """打包环境下遇到致命错误时暂停，避免窗口闪退。"""
    print(message, file=sys.stderr)
    if getattr(sys, "frozen", False):
        try:
            input("按回车键关闭窗口...")
        except EOFError:
            pass


def runtime_base_dir() -> Path:
    """程序运行目录（源码或打包后 exe 所在目录）。"""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def _ensure_playwright_env(base_dir: Path) -> None:
    """若发布包内存在浏览器目录，设置环境变量供 worker 使用。"""
    browser_dir = base_dir / "ms-playwright_browsers"
    if browser_dir.is_dir():
        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(browser_dir.resolve())


def _run_subprocess(
    base_dir: Path,
    exe_name: str,
    log_widget: "tk.Text",
    on_start: "callable",
    on_finish: "callable",
) -> None:
    """在后台线程中执行 exe，将 stdout/stderr 追加到 log_widget；on_start/on_finish 在主线程回调。"""
    exe_path = base_dir / exe_name
    if not exe_path.exists():
        log_widget.insert(tk.END, f"错误：未找到 {exe_path}\n")
        return

    def run() -> None:
        try:
            proc = subprocess.Popen(
                [str(exe_path)],
                cwd=str(base_dir),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env=os.environ.copy(),
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
                encoding="utf-8",
                errors="replace",
            )
        except Exception as e:
            log_widget.after(0, lambda: _append_log(log_widget, f"启动失败: {e}\n"))
            log_widget.after(0, on_finish)
            return

        log_widget._current_proc = proc  # type: ignore

        def read_loop() -> None:
            for line in proc.stdout:
                log_widget.after(0, lambda l=line: _append_log(log_widget, l))
            proc.wait()
            log_widget.after(0, lambda: _append_log(log_widget, f"\n[进程结束，退出码 {proc.returncode}]\n"))
            log_widget.after(0, on_finish)

        t = threading.Thread(target=read_loop, daemon=True)
        t.start()

    # 在子线程中启动 Popen，避免阻塞
    def start_proc() -> None:
        log_widget._current_proc = None  # type: ignore
        on_start()
        threading.Thread(target=run, daemon=True).start()

    # 先绑定 _current_proc 再启动
    if not hasattr(log_widget, "_current_proc"):
        log_widget._current_proc = None  # type: ignore
    start_proc()


def _append_log(log_widget: "tk.Text", text: str) -> None:
    log_widget.insert(tk.END, text)
    log_widget.see(tk.END)


def _kill_current(log_widget: "tk.Text") -> None:
    p = getattr(log_widget, "_current_proc", None)
    if p is not None and p.poll() is None:
        p.terminate()
        _append_log(log_widget, "\n[已请求停止]\n")


def _run_subprocess_console(base_dir: Path, exe_name: str) -> int:
    """无 tkinter 时在控制台前台执行子进程并实时输出。"""
    exe_path = base_dir / exe_name
    if not exe_path.exists():
        print(f"错误：未找到 {exe_path}", file=sys.stderr)
        return 1
    try:
        proc = subprocess.Popen([str(exe_path)], cwd=str(base_dir), env=os.environ.copy())
        return proc.wait()
    except Exception as exc:
        print(f"启动失败: {exc}", file=sys.stderr)
        return 1


def _run_cli_launcher(base_dir: Path) -> int:
    """tkinter 不可用时，提供命令行菜单作为兜底启动器。"""
    main_exe = "main_worker.exe" if sys.platform == "win32" else "main_worker"
    uploader_exe = "uploader_worker.exe" if sys.platform == "win32" else "uploader_worker"
    print("检测到 tkinter 不可用，已切换到命令行模式。")
    print(f"运行目录: {base_dir}")
    while True:
        print("\n请选择要执行的任务：")
        print("1) 运行主流程（抓取下载）")
        print("2) 运行本地同步（上传到飞书）")
        print("0) 退出")
        try:
            choice = input("请输入选项: ").strip()
        except EOFError:
            return 0
        if choice == "1":
            code = _run_subprocess_console(base_dir, main_exe)
            print(f"[主流程结束，退出码 {code}]")
        elif choice == "2":
            code = _run_subprocess_console(base_dir, uploader_exe)
            print(f"[本地同步结束，退出码 {code}]")
        elif choice == "0":
            return 0
        else:
            print("无效选项，请重新输入。")


def main() -> None:
    base_dir = runtime_base_dir()
    _ensure_playwright_env(base_dir)

    if tk is None:
        code = _run_cli_launcher(base_dir)
        if getattr(sys, "frozen", False):
            _pause_before_exit("启动器已退出。")
        sys.exit(code)

    root = tk.Tk()
    root.title("自动检测下载 - 启动器")
    root.geometry("700x420")
    root.minsize(400, 300)

    main_exe = "main_worker.exe"
    uploader_exe = "uploader_worker.exe"
    if sys.platform != "win32":
        main_exe = "main_worker"
        uploader_exe = "uploader_worker"

    log = scrolledtext.ScrolledText(root, wrap=tk.WORD, height=16)
    log.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)
    log._current_proc = None  # type: ignore

    btn_frame = tk.Frame(root)
    btn_frame.pack(fill=tk.X, padx=6, pady=4)

    btn_main = tk.Button(btn_frame, text="运行主流程（抓取下载）", width=22, state=tk.NORMAL)
    btn_upload = tk.Button(btn_frame, text="运行本地同步（上传到飞书）", width=24, state=tk.NORMAL)
    btn_stop = tk.Button(btn_frame, text="停止当前任务", width=14, state=tk.DISABLED)

    def set_buttons_running(running: bool) -> None:
        state = tk.DISABLED if running else tk.NORMAL
        btn_main.config(state=state)
        btn_upload.config(state=state)
        btn_stop.config(state=tk.NORMAL if running else tk.DISABLED)

    def on_start() -> None:
        set_buttons_running(True)
        _append_log(log, "\n--- 已启动，输出如下 ---\n")

    def on_finish() -> None:
        root.after(0, lambda: set_buttons_running(False))

    def run_main() -> None:
        _append_log(log, f">>> 启动 {main_exe}\n")
        _run_subprocess(base_dir, main_exe, log, on_start, on_finish)

    def run_uploader() -> None:
        _append_log(log, f">>> 启动 {uploader_exe}\n")
        _run_subprocess(base_dir, uploader_exe, log, on_start, on_finish)

    btn_main.config(command=run_main)
    btn_upload.config(command=run_uploader)
    btn_stop.config(command=lambda: _kill_current(log))

    btn_main.pack(side=tk.LEFT, padx=4)
    btn_upload.pack(side=tk.LEFT, padx=4)
    btn_stop.pack(side=tk.LEFT, padx=4)

    _append_log(log, f"运行目录: {base_dir}\n")
    _append_log(log, "点击上方按钮运行对应任务；输出将显示在下方。\n")

    root.protocol("WM_DELETE_WINDOW", root.quit)
    root.mainloop()


if __name__ == "__main__":
    main()
