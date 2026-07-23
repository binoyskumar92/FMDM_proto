import os
import glob
import json
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse

try:
    import questionary
    from rich import box
    from rich.console import Console
    from rich.json import JSON as RichJSON
    from rich.live import Live
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
except ImportError:
    print("Missing UI libraries. Please run: pip install questionary rich")
    sys.exit(1)

console = Console()


# ==========================================
# HELPERS
# ==========================================

def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")


def header():
    console.print(Panel(
        Text("FMDM — MongoDB ETL Wizard", style="bold cyan", justify="center"),
        border_style="cyan",
    ))


def mask_uri(uri: str) -> str:
    """Redact password from a MongoDB URI for safe display."""
    try:
        parsed = urlparse(uri)
        if parsed.password:
            safe_netloc = parsed.netloc.replace(f":{parsed.password}@", ":****@")
            return urlunparse(parsed._replace(netloc=safe_netloc))
    except Exception:
        pass
    return uri


def preview_config(config_data: dict):
    """Print config with credentials redacted."""
    safe = dict(config_data)
    for key in ("source_uri", "dest_uri", "bookkeeping_uri"):
        if key in safe:
            safe[key] = mask_uri(safe[key])
    console.print(RichJSON.from_data(safe))


def get_bookkeeping_db(config_data: dict):
    """Return a pymongo DB handle for the bookkeeping cluster, or None on failure."""
    from pymongo import MongoClient
    client = MongoClient(config_data["bookkeeping_uri"], serverSelectionTimeoutMS=4000)
    try:
        client.admin.command("ping")
        return client["masking_control"]
    except Exception:
        client.close()
        return None


def check_connectivity(config_data: dict) -> bool:
    """Ping all three clusters before launching."""
    try:
        from pymongo import MongoClient
    except ImportError:
        console.print("[yellow]pymongo not installed, skipping pre-flight check.[/yellow]")
        return True

    uris = {
        "Source     ": config_data.get("source_uri"),
        "Destination": config_data.get("dest_uri"),
        "Bookkeeping": config_data.get("bookkeeping_uri"),
    }

    console.print("\n[bold]Pre-flight connectivity check[/bold]")
    all_ok = True
    for name, uri in uris.items():
        if not uri:
            console.print(f"  [yellow]⚠  {name}: URI not set[/yellow]")
            all_ok = False
            continue
        client = MongoClient(uri, serverSelectionTimeoutMS=4000)
        try:
            client.admin.command("ping")
            console.print(f"  [green]✓  {name}[/green]")
        except Exception as e:
            console.print(f"  [red]✗  {name}: {e}[/red]")
            all_ok = False
        finally:
            client.close()

    return all_ok


# ==========================================
# RESUME DETECTION
# ==========================================

def check_resume(config_data: dict) -> bool:
    """
    Check if a job with this job_id already exists in the bookkeeping DB.
    If so, show its state and ask the user whether to resume or abort.
    Returns True to proceed, False to abort.
    """
    db = get_bookkeeping_db(config_data)
    if db is None:
        return True  # Can't check — let the ETL handle it

    try:
        job_id = config_data["job_id"]
        existing = db.jobs.find_one({"job_id": job_id})
        if not existing:
            return True  # Fresh job, nothing to warn about

        status = existing.get("status", "UNKNOWN")
        created_at = existing.get("created_at", "unknown")
        total_chunks = existing.get("total_chunks", "?")
        completed = db.chunks.count_documents(
            {"job_id": job_id, "status": {"$in": ["LOADED", "RAW_DELETED"]}}
        )
        failed = db.chunks.count_documents(
            {"job_id": job_id, "status": {"$regex": "FAILED|QUARANTINED"}}
        )

        t = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
        t.add_column(style="dim", min_width=14)
        t.add_column()
        t.add_row("Job ID",    job_id)
        t.add_row("Status",    status)
        t.add_row("Created",   str(created_at))
        t.add_row("Progress",  f"{completed} / {total_chunks} chunks loaded")
        t.add_row("Failed",    f"[red]{failed}[/red]" if failed else "0")

        # Terminal states: job is done, re-running will write 0 docs.
        TERMINAL_STATUSES = {"COMPLETED", "PARTIAL_SUCCESS_WITH_ERRORS"}
        if status in TERMINAL_STATUSES:
            console.print(Panel(
                t,
                title="[bold red]✗  Job already finished[/bold red]",
                border_style="red",
            ))
            console.print(
                f"[bold red]This job is {status}. Re-running with the same job_id will "
                f"write 0 documents — all chunks are already in a terminal state.[/bold red]"
            )
            console.print("[dim]Change job_id in your config file to start a fresh migration.[/dim]\n")
            return False

        console.print(Panel(t, title="[bold yellow]⚠  Existing job found[/bold yellow]", border_style="yellow"))
        console.print("[dim]Same job_id → the job will RESUME from where it left off.[/dim]")
        console.print("[dim]To start fresh, change job_id in your config file.[/dim]\n")

        return bool(questionary.confirm("Resume this job?", default=True).ask())
    finally:
        db.client.close()


# ==========================================
# MASKING AUDIT REPORT
# ==========================================

def show_audit_report(config_data: dict):
    """
    Aggregate field_mask_counts per collection and show per-field coverage
    as a percentage of that collection's docs written.
    """
    db = get_bookkeeping_db(config_data)
    if db is None:
        console.print("[yellow]Could not reach bookkeeping DB — skipping audit report.[/yellow]")
        return

    try:
        job_id = config_data["job_id"]

        # Build expected masking fields per collection from config
        coll_fields: dict[str, list] = {}
        for coll in config_data.get("collections", []):
            fields = coll.get("masking_fields", [])
            if fields:
                coll_fields[coll["source_collection"]] = fields

        if not coll_fields:
            return

        # Aggregate per-collection: docs_written and field_mask_counts
        # coll_stats[src_coll] = {"_docs": int, "<field>": int, ...}
        coll_stats: dict[str, dict] = {}
        for chunk in db.chunks.find(
            {"job_id": job_id, "status": {"$in": ["LOADED", "RAW_DELETED"]}},
            {"field_mask_counts": 1, "docs_written": 1, "source_collection": 1},
        ):
            src = chunk.get("source_collection", "unknown")
            if src not in coll_stats:
                coll_stats[src] = {"_docs": 0}
            coll_stats[src]["_docs"] += chunk.get("docs_written", 0)
            for field, count in chunk.get("field_mask_counts", {}).items():
                coll_stats[src][field] = coll_stats[src].get(field, 0) + count

        if not coll_stats:
            console.print("[yellow]No completed chunks found for audit report.[/yellow]")
            return

        t = Table(box=box.SIMPLE_HEAVY, show_header=True, padding=(0, 2))
        t.add_column("Collection", style="dim")
        t.add_column("Field", style="cyan")
        t.add_column("Docs Masked", justify="right")
        t.add_column("Coverage", justify="right")

        total_docs_all = 0
        for src_coll in sorted(coll_stats.keys()):
            stats = coll_stats[src_coll]
            coll_docs = stats["_docs"]
            total_docs_all += coll_docs
            for field in sorted(coll_fields.get(src_coll, [])):
                count = stats.get(field, 0)
                pct = count / coll_docs * 100 if coll_docs else 0
                color = "green" if pct >= 99.9 else "yellow" if pct > 0 else "red"
                t.add_row(src_coll, field, f"{count:,}", f"[{color}]{pct:.1f}%[/{color}]")

        console.print(Panel(t, title="[bold]Masking Audit Report[/bold]", border_style="cyan"))
        console.print(f"[dim]Total docs written: {total_docs_all:,}[/dim]")
    finally:
        db.client.close()


# ==========================================
# JOB HISTORY
# ==========================================

def show_job_history(config_data: dict):
    """List the 10 most recent jobs from the bookkeeping DB."""
    db = get_bookkeeping_db(config_data)
    if db is None:
        console.print("[red]Could not connect to bookkeeping DB.[/red]")
        return

    try:
        jobs = list(db.jobs.find({}, sort=[("created_at", -1)], limit=10))
        if not jobs:
            console.print("[yellow]No jobs found in the bookkeeping database.[/yellow]")
            return

        t = Table(box=box.SIMPLE_HEAVY, show_header=True, padding=(0, 2))
        t.add_column("Job ID", style="cyan")
        t.add_column("Mode")
        t.add_column("Status")
        t.add_column("Chunks")
        t.add_column("Docs Written", justify="right")
        t.add_column("Created")

        for job in jobs:
            job_id   = job.get("job_id", "?")
            mode     = job.get("mode", "?")
            status   = job.get("status", "?")
            chunks   = str(job.get("total_chunks", "?"))
            docs     = job.get("total_docs_written", 0)
            created  = job.get("created_at")

            if "COMPLETED" in status:
                color = "green"
            elif status in ("RUNNING", "PLANNING"):
                color = "cyan"
            else:
                color = "yellow"

            t.add_row(
                job_id,
                mode,
                f"[{color}]{status}[/{color}]",
                chunks,
                f"{docs:,}" if isinstance(docs, int) else "—",
                created.strftime("%Y-%m-%d %H:%M") if created else "?",
            )

        console.print(Panel(t, title="[bold]Recent Jobs[/bold]", border_style="cyan"))
    finally:
        db.client.close()


# ==========================================
# STOP JOB
# ==========================================

def stop_job(config_data: dict):
    """
    Stop a running ETL job by sending SIGTERM to its process group.
    PID is stored in the wizard_sessions collection when the job is launched.
    Killing the process group ensures all worker subprocesses are also terminated.
    """
    db = get_bookkeeping_db(config_data)
    if db is None:
        console.print("[red]Could not connect to bookkeeping DB.[/red]")
        return

    try:
        job_id = config_data["job_id"]
        session = db.wizard_sessions.find_one({"job_id": job_id})

        if not session or not session.get("etl_pid"):
            console.print(f"[yellow]No PID found for job '{job_id}'.[/yellow]")
            console.print("[dim]This job may not have been launched from this wizard, or the record was cleared.[/dim]")
            return

        pid = session["etl_pid"]

        # Verify the process is actually running before asking for confirmation
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            console.print(f"[yellow]Process {pid} is not running — job may have already finished or crashed.[/yellow]")
            job = db.jobs.find_one({"job_id": job_id}, {"status": 1})
            if job:
                console.print(f"[dim]Last known status: {job.get('status', 'UNKNOWN')}[/dim]")
            return
        except PermissionError:
            pass  # Process exists, permission issue only for signal 0 — proceed

        confirmed = questionary.confirm(
            f"Stop job '{job_id}' (PID: {pid})? In-progress chunks will be reset to READY on next resume.",
            default=False,
        ).ask()
        if not confirmed:
            return

        try:
            pgid = os.getpgid(pid)
            os.killpg(pgid, signal.SIGTERM)
            console.print(f"[green]✓ Sent SIGTERM to process group {pgid} (ETL + all workers)[/green]")

            db.jobs.update_one(
                {"job_id": job_id},
                {"$set": {"status": "STOPPED_BY_USER", "stopped_at": datetime.now(timezone.utc)}},
            )
            db.wizard_sessions.update_one(
                {"job_id": job_id},
                {"$set": {"etl_pid": None, "stopped_at": datetime.now(timezone.utc)}},
            )
            console.print("[dim]Status updated to STOPPED_BY_USER. Resume anytime using the same job_id.[/dim]")

        except Exception as e:
            console.print(f"[red]Failed to stop job: {e}[/red]")
    finally:
        db.client.close()


# ==========================================
# LIVE STATUS PANEL
# ==========================================

def build_status_panel(db, job_id: str, start_time: float, running=None) -> Panel:
    """
    Query bookkeeping DB and render a status panel.
    running=True/False: known from process state.
    running=None: infer from active chunk states (used in monitor-only mode).
    """
    try:
        dist = {
            doc["_id"]: doc["count"]
            for doc in db.chunks.aggregate([
                {"$match": {"job_id": job_id}},
                {"$group": {"_id": "$status", "count": {"$sum": 1}}},
            ])
        }

        total      = sum(dist.values())
        loaded     = dist.get("LOADED", 0) + dist.get("RAW_DELETED", 0)
        in_prog    = sum(v for k, v in dist.items() if k in {"STREAMING", "DUMPING", "LOADING"})
        remaining  = sum(v for k, v in dist.items() if k in {"READY", "READY_TO_DUMP"})
        failed     = sum(v for k, v in dist.items() if "FAILED" in k)
        quarantine = sum(v for k, v in dist.items() if "QUARANTINED" in k)

        if running is None:
            running = (in_prog + remaining) > 0

        docs_agg = list(db.chunks.aggregate([
            {"$match": {"job_id": job_id, "status": {"$in": ["LOADED", "RAW_DELETED"]}}},
            {"$group": {"_id": None, "total": {"$sum": "$docs_written"}}},
        ]))
        total_docs = docs_agg[0]["total"] if docs_agg else 0

        if total == 0:
            bar = "[cyan]Planning chunks...[/cyan]" if running else "[yellow]No chunks found — job may have done nothing[/yellow]"
        else:
            pct    = loaded / total * 100
            filled = int(pct / 5)
            bar    = f"[{'█' * filled}{'░' * (20 - filled)}] {loaded}/{total} ({pct:.1f}%)"

        elapsed     = time.time() - start_time
        elapsed_str = f"{int(elapsed // 3600):02d}:{int((elapsed % 3600) // 60):02d}:{int(elapsed % 60):02d}"
        status_str  = "[bold green]RUNNING[/bold green]" if running else "[bold cyan]COMPLETE[/bold cyan]"

        t = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
        t.add_column(style="dim", min_width=14)
        t.add_column()

        t.add_row("Status",       status_str)
        t.add_row("Elapsed",      elapsed_str)
        t.add_row("Progress",     bar)
        t.add_row("In Progress",  str(in_prog))
        t.add_row("Remaining",    str(remaining))
        t.add_row("Failed",       f"[red]{failed}[/red]" if failed else "0")
        t.add_row("Quarantined",  f"[red]{quarantine}[/red]" if quarantine else "0")
        t.add_row("Docs Written", f"{total_docs:,}")
        t.add_row("Last Refresh", datetime.now().strftime("%H:%M:%S"))

        border = "cyan" if running else "green"
        return Panel(t, title=f"[bold cyan]{job_id}[/bold cyan]", border_style=border)

    except Exception as e:
        return Panel(f"[red]Could not reach bookkeeping DB: {e}[/red]", border_style="red")


def monitor_job(config_data: dict, proc=None, poll_interval: int = 10):
    """
    Live status loop. If proc is given, exits when the process finishes.
    Pass proc=None to monitor a job that's already running elsewhere.
    Press Ctrl+C to exit monitoring without killing the ETL.
    """
    try:
        from pymongo import MongoClient
    except ImportError:
        console.print("[red]pymongo required for live monitoring.[/red]")
        return

    job_id    = config_data["job_id"]
    bk_client = MongoClient(config_data["bookkeeping_uri"])
    db        = bk_client["masking_control"]
    start_time = time.time()

    console.print(f"\n[dim]Refreshing every {poll_interval}s — Ctrl+C to exit monitor (ETL keeps running)[/dim]\n")

    try:
        with Live(console=console, refresh_per_second=0.2) as live:
            while True:
                if proc is not None:
                    running = proc.poll() is None
                    live.update(build_status_panel(db, job_id, start_time, running))
                    if not running:
                        time.sleep(1)
                        live.update(build_status_panel(db, job_id, start_time, False))
                        break
                else:
                    live.update(build_status_panel(db, job_id, start_time, None))

                time.sleep(poll_interval)

    except KeyboardInterrupt:
        console.print("\n[yellow]Exited monitor. ETL job continues in the background.[/yellow]")
        if proc:
            console.print(f"[dim]PID: {proc.pid}[/dim]")

    finally:
        bk_client.close()


# ==========================================
# LAUNCH
# ==========================================

def launch_job(selected_config: str, config_data: dict, env_vars: dict):
    job_id   = config_data["job_id"]
    log_path = f"fmdm_{job_id}.log"

    console.print(f"\n[bold cyan]Launching ETL...[/bold cyan]")
    console.print(f"[dim]Logs → {log_path}[/dim]\n")

    with open(log_path, "w") as log_file:
        proc = subprocess.Popen(
            [sys.executable, "etl_benchmark.py", "--config", selected_config],
            env=env_vars,
            stdout=log_file,
            stderr=log_file,
            start_new_session=True,  # Detach from terminal process group so Ctrl+C doesn't kill ETL
        )

    # Store PID in bookkeeping DB so the stop feature can find it later.
    # Uses a separate wizard_sessions collection to avoid interfering with ETL bookkeeping.
    try:
        db = get_bookkeeping_db(config_data)
        if db is not None:
            try:
                db.wizard_sessions.update_one(
                    {"job_id": job_id},
                    {"$set": {"etl_pid": proc.pid, "started_at": datetime.now(timezone.utc), "log": log_path}},
                    upsert=True,
                )
            finally:
                db.client.close()
    except Exception:
        pass  # Non-critical — job still runs fine without this

    monitor_job(config_data, proc=proc)

    # If proc is still running, the user exited the monitor early (Ctrl+C).
    # Don't block on proc.wait() — just report the PID and return to menu.
    if proc.poll() is None:
        console.print(f"\n[dim]Job running in background — PID: {proc.pid} | Logs: {log_path}[/dim]")
        return

    if proc.returncode == 0:
        docs_written = 0
        db = get_bookkeeping_db(config_data)
        if db is not None:
            try:
                agg = list(db.chunks.aggregate([
                    {"$match": {"job_id": job_id, "status": {"$in": ["LOADED", "RAW_DELETED"]}}},
                    {"$group": {"_id": None, "total": {"$sum": "$docs_written"}}},
                ]))
                docs_written = agg[0]["total"] if agg else 0
            finally:
                db.client.close()

        if docs_written == 0:
            console.print("\n[bold yellow]⚠  Job exited cleanly but wrote 0 documents.[/bold yellow]")
            console.print("[dim]All chunks may already be in a terminal state from a previous run.[/dim]")
            console.print("[dim]Change job_id in your config file to start a fresh migration.[/dim]\n")
        else:
            console.print("\n[bold green]✓ Job completed successfully.[/bold green]\n")
            show_audit_report(config_data)
    else:
        console.print(f"\n[bold red]✗ ETL exited with code {proc.returncode}. Check {log_path}[/bold red]")


# ==========================================
# MAIN
# ==========================================

def main():
    clear_screen()
    header()

    while True:
        console.print()
        action = questionary.select(
            "What would you like to do?",
            choices=[
                "Launch / Resume a job",
                "Monitor an existing job",
                "Stop a running job",
                "View job history",
                "Dry run (connectivity check only)",
                "Quit",
            ],
            pointer="➜",
            style=questionary.Style([("pointer", "fg:cyan bold")]),
        ).ask()

        if not action or action == "Quit":
            break

        # Config selection (exclude the sample file)
        json_files = sorted(f for f in glob.glob("*.json") if f != "config_sample.json")
        if not json_files:
            console.print("[bold red]No .json config files found in the current directory.[/bold red]")
            continue

        selected_config = questionary.select(
            "Select a config file:",
            choices=json_files,
            pointer="➜",
            style=questionary.Style([("pointer", "fg:cyan bold")]),
        ).ask()

        if not selected_config:
            continue

        try:
            with open(selected_config) as f:
                config_data = json.load(f)
        except Exception as e:
            console.print(f"[bold red]Failed to read {selected_config}: {e}[/bold red]")
            continue

        console.print(f"\n[bold green]{selected_config}[/bold green]")
        preview_config(config_data)

        if action == "Dry run (connectivity check only)":
            check_connectivity(config_data)
            continue

        if action == "View job history":
            show_job_history(config_data)
            continue

        if action == "Monitor an existing job":
            monitor_job(config_data)
            continue

        if action == "Stop a running job":
            stop_job(config_data)
            continue

        # --- Launch flow ---

        # Allow overriding job_id before anything else
        current_job_id = config_data.get("job_id", "")
        new_job_id = questionary.text(
            "Job ID (edit to override, Enter to keep):",
            default=current_job_id,
        ).ask()
        if not new_job_id:
            continue
        new_job_id = new_job_id.strip()
        config_data["job_id"] = new_job_id

        # Persist the new job_id back to the config file so it survives restarts.
        if new_job_id != current_job_id:
            try:
                with open(selected_config, "w") as f:
                    json.dump(config_data, f, indent=4)
                console.print(f"[dim]job_id updated in {selected_config}[/dim]")
            except Exception as e:
                console.print(f"[yellow]Could not save job_id to config: {e}[/yellow]")

        # Resume detection
        if not check_resume(config_data):
            continue

        # Connectivity
        ok = check_connectivity(config_data)
        if not ok:
            proceed = questionary.confirm(
                "One or more connections failed. Launch anyway?", default=False
            ).ask()
            if not proceed:
                continue

        # Salt
        env_vars = os.environ.copy()
        if not env_vars.get("FMDM_HASH_SALT"):
            console.print("\n[bold yellow]FMDM_HASH_SALT is not set.[/bold yellow]")
            salt = questionary.password("Enter masking salt:").ask()
            if not salt:
                console.print("[bold red]Salt required.[/bold red]")
                continue
            env_vars["FMDM_HASH_SALT"] = salt

        confirm = questionary.confirm(
            f"Launch job '{config_data.get('job_id')}'?", default=False
        ).ask()
        if not confirm:
            continue

        clear_screen()
        header()
        launch_job(selected_config, config_data, env_vars)

    console.print("[dim]Goodbye.[/dim]")


if __name__ == "__main__":
    main()
