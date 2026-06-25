"""
FileProcessor — processes files with progress persistence and interruption recovery.
Progress is saved to a JSON checkpoint file after each file completes.
"""

import os
import json
import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Any

logger = logging.getLogger(__name__)


class FileProcessor:
    """
    Process all files in a directory, saving a checkpoint after each one.
    On restart, already-completed files are skipped automatically.

    Args:
        input_dir      : directory of files to process
        progress_file  : path to the JSON checkpoint file
        process_file_fn: callable(file_path: str) -> any result; raise on error
        retry_limit    : retries per file before marking it failed (default 2)
    """

    def __init__(
        self,
        input_dir: str,
        progress_file: str,
        process_file_fn: Callable[[str], Any],
        retry_limit: int = 2,
    ):
        self.input_dir       = input_dir
        self.progress_file   = Path(progress_file)
        self.process_file_fn = process_file_fn
        self.retry_limit     = retry_limit

    # ── checkpoint helpers ────────────────────────────────────────────────────

    def _load_progress(self) -> dict:
        try:
            if self.progress_file.exists():
                raw = self.progress_file.read_text(encoding="utf-8")
                return json.loads(raw)
        except (json.JSONDecodeError, OSError):
            pass  # corrupt or unreadable → start fresh
        return {
            "completed": {},
            "failed": {},
            "started_at": _now(),
        }

    def _save_progress(self, state: dict) -> None:
        self.progress_file.write_text(
            json.dumps(state, indent=2), encoding="utf-8"
        )

    def _reset_progress(self) -> None:
        if self.progress_file.exists():
            self.progress_file.unlink()

    # ── public API ────────────────────────────────────────────────────────────

    def run(self, *, force_restart: bool = False) -> dict:
        """
        Run (or resume) the processing job.
        Returns a summary dict with keys: total, skipped, processed, failed, errors.
        """
        if force_restart:
            self._reset_progress()

        state = self._load_progress()
        progress_filename = self.progress_file.name

        all_files = sorted(
            f for f in os.listdir(self.input_dir)
            if f != progress_filename
            and os.path.isfile(os.path.join(self.input_dir, f))
        )

        results = {
            "total":      len(all_files),
            "skipped":    0,
            "processed":  0,
            "failed":     0,
            "errors":     [],
            "resumed_from": state.get("started_at"),
        }

        for filename in all_files:
            file_path = os.path.join(self.input_dir, filename)

            # ── already completed in a prior run ──────────────────────────
            if filename in state["completed"]:
                results["skipped"] += 1
                logger.debug("Skipping (already done): %s", filename)
                continue

            # ── attempt with retries ───────────────────────────────────────
            last_error = None
            succeeded  = False

            for attempt in range(1, self.retry_limit + 2):
                try:
                    result = self.process_file_fn(file_path)
                    state["completed"][filename] = {
                        "processed_at": _now(),
                        "attempts":     attempt,
                        "checksum":     _md5(file_path),
                        "result":       result,
                    }
                    state["failed"].pop(filename, None)
                    self._save_progress(state)          # ← persist immediately
                    succeeded = True
                    logger.info("Processed (%d/%d attempt): %s", attempt, self.retry_limit + 1, filename)
                    break
                except Exception as exc:
                    last_error = exc
                    logger.warning("Attempt %d failed for %s: %s", attempt, filename, exc)

            if succeeded:
                results["processed"] += 1
            else:
                state["failed"][filename] = {
                    "failed_at": _now(),
                    "error":     str(last_error),
                    "attempts":  self.retry_limit + 1,
                }
                self._save_progress(state)
                results["failed"] += 1
                results["errors"].append({"file": filename, "error": str(last_error)})
                logger.error("Permanently failed: %s — %s", filename, last_error)

        results["completed_at"] = _now()
        return results

    def get_progress(self) -> dict:
        """Return the current checkpoint state without running anything."""
        return self._load_progress()


# ── helpers ────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _md5(file_path: str) -> str:
    h = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()
