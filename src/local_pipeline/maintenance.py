"""Parquet storage maintenance and file retention utilities for the local pipeline."""

from pathlib import Path

from common.logger import get_logger

logger = get_logger("local-pipeline-maintenance")


def prune_parquet_directory(dir_path: str | Path, keep_last: int = 3) -> int:
    """Retain only the newest `keep_last` parquet files in a directory, purging older ones.

    Args:
        dir_path: Path to the parquet directory (e.g. data/lakehouse/bronze).
        keep_last: Number of recent parquet part files to keep. Defaults to 3.

    Returns:
        Number of purged files.
    """
    p = Path(dir_path).resolve()
    if not p.is_dir():
        return 0

    # Locate all .parquet part files, ignoring hidden/temporary files
    parquet_files = sorted(
        [f for f in p.glob("*.parquet") if f.is_file() and not f.name.startswith(".")],
        key=lambda f: f.stat().st_mtime,
    )

    if len(parquet_files) <= keep_last:
        return 0

    files_to_delete = parquet_files[:-keep_last]
    purged_count = 0
    for old_file in files_to_delete:
        try:
            old_file.unlink(missing_ok=True)
            purged_count += 1
        except OSError as exc:
            logger.warning("Failed to delete old parquet file %s: %s", old_file, exc)

    if purged_count > 0:
        logger.info(
            "Pruned %s old parquet file(s) from %s (retained newest %s)",
            purged_count,
            p.name,
            keep_last,
        )

    return purged_count

