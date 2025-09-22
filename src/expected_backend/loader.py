from __future__ import annotations

import io
import os
from typing import Any, Dict, List, Optional

import boto3
import pandas as pd

# -------- Environment (matches template.yaml) --------
DATA_BUCKET = os.environ["DATA_BUCKET"]
DATA_PREFIX = os.environ.get("DATA_PREFIX", "tsv/")

s3 = boto3.client("s3")

# -------- Column candidates (centralized so other modules can import) --------
# participant/case id across tables
PID_CANDIDATES: List[str] = [
    "case_record_id",
    "case.case_record_id",
    "participant_id",
    "case_id",
    "submitter_id",
    "id",
]

# sample/file primary ids (for uniqueness)
SAMPLE_ID_CANDS: List[str] = ["sample_id", "aliquot_id", "sample_submitter_id", "id"]
FILE_ID_CANDS: List[str] = ["uuid", "file_id", "object_id", "file_submitter_id", "file_name", "id"]

# sample/file -> which column might carry the participant/case id (pid)
SAMPLE_PID_CANDS: List[str] = PID_CANDIDATES + ["case_submitter_id", "case.submitter_id"]
FILE_PID_CANDS: List[str] = PID_CANDIDATES + ["case_submitter_id", "case.submitter_id"]

# files -> which column links files to samples
FILE_SAMPLE_FK_CANDS: List[str] = ["sample.sample_id", "sample_id"]

# diagnosis disease term columns
DISEASE_TERM_CANDS: List[str] = ["disease_term", "primary_diagnosis", "diagnosis"]


# -------- S3 / TSV helpers --------
def _s3_read_tsv(bucket: str, key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=bucket, Key=key)
    # Keep everything as string to avoid dtype surprises
    return pd.read_csv(
        io.BytesIO(obj["Body"].read()),
        sep="\t",
        dtype=str,
        keep_default_na=False,
        low_memory=False,
    )


def _read_tsv(study: str, table: str) -> Optional[pd.DataFrame]:
    key = f"{DATA_PREFIX}{study}-{table}.tsv"
    try:
        return _s3_read_tsv(DATA_BUCKET, key)
    except Exception:
        return None


# -------- DataFrame utilities (reused by other modules) --------
def first_col(df: Any, cands: List[str]) -> Optional[str]:
    if df is None or getattr(df, "empty", True):
        return None
    for c in cands:
        if c in df.columns:
            return c
    return None


def add_pid_from(df: Any, pid_candidates: List[str]) -> Any:
    """Ensure df has a 'pid' column derived from the first matching candidate."""
    if df is None or getattr(df, "empty", True):
        return df
    col = first_col(df, pid_candidates)
    if col and "pid" not in df.columns:
        df = df.copy()
        df["pid"] = df[col].astype(str)
    return df


def unique_count(df: Any, cands: List[str]) -> int:
    if df is None or getattr(df, "empty", True):
        return 0
    col = first_col(df, cands)
    return int(df[col].nunique()) if col else 0


def unique_ids(df: Any, cands: List[str], limit: int = 2000) -> List[str]:
    if df is None or getattr(df, "empty", True):
        return []
    col = first_col(df, cands)
    if not col:
        return []
    return sorted(df[col].astype(str).unique().tolist())[:limit]


# -------- Public: load all needed tables with normalized 'pid' --------
def load_tables(study: str) -> Dict[str, Any]:
    dfs: Dict[str, Any] = {
        "case": _read_tsv(study, "case"),
        "demographic": _read_tsv(study, "demographic"),
        "diagnosis": _read_tsv(study, "diagnosis"),
        "sample": _read_tsv(study, "sample"),
        "file": _read_tsv(study, "file"),
    }

    # add unified 'pid' to participant-level tables
    for k in ("case", "demographic", "diagnosis"):
        dfs[k] = add_pid_from(dfs[k], PID_CANDIDATES)

    # also try to add 'pid' to sample/file (if they carry a participant/case id)
    dfs["sample"] = add_pid_from(dfs["sample"], SAMPLE_PID_CANDS)
    dfs["file"] = add_pid_from(dfs["file"], FILE_PID_CANDS)

    return dfs


# from pathlib import Path
# import pandas as pd

# def load_tables(study: str, data_dir: Path):
#     # load only what you need for counts & ids
#     part = pd.read_csv(data_dir / f"{study}-participant.tsv", sep="\t")
#     samp = pd.read_csv(data_dir / f"{study}-sample.tsv", sep="\t")
#     files = pd.read_csv(data_dir / f"{study}-file.tsv", sep="\t")
#     return {"participant": part, "sample": samp, "file": files}
