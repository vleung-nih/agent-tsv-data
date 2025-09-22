from __future__ import annotations
import json
from datetime import datetime, timezone

from expected_backend.loader import load_tables
from expected_backend.filter import apply_filters
from expected_backend.stats_bar import build_expected_payload

def handler(event, context):
    try:
        body = event.get("body")
        if isinstance(body, str):
            body = json.loads(body)
        elif not body:
            body = event

        study = body["study"]
        filters = body.get("filters", {})

        dfs = load_tables(study)
        filtered = apply_filters(dfs, filters)
        expected = build_expected_payload(dfs, filtered)

        out = {
            "study": study,
            "filters": filters,
            "expected": expected,
            "meta": {"source": "tsv", "ts": datetime.now(timezone.utc).isoformat()},
        }
        return {"statusCode": 200, "headers": {"Content-Type": "application/json"}, "body": json.dumps(out)}
    except Exception as e:
        return {"statusCode": 500, "headers": {"Content-Type": "application/json"}, "body": json.dumps({"error": str(e)})}


# from __future__ import annotations

# import io
# import json
# import os
# from datetime import datetime, timezone
# from typing import Any, Dict, List, Optional, Set, Tuple

# import boto3
# import pandas as pd

# # ---------- Environment (matches template.yaml) ----------
# DATA_BUCKET = os.environ["DATA_BUCKET"]
# DATA_PREFIX = os.environ.get("DATA_PREFIX", "tsv/")

# s3 = boto3.client("s3")

# # ---------- Column name candidates ----------
# # participant/case id across tables
# PID_CANDIDATES = [
#     "case_record_id",
#     "case.case_record_id",
#     "participant_id",
#     "case_id",
#     "submitter_id",
#     "id",
# ]

# # sample/file primary ids (for uniqueness)
# SAMPLE_ID_CANDS = ["sample_id", "aliquot_id", "sample_submitter_id", "id"]
# FILE_ID_CANDS = ["uuid", "file_id", "object_id", "file_submitter_id", "file_name", "id"]

# # sample/file -> which column might carry the participant/case id (pid)
# SAMPLE_PID_CANDS = PID_CANDIDATES + ["case_submitter_id", "case.submitter_id"]
# FILE_PID_CANDS = PID_CANDIDATES + ["case_submitter_id", "case.submitter_id"]

# # diagnosis disease term columns
# DISEASE_TERM_CANDS = ["disease_term", "primary_diagnosis", "diagnosis"]

# # columns that link files -> samples
# FILE_SAMPLE_FK_CANDS = ["sample.sample_id", "sample_id"]



# # ---------- S3 / TSV helpers ----------
# def _s3_read_tsv(bucket: str, key: str) -> pd.DataFrame:
#     obj = s3.get_object(Bucket=bucket, Key=key)
#     # Keep everything as string to avoid dtype surprises
#     return pd.read_csv(
#         io.BytesIO(obj["Body"].read()),
#         sep="\t",
#         dtype=str,
#         keep_default_na=False,
#         low_memory=False,
#     )


# def _read_tsv(study: str, table: str) -> Optional[pd.DataFrame]:
#     key = f"{DATA_PREFIX}{study}-{table}.tsv"
#     try:
#         return _s3_read_tsv(DATA_BUCKET, key)
#     except Exception:
#         return None


# # ---------- DataFrame utilities ----------
# def _first_col(df: Any, cands: List[str]) -> Optional[str]:
#     if df is None or getattr(df, "empty", True):
#         return None
#     for c in cands:
#         if c in df.columns:
#             return c
#     return None


# def _add_pid_from(df: Any, pid_candidates: List[str]) -> Any:
#     """Ensure df has a 'pid' column derived from the first matching candidate."""
#     if df is None or getattr(df, "empty", True):
#         return df
#     col = _first_col(df, pid_candidates)
#     if col and "pid" not in df.columns:
#         df = df.copy()
#         df["pid"] = df[col].astype(str)
#     return df


# def _unique_count(df: Any, cands: List[str]) -> int:
#     if df is None or getattr(df, "empty", True):
#         return 0
#     col = _first_col(df, cands)
#     return int(df[col].nunique()) if col else 0


# def _unique_ids(df: Any, cands: List[str], limit: int = 2000) -> List[str]:
#     if df is None or getattr(df, "empty", True):
#         return []
#     col = _first_col(df, cands)
#     if not col:
#         return []
#     return sorted(df[col].astype(str).unique().tolist())[:limit]


# # ---------- Load & normalize ----------
# def load_tables(study: str) -> Dict[str, Any]:
#     dfs = {
#         "case": _read_tsv(study, "case"),
#         "demographic": _read_tsv(study, "demographic"),
#         "diagnosis": _read_tsv(study, "diagnosis"),
#         "sample": _read_tsv(study, "sample"),
#         "file": _read_tsv(study, "file"),
#     }

#     # add unified 'pid' to participant-level tables
#     for k in ("case", "demographic", "diagnosis"):
#         dfs[k] = _add_pid_from(dfs[k], PID_CANDIDATES)

#     # also try to add 'pid' to sample/file (if they carry a participant/case id)
#     dfs["sample"] = _add_pid_from(dfs["sample"], SAMPLE_PID_CANDS)
#     dfs["file"] = _add_pid_from(dfs["file"], FILE_PID_CANDS)

#     return dfs


# # ---------- Filtering (participant-level) ----------
# def apply_filters(dfs: Dict[str, Any], filters: Dict[str, Any]) -> pd.DataFrame:
#     case = dfs.get("case")
#     demo = dfs.get("demographic")
#     diag = dfs.get("diagnosis")

#     # choose base table (prefer case)
#     base = case if (case is not None and "pid" in case.columns) else None
#     if base is None and demo is not None and "pid" in demo.columns:
#         base = demo
#     if base is None:
#         return pd.DataFrame()

#     filtered = base.copy()

#     # bring breed/sex from demographic if missing
#     if demo is not None and "pid" in demo.columns:
#         to_bring = [c for c in ("breed", "sex") if c in demo.columns and c not in filtered.columns]
#         if to_bring:
#             filtered = filtered.merge(demo[["pid"] + to_bring], on="pid", how="left")

#     # Breed
#     if "Breed" in filters and "breed" in filtered.columns:
#         vals = [str(x) for x in filters["Breed"]]
#         filtered = filtered[filtered["breed"].isin(vals)]

#     # Sex
#     if "Sex" in filters and "sex" in filtered.columns:
#         vals = [str(x) for x in filters["Sex"]]
#         filtered = filtered[filtered["sex"].isin(vals)]

#     # Diagnosis.disease_term
#     if "Diagnosis.disease_term" in filters and diag is not None and "pid" in diag.columns:
#         term_col = _first_col(diag, DISEASE_TERM_CANDS)
#         if term_col:
#             terms = set(str(x) for x in filters["Diagnosis.disease_term"])
#             keep_pids = set(diag[diag[term_col].isin(terms)]["pid"].astype(str).tolist())
#             filtered = filtered[filtered["pid"].astype(str).isin(keep_pids)]

#     return filtered


# # ---------- Stats (filter-aware) ----------
# def _filtered_sample_count(samples: Any, keep_pids: Set[str]) -> int:
#     if samples is None or getattr(samples, "empty", True) or not keep_pids:
#         return 0
#     if "pid" not in samples.columns:
#         return 0
#     samp_df = samples[samples["pid"].astype(str).isin(keep_pids)]
#     return _unique_count(samp_df, SAMPLE_ID_CANDS)


# def _split_files_case_study(files: Any, samples: Any, keep_pids: Set[str]) -> Tuple[int, int]:
#     """
#     Return (case_files_count, study_files_count).
#     Case files: files whose sample FK points to a sample whose pid ∈ keep_pids.
#     Study files: files with no sample FK (missing/empty).
#     """
#     if files is None or getattr(files, "empty", True):
#         return (0, 0)

#     # Build the filtered set of sample_ids tied to the kept pids
#     keep_sample_ids: Set[str] = set()
#     if samples is not None and not getattr(samples, "empty", True) and keep_pids:
#         samp_pid_col = "pid" if "pid" in samples.columns else None
#         samp_id_col = _first_col(samples, ["sample_id", "aliquot_id", "sample_submitter_id", "id"])
#         if samp_pid_col and samp_id_col:
#             keep_sample_ids = set(
#                 samples[samples[samp_pid_col].astype(str).isin(keep_pids)][samp_id_col].astype(str).tolist()
#             )

#     fk_col = _first_col(files, FILE_SAMPLE_FK_CANDS)

#     # Case files: files that reference one of the kept sample_ids via FK
#     if fk_col and keep_sample_ids:
#         case_df = files[files[fk_col].astype(str).isin(keep_sample_ids)]
#         case_count = _unique_count(case_df, FILE_ID_CANDS)
#     else:
#         case_count = 0

#     # Study files: files with no FK (empty or NaN)
#     if fk_col:
#         study_df = files[(files[fk_col].astype(str) == "") | (files[fk_col].isna())]
#     else:
#         # no detectable FK at all → conservatively 0 study files
#         study_df = files.iloc[0:0]
#     study_count = _unique_count(study_df, FILE_ID_CANDS)

#     return (case_count, study_count)


# def build_expected_payload(dfs: Dict[str, Any], filtered: pd.DataFrame) -> Dict[str, Any]:
#     # participants
#     part_count = int(filtered["pid"].nunique()) if ("pid" in filtered.columns and not filtered.empty) else 0

#     # ids (prefer human-friendly column if present)
#     id_col = _first_col(filtered, ["case_record_id", "case.case_record_id", "participant_id", "pid"])
#     ids = sorted(filtered[id_col].astype(str).unique().tolist())[:2000] if (id_col and not filtered.empty) else []

#     # filtered pid set
#     keep_pids = set(filtered["pid"].astype(str).tolist()) if ("pid" in filtered.columns and not filtered.empty) else set()

#     # samples (filter-aware via pid)
#     samp_count = _filtered_sample_count(dfs.get("sample"), keep_pids)

#     # files: join through samples → case vs study
#     case_files, study_files = _split_files_case_study(dfs.get("file"), dfs.get("sample"), keep_pids)

#     stats = {
#         "participants": part_count,
#         "samples": samp_count,
#         "files": case_files,        # keep 'files' as Case Files for compatibility
#         "caseFiles": case_files,    # explicit
#         "studyFiles": study_files,  # explicit
#     }

#     stat_bar = {
#         "participants": part_count,
#         "samples": samp_count,
#         "studies": 1,
#     }

#     return {"count": part_count, "ids": ids, "stats": stats, "statBar": stat_bar}


# # ---------- Lambda handler ----------
# def handler(event, context):
#     try:
#         body = event.get("body")
#         if body and isinstance(body, str):
#             body = json.loads(body)
#         elif not body:
#             # support direct-invoke with a raw event
#             body = event

#         study = body["study"]
#         filters = body.get("filters", {})

#         dfs = load_tables(study)
#         filtered = apply_filters(dfs, filters)
#         expected = build_expected_payload(dfs, filtered)

#         out = {
#             "study": study,
#             "filters": filters,
#             "expected": expected,
#             "meta": {"source": "tsv", "ts": datetime.now(timezone.utc).isoformat()},
#         }
#         return {
#             "statusCode": 200,
#             "headers": {"Content-Type": "application/json"},
#             "body": json.dumps(out),
#         }

#     except Exception as e:
#         # Always return JSON on errors to avoid 502/HTML bodies
#         return {
#             "statusCode": 500,
#             "headers": {"Content-Type": "application/json"},
#             "body": json.dumps({"error": str(e)}),
#         }
