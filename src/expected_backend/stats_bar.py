from __future__ import annotations

from typing import Any, Dict, Set, Tuple

import pandas as pd

from .loader import (
    first_col,
    unique_count,
    unique_ids,
    PID_CANDIDATES,
    SAMPLE_ID_CANDS,
    FILE_ID_CANDS,
    FILE_SAMPLE_FK_CANDS,
)


def _filtered_sample_count(samples: Any, keep_pids: Set[str]) -> int:
    if samples is None or getattr(samples, "empty", True) or not keep_pids:
        return 0
    if "pid" not in samples.columns:
        return 0
    samp_df = samples[samples["pid"].astype(str).isin(keep_pids)]
    return unique_count(samp_df, SAMPLE_ID_CANDS)


def _split_files_case_study(files: Any, samples: Any, keep_pids: Set[str]) -> Tuple[int, int]:
    """
    Return (case_files_count, study_files_count).
    Case files: files whose sample FK points to a sample whose pid ∈ keep_pids.
    Study files: files with no sample FK (missing/empty).
    """
    if files is None or getattr(files, "empty", True):
        return (0, 0)

    # Build the filtered set of sample_ids tied to the kept pids
    keep_sample_ids: Set[str] = set()
    if samples is not None and not getattr(samples, "empty", True) and keep_pids:
        samp_pid_col = "pid" if "pid" in samples.columns else None
        samp_id_col = first_col(samples, ["sample_id", "aliquot_id", "sample_submitter_id", "id"])
        if samp_pid_col and samp_id_col:
            keep_sample_ids = set(
                samples[samples[samp_pid_col].astype(str).isin(keep_pids)][samp_id_col].astype(str).tolist()
            )

    fk_col = first_col(files, FILE_SAMPLE_FK_CANDS)

    # Case files: files that reference one of the kept sample_ids via FK
    if fk_col and keep_sample_ids:
        case_df = files[files[fk_col].astype(str).isin(keep_sample_ids)]
        case_count = unique_count(case_df, FILE_ID_CANDS)
    else:
        case_count = 0

    # Study files: files with no FK (empty or NaN)
    if fk_col:
        study_df = files[(files[fk_col].astype(str) == "") | (files[fk_col].isna())]
    else:
        # no detectable FK at all → conservatively 0 study files
        study_df = files.iloc[0:0]
    study_count = unique_count(study_df, FILE_ID_CANDS)

    return (case_count, study_count)


def build_expected_payload(dfs: Dict[str, Any], filtered: pd.DataFrame) -> Dict[str, Any]:
    # participants
    part_count = int(filtered["pid"].nunique()) if ("pid" in filtered.columns and not filtered.empty) else 0

    # ids (prefer human-friendly column if present)
    id_col = first_col(filtered, ["case_record_id", "case.case_record_id", "participant_id", "pid"])
    ids = sorted(filtered[id_col].astype(str).unique().tolist())[:2000] if (id_col and not filtered.empty) else []

    # filtered pid set
    keep_pids: Set[str] = set(filtered["pid"].astype(str).tolist()) if ("pid" in filtered.columns and not filtered.empty) else set()

    # samples (filter-aware via pid)
    samp_count = _filtered_sample_count(dfs.get("sample"), keep_pids)

    # files: join through samples → case vs study
    case_files, study_files = _split_files_case_study(dfs.get("file"), dfs.get("sample"), keep_pids)

    stats = {
        "participants": part_count,
        "samples": samp_count,
        "files": case_files,        # keep 'files' as Case Files for compatibility
        "caseFiles": case_files,    # explicit
        "studyFiles": study_files,  # explicit
    }

    stat_bar = {
        "participants": part_count,
        "samples": samp_count,
        "studies": 1,
    }

    return {"count": part_count, "ids": ids, "stats": stats, "statBar": stat_bar}


# def stat_bar(part_df, samp_df, files_df):
#     return {
#         "participants": int(part_df["participant_id"].nunique()),
#         "samples": int(samp_df["sample_id"].nunique()),
#         "studies": 1
#     }
