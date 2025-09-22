from __future__ import annotations

from typing import Any, Dict

import pandas as pd

from .loader import first_col, DISEASE_TERM_CANDS


def apply_filters(dfs: Dict[str, Any], filters: Dict[str, Any]) -> pd.DataFrame:
    """
    Participant-level filtering.
    - Base = 'case' if available (with pid), else 'demographic'
    - Bring 'breed'/'sex' from demographic if missing
    - Apply Breed, Sex, Diagnosis.disease_term filters
    """
    case = dfs.get("case")
    demo = dfs.get("demographic")
    diag = dfs.get("diagnosis")

    # choose base table (prefer case)
    base = case if (case is not None and "pid" in case.columns) else None
    if base is None and demo is not None and "pid" in demo.columns:
        base = demo
    if base is None:
        return pd.DataFrame()

    filtered = base.copy()

    # bring breed/sex from demographic if missing
    if demo is not None and "pid" in demo.columns:
        to_bring = [c for c in ("breed", "sex") if c in demo.columns and c not in filtered.columns]
        if to_bring:
            filtered = filtered.merge(demo[["pid"] + to_bring], on="pid", how="left")

    # Breed
    if "Breed" in filters and "breed" in filtered.columns:
        vals = [str(x) for x in filters["Breed"]]
        filtered = filtered[filtered["breed"].isin(vals)]

    # Sex
    if "Sex" in filters and "sex" in filtered.columns:
        vals = [str(x) for x in filters["Sex"]]
        filtered = filtered[filtered["sex"].isin(vals)]

    # Diagnosis.disease_term
    if "Diagnosis.disease_term" in filters and diag is not None and "pid" in diag.columns:
        term_col = first_col(diag, DISEASE_TERM_CANDS)
        if term_col:
            terms = set(str(x) for x in filters["Diagnosis.disease_term"])
            keep_pids = set(diag[diag[term_col].isin(terms)]["pid"].astype(str).tolist())
            filtered = filtered[filtered["pid"].astype(str).isin(keep_pids)]

    return filtered


# def apply_filters(dfs, filters: dict):
#     # TODO: map human filters -> column names; use dotmap spec
#     # e.g., Breed -> participant.breed
#     part = dfs["participant"]
#     # example: filter by breed if present
#     if "Breed" in filters:
#         part = part[part["breed"].isin(filters["Breed"])]
#     return part  # for demo; extend to joins across sample/file
