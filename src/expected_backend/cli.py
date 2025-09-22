# expected_backend/cli.py
import json, argparse
from .loader import load_tables
from .filter import apply_filters
from .stats_bar import build_expected_payload

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--study", required=True)
    ap.add_argument("--filters", required=True, help="path to filters.json")
    args = ap.parse_args()
    data = json.loads(open(args.filters).read())
    dfs = load_tables(args.study)
    filtered = apply_filters(dfs, data.get("filters", {}))
    expected = build_expected_payload(dfs, filtered)
    print(json.dumps({"study": args.study, "filters": data.get("filters", {}), "expected": expected}, indent=2))

if __name__ == "__main__":
    main()


# import json, argparse
# from pathlib import Path
# from .loader import load_tables
# from .filter import apply_filters
# from .stats_bar import stat_bar

# def main():
#     ap = argparse.ArgumentParser()
#     ap.add_argument("--study", required=True)
#     ap.add_argument("--filters", required=True, help="path to filters.json")
#     ap.add_argument("--data_dir", default="data")
#     ap.add_argument("--out", required=True)
#     args = ap.parse_args()

#     filt = json.loads(Path(args.filters).read_text())
#     dfs = load_tables(args.study, Path(args.data_dir))
#     part = apply_filters(dfs, filt.get("filters", {}))

#     # counts/ids (extend to proper joins)
#     expected = {
#       "count": int(part["participant_id"].nunique()),
#       "ids": sorted(part["participant_id"].astype(str).unique().tolist()),
#       "stats": {
#         "participants": int(part["participant_id"].nunique()),
#         "samples": int(dfs["sample"]["sample_id"].nunique()),
#         "files": int(dfs["file"]["file_id"].nunique())
#       },
#       "statBar": stat_bar(part, dfs["sample"], dfs["file"])
#     }

#     payload = {"study": args.study, "filters": filt.get("filters", {}), "expected": expected}
#     Path(args.out).write_text(json.dumps(payload, indent=2))

# if __name__ == "__main__": main()
