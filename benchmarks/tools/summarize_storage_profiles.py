"""Extract sampled function shares from saved storage benchmark/profile results.

python benchmarks/tools/summarize_storage_profiles.py RESULT_DIRECTORY
No benchmark is rerun. Complete profiles in a partial experiment can also be read;
consult metadata.json for the experiment's overall verification status.
"""
import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone

from pathlib import Path


def read_rows(path):
    with path.open(newline="", encoding="utf-8") as source:
        return list(csv.DictReader(source))


def summarize_profiles(root):
    root = Path(root).resolve()
    metadata = json.loads((root / "metadata.json").read_text(encoding="utf-8"))
    output = []
    profile_count, sample_count = 0, 0
    for run in metadata["runs"]:
        config = run["config"]
        if config.get("profiled") != "1":
            continue
        if config.get("verification") != "passed":
            raise RuntimeError("refusing an unverified profile")
        backend, mode, operation = config["backend"], "sst" if config["sst"] == "1" else "memory", config["profile_operation"]
        functions = read_rows(root / run["label"] / "functions" / "functions.csv")
        total = sum(int(row["exclusive_samples"]) for row in functions)
        if total == 0:
            raise RuntimeError("profile has no samples; increase --profile-iterations or --profile-flush-keys")
        contract = int(config.get("storage_contract_version", 1))
        matches = contract >= 2 and operation != "flush" and int(config["keys"]) == int(metadata["arguments"]["keys"]) and int(
            config["profile_operations"]) == int(metadata["arguments"]["iterations"])
        profile_count += 1
        sample_count += total
        for row in functions:
            exclusive, inclusive = int(row["exclusive_samples"]), int(row["inclusive_samples"])
            if not 0 <= exclusive <= inclusive <= total:
                raise RuntimeError("invalid function sample counts")
            output.append({"backend": backend, "mode": mode, "operation": operation,
                           "storage_contract_version": contract, "scope": config["profile_scope"],
                           "profile_keys": config["keys"], "profile_operations": config["profile_operations"],
                           "profile_wall_ns": config["profile_elapsed_ns"], "total_samples": total,
                           "sparse_profile": total < 200, "normal_workload_matches": matches,
                           "symbol": row["symbol"], "exclusive_samples": exclusive, "inclusive_samples": inclusive,
                           "exclusive_percent": 100 * exclusive / total, "inclusive_percent": 100 * inclusive / total})
    if not output:
        raise RuntimeError("no complete function profiles found")
    with (root / "function-breakdown.csv").open("w", newline="", encoding="utf-8") as target:
        writer = csv.DictWriter(target, fieldnames=list(output[0]))
        writer.writeheader();
        writer.writerows(output)
    details = {"generated_utc": datetime.now(timezone.utc).isoformat(), "profiles": profile_count,
               "samples": sample_count, "experiment_verification": metadata["verification"],
               "exporter_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
               "schema_version": 2,
               "method": "sampled exclusive/inclusive wall-time shares; inclusive counts overlap; no function duration conversion",
               "removed_columns": ["estimated_exclusive_ns", "estimated_inclusive_ns", "estimate_reference_ns", "normal_reference_ns", "normal_mean_ns"],
               "reason": "separate run means and different workloads cannot establish function durations",
               "flush_scope": "persistence worker, not caller end-to-end flush duration"}
    (root / "function-breakdown.metadata.json").write_text(json.dumps(details, indent=2) + "\n", encoding="utf-8")
    return details


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path)
    args = parser.parse_args()
    result = summarize_profiles(args.results)
    print(f"Exported {result['profiles']} profiles / {result['samples']} samples")
