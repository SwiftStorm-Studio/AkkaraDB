"""Validate scaling CLI, result ownership, checksums and exact work accounting."""
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from run_memtable_scaling_experiment import validate


exe = str(Path(sys.argv[1]).resolve())
parent = Path(sys.argv[2]).resolve()
parent.mkdir(parents=True, exist_ok=True)
with tempfile.TemporaryDirectory(prefix="memtable-scaling-smoke-", dir=parent) as temporary:
    root = Path(temporary)
    for backend in ("skiplist", "bptree", "art", "wormhole"):
        for length in (16, 32, 64, 128):
            destination = root / f"{backend}-{length}"
            cmd = [exe, "--backend", backend, "--keys", "17", "--key-bytes", str(length), "--value-bytes", "8",
                   "--iterations", "37", "--scan-iterations", "11", "--warmup", "3", "--rounds", "2",
                   "--scan-rows", "1,8,17", "--output", str(destination)]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            assert result.returncode == 0, result.stdout + result.stderr
            config, summary = validate(destination)
            assert config["backend"] == backend and int(config["key_bytes"]) == length
            assert len(summary) == 10 and int(config["key_slab_bytes"]) == 18 * length
            original = (destination / "summary.csv").read_bytes()
            assert subprocess.run(cmd, capture_output=True, timeout=30).returncode == 2
            assert (destination / "summary.csv").read_bytes() == original
        for size, counts in ((1, "1"), (35, "1,8,32,33,35"), (257, "1,32,128,257")):
            destination = root / f"{backend}-boundary-{size}"
            result = subprocess.run([exe, "--backend", backend, "--keys", str(size), "--scan-rows", counts,
                                     "--iterations", "9", "--scan-iterations", "7", "--warmup", "0", "--rounds", "2",
                                     "--output", str(destination)], capture_output=True, text=True, timeout=30)
            assert result.returncode == 0, result.stdout + result.stderr
            validate(destination)
    for index, options in enumerate((
        ["--keys", "0"], ["--key-bytes", "15"], ["--key-bytes", "65535"], ["--value-bytes", "7"],
        ["--keys", "17", "--scan-rows", "18"], ["--scan-rows", "1,1"], ["--scan-rows", "0"],
        ["--backend", "other"], ["--rounds", "0"], ["--cpu", "64"],
        ["--keys", "18446744073709551615"], ["--iterations", "18446744073709551615"],
        ["--scan-iterations", "18446744073709551615"], ["--rounds", "18446744073709551615"],
        ["--scan-rows", "1,"], ["--unknown", "1"], ["--output", ""],
    )):
        result = subprocess.run([exe, "--output", str(root / f"invalid-{index}"), *options],
                                capture_output=True, text=True, timeout=10)
        assert result.returncode == 2, (options, result.stdout, result.stderr)
print("MemTable scaling benchmark CLI/accounting smoke: passed")
