"""Phase 22 verification: Function registry persistence.

Tests:
1. Register function → JSON file written with correct content
2. Restart (new registry from same dir) → functions auto-restored
3. Unregister function → JSON updated (function removed)
4. Invalid module in JSON → skipped without blocking other functions

Usage:
    python3 python/tests/verify_phase22_persistence.py
"""

import json
import os
import shutil
import sys
import tempfile

# Ensure project root is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

PASS = 0
FAIL = 0


def report(name: str, ok: bool, detail: str = ""):
    global PASS, FAIL
    status = "PASS" if ok else "FAIL"
    if ok:
        PASS += 1
    else:
        FAIL += 1
    msg = f"[{status}] {name}"
    if detail:
        msg += f" — {detail}"
    print(msg)


def test_register_writes_json(tmp_dir: str):
    """Test 1: Register a function and verify JSON file is written."""
    from starrocks.coordinator.function_registry import FunctionRegistry

    registry = FunctionRegistry(persist_dir=tmp_dir)
    # Register a function from the standard library
    registry.register("my_len", "builtins", "len")

    json_path = os.path.join(tmp_dir, "functions.json")
    if not os.path.exists(json_path):
        report("Register writes JSON", False, "JSON file not created")
        return

    data = json.loads(open(json_path).read())
    funcs = data.get("functions", {})
    ok = ("my_len" in funcs
          and funcs["my_len"]["module_path"] == "builtins"
          and funcs["my_len"]["callable_name"] == "len")
    report("Register writes JSON", ok,
           f"content={json.dumps(funcs)}" if not ok else "")


def test_restart_restores_functions(tmp_dir: str):
    """Test 2: New registry from same dir restores functions."""
    from starrocks.coordinator.function_registry import FunctionRegistry

    # First: register
    r1 = FunctionRegistry(persist_dir=tmp_dir)
    r1.register("my_len", "builtins", "len")
    r1.register("my_abs", "builtins", "abs")

    # Second: new registry, same dir — should auto-load
    r2 = FunctionRegistry(persist_dir=tmp_dir)
    restored = r2.load_persisted()

    funcs = r2.list_functions()
    ok = (restored == 2
          and "my_len" in funcs
          and "my_abs" in funcs)
    report("Restart restores functions", ok,
           f"restored={restored}, funcs={list(funcs.keys())}")


def test_unregister_updates_json(tmp_dir: str):
    """Test 3: Unregister removes function from JSON."""
    from starrocks.coordinator.function_registry import FunctionRegistry

    registry = FunctionRegistry(persist_dir=tmp_dir)
    registry.register("fn_a", "builtins", "len")
    registry.register("fn_b", "builtins", "abs")
    registry.unregister("fn_a")

    json_path = os.path.join(tmp_dir, "functions.json")
    data = json.loads(open(json_path).read())
    funcs = data.get("functions", {})
    ok = "fn_a" not in funcs and "fn_b" in funcs
    report("Unregister updates JSON", ok,
           f"remaining={list(funcs.keys())}")


def test_invalid_module_skipped(tmp_dir: str):
    """Test 4: Invalid module in JSON is skipped, other functions still load."""
    from starrocks.coordinator.function_registry import FunctionRegistry

    # Write a JSON with one valid and one invalid function
    json_path = os.path.join(tmp_dir, "functions.json")
    os.makedirs(tmp_dir, exist_ok=True)
    with open(json_path, "w") as f:
        json.dump({
            "functions": {
                "good_fn": {
                    "module_path": "builtins",
                    "callable_name": "len",
                },
                "bad_fn": {
                    "module_path": "nonexistent_module_xyz_12345",
                    "callable_name": "no_func",
                },
            }
        }, f)

    registry = FunctionRegistry(persist_dir=tmp_dir)
    restored = registry.load_persisted()

    funcs = registry.list_functions()
    ok = (restored == 1
          and "good_fn" in funcs
          and "bad_fn" not in funcs)
    report("Invalid module skipped", ok,
           f"restored={restored}, funcs={list(funcs.keys())}")


def main():
    print("=" * 60)
    print("Phase 22: Function Registry Persistence Verification")
    print("=" * 60)

    # Each test gets its own temp directory
    base_tmp = tempfile.mkdtemp(prefix="phase22_")
    try:
        test_register_writes_json(os.path.join(base_tmp, "t1"))
        test_restart_restores_functions(os.path.join(base_tmp, "t2"))
        test_unregister_updates_json(os.path.join(base_tmp, "t3"))
        test_invalid_module_skipped(os.path.join(base_tmp, "t4"))
    finally:
        shutil.rmtree(base_tmp, ignore_errors=True)

    print("=" * 60)
    print(f"Results: {PASS} PASS, {FAIL} FAIL out of {PASS + FAIL}")
    print("=" * 60)
    sys.exit(0 if FAIL == 0 else 1)


if __name__ == "__main__":
    main()
