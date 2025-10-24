import sys, re, yaml
from pathlib import Path

def main(path: str):
    p = Path(path)
    data = yaml.safe_load(p.read_text(encoding="utf-8"))

    ids = set()
    for i, lab in enumerate(data.get("labels", []), 1):
        lid = lab.get("id")
        if not lid:
            print(f"[ERR] label #{i} senza id"); return 1
        if lid in ids:
            print(f"[ERR] id duplicato: {lid}"); return 1
        ids.add(lid)

        cat = lab.get("category")
        if cat not in data.get("categories", {}):
            print(f"[ERR] label {lid}: categoria sconosciuta {cat}"); return 1

        for r in lab.get("regex", []):
            pat = r.get("pattern")
            flags = 0
            for fl in r.get("flags", []):
                if fl.lower() == "i": flags |= re.IGNORECASE
                if fl.lower() == "m": flags |= re.MULTILINE
            try:
                re.compile(pat, flags)
            except re.error as e:
                print(f"[ERR] label {lid}: regex invalida → {e}"); return 1

    print(f"[OK] Tassonomia valida: {len(ids)} label, version={data.get('version')}")
    return 0

if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "analysis_rag/configs/taxonomy.yaml"
    sys.exit(main(path))
