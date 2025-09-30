# scripts/list_downloads.py
import os, json
from glob import glob

BASE = "data/reports"

def main():
    if not os.path.exists(BASE):
        print("Nessun download trovato.")
        return
    count = 0
    for root, _, files in os.walk(BASE):
        for fn in files:
            if fn.lower().endswith(".pdf"):
                pdf = os.path.join(root, fn)
                meta = pdf + ".meta.json"
                size = os.path.getsize(pdf)
                info = {}
                if os.path.exists(meta):
                    try:
                        info = json.load(open(meta, encoding="utf-8"))
                    except Exception:
                        pass
                print(f"- {pdf}  ({size/1024/1024:.1f} MB)")
                if info:
                    print(f"    url: {info.get('url')}")
                    print(f"    referer: {info.get('referer')}")
                    print(f"    year: {info.get('suspected_year')}")
                count += 1
    print(f"\nTotale PDF: {count}")

if __name__ == "__main__":
    main()
