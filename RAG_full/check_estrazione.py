from __future__ import annotations

from pathlib import Path
import pandas as pd

''' Questo file serve semplicemente per verificare la presenza di file md  nelle cartelle marker_artifacts, 
per avere un'idea di quanti report sono stati effettivamente estratti e salvati in formato leggibile.'''
# =========================
# CONFIG
# =========================
BASE_DIR = Path(r"C:\Universita\TESI\esg_agent\RAG_full\marker_artifacts")

# opzionale: salva dettagli
SAVE_DETAILS_CSV = True
DETAILS_CSV_PATH = BASE_DIR / "_year_folder_coverage_with_md.csv"


def has_any_md(folder: Path) -> bool:
    """
    True se dentro 'folder' esiste almeno un file .md (anche in sottocartelle).
    """
    if not folder.exists() or not folder.is_dir():
        return False
    # rglob è più robusto perché i .md possono stare in marker_out o simili
    return any(p.is_file() for p in folder.rglob("*.md"))


def main() -> None:
    if not BASE_DIR.exists():
        raise FileNotFoundError(f"Cartella non trovata: {BASE_DIR}")

    # contatori principali basati sulla PRESENZA DI MD
    only_2023_with_md = 0
    only_2024_with_md = 0
    both_with_md = 0
    neither_with_md = 0

    # contatori “di supporto” (cartelle presenti ma senza md)
    folder_2023_present_no_md = 0
    folder_2024_present_no_md = 0

    rows = []

    company_dirs = [p for p in BASE_DIR.iterdir() if p.is_dir()]

    for cdir in company_dirs:
        dir_2023 = cdir / "2023"
        dir_2024 = cdir / "2024"

        has_2023_dir = dir_2023.is_dir()
        has_2024_dir = dir_2024.is_dir()

        has_2023_md = has_any_md(dir_2023) if has_2023_dir else False
        has_2024_md = has_any_md(dir_2024) if has_2024_dir else False

        # contatori di supporto: cartella esiste ma non contiene md
        if has_2023_dir and not has_2023_md:
            folder_2023_present_no_md += 1
        if has_2024_dir and not has_2024_md:
            folder_2024_present_no_md += 1

        # classificazione principale basata su MD
        if has_2023_md and has_2024_md:
            category = "both_with_md"
            both_with_md += 1
        elif has_2023_md and not has_2024_md:
            category = "only_2023_with_md"
            only_2023_with_md += 1
        elif has_2024_md and not has_2023_md:
            category = "only_2024_with_md"
            only_2024_with_md += 1
        else:
            category = "neither_with_md"
            neither_with_md += 1

        rows.append({
            "company_slug_dir": cdir.name,
            "has_2023_dir": has_2023_dir,
            "has_2024_dir": has_2024_dir,
            "has_2023_md": has_2023_md,
            "has_2024_md": has_2024_md,
            "category": category,
            "path": str(cdir),
        })

    total = len(company_dirs)

    print("========== COVERAGE marker_artifacts (WITH .md CHECK) ==========")
    print(f"Base dir: {BASE_DIR}")
    print(f"Tot aziende (cartelle): {total:,}")
    print("---------------------------------------------------------------")
    print(f"SOLO 2023 (con .md):           {only_2023_with_md:,}")
    print(f"SOLO 2024 (con .md):           {only_2024_with_md:,}")
    print(f"ENTRAMBE (con .md):            {both_with_md:,}")
    print(f"NESSUNA (senza .md in 23/24):  {neither_with_md:,}")
    print("---------------------------------------------------------------")
    print(f"Cartella 2023 presente ma senza .md: {folder_2023_present_no_md:,}")
    print(f"Cartella 2024 presente ma senza .md: {folder_2024_present_no_md:,}")
    print("===============================================================")

    if SAVE_DETAILS_CSV:
        df = pd.DataFrame(rows).sort_values(["category", "company_slug_dir"])
        df.to_csv(DETAILS_CSV_PATH, index=False, encoding="utf-8")
        print(f"[OK] Dettagli salvati in: {DETAILS_CSV_PATH}")


if __name__ == "__main__":
    main()
