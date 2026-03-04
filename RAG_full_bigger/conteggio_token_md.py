''' questo codice mi serve per contare i token dei file md generati da 2_document_reduction.py, 
per capire se sono troppo lunghi per essere usati come contesto in RAG. '''

from __future__ import annotations

from pathlib import Path
import statistics


BASE_DIR = Path(r"C:\Universita\TESI\esg_agent\RAG_full_bigger\marker_artifacts")


def iter_focused_md_files(base_dir: Path) -> list[Path]:
    """
    Prende SOLO i file focused creati da noi:
      .../<slug>/<year>/focused/*_taxonomy_focused.md
    per year in {2023, 2024}.
    """
    patterns = [
        "*/*/focused/*_taxonomy_focused.md",   # copre <slug>/<year>/focused/<file>
    ]
    files: list[Path] = []
    for pat in patterns:
        files.extend([p for p in base_dir.glob(pat) if p.is_file()])

    # tieni solo anni 2023/2024 per sicurezza
    out = []
    for p in files:
        try:
            year = p.parent.parent.name  # .../<year>/focused/<file>
            if year in {"2023", "2024"}:
                out.append(p)
        except Exception:
            continue

    return sorted(set(out))


def count_tokens_tiktoken(text: str) -> int:
    """
    Conteggio token con tiktoken (più accurato).
    Usa cl100k_base come encoding standard moderno.
    """
    import tiktoken  # type: ignore

    enc = tiktoken.get_encoding("cl100k_base")
    return len(enc.encode(text))


def estimate_tokens_chars(text: str) -> int:
    """
    Fallback grezzo: ~1 token ogni 4 caratteri.
    """
    if not text:
        return 0
    return max(1, len(text) // 4)


def main() -> None:
    if not BASE_DIR.exists():
        raise FileNotFoundError(f"Cartella non trovata: {BASE_DIR}")

    md_files = iter_focused_md_files(BASE_DIR)
    if not md_files:
        # stampa SOLO min max media (qui: zero)
        print("0 0 0")
        return

    # prova tiktoken, altrimenti fallback
    use_tiktoken = True
    try:
        import tiktoken  # noqa: F401
    except Exception:
        use_tiktoken = False

    token_counts = []

    for p in md_files:
        txt = p.read_text(encoding="utf-8", errors="replace")
        if use_tiktoken:
            n = count_tokens_tiktoken(txt)
        else:
            n = estimate_tokens_chars(txt)
        token_counts.append(n)

    mn = min(token_counts)
    mx = max(token_counts)
    avg = statistics.mean(token_counts)

    # stampa SOLO min, max, media
    # (media con 2 decimali)
    print(f"{mn} {mx} {avg:.2f}")

    # Se vuoi essere avvisato quando manca tiktoken, decommenta le righe sotto:
    # if not use_tiktoken:
    #     print("NOTE: tiktoken non installato. Installa con: pip install tiktoken")


if __name__ == "__main__":
    main()
