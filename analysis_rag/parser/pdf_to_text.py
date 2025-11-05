# PDF → testo 
# Output: un .txt per PDF

import os
import re
import csv
import argparse
from pathlib import Path
from typing import List, Dict, Any

import fitz  # PyMuPDF

try:
    import pytesseract
    from PIL import Image
    _OCR_OK = True
except Exception:
    _OCR_OK = False


def sanitize_text(s: str) -> str:
    s = s.replace("\x0c", " ")
    s = re.sub(r"-\s*\n\s*", "", s)              # de-sillabazione
    s = re.sub(r"\r\n|\r", "\n", s)              # normalizza newline
    s = re.sub(r"[ \t]+", " ", s)                # comprimi spazi
    s = re.sub(r"\n{3,}", "\n\n", s)             # limita newline consecutivi
    return s.strip()


def has_enough_text(t: str, min_chars: int = 40) -> bool:
    return len(t.strip()) >= min_chars


def safe_name(s: str) -> str:
    return re.sub(r"[^\w\-. ]+", "_", s).strip().strip(".")


def find_pdfs(root: Path) -> List[Path]:
    return list(root.rglob("*.pdf"))


def page_ocr_text(page: fitz.Page, dpi: int, ocr_lang: str) -> str:

    zoom = dpi / 72.0
    mat = fitz.Matrix(zoom, zoom)  # isotropico
    pix = page.get_pixmap(matrix=mat, alpha=False)  # RGB
    img_bytes = pix.tobytes("png")
    from io import BytesIO
    pil_img = Image.open(BytesIO(img_bytes))
    text = pytesseract.image_to_string(pil_img, lang=ocr_lang)
    pil_img.close()
    return text


def extract_text_from_pdf(
    pdf_path: Path,
    enable_ocr: bool,
    ocr_lang: str,
    ocr_min_chars: int,
    ocr_dpi: int = 300
) -> Dict[str, Any]:
    doc = fitz.open(pdf_path.as_posix())
    total_pages = doc.page_count
    ocr_pages = 0
    all_text_parts: List[str] = []

    for i in range(total_pages):
        page = doc.load_page(i)
        raw = page.get_text("text") or ""
        used_ocr = False

        if enable_ocr and (not has_enough_text(raw, ocr_min_chars)):
            if not _OCR_OK:
                raise RuntimeError(
                    "OCR richiesto ma pytesseract/Pillow non disponibili. "
                    "Installa con: pip install pytesseract Pillow"
                )
            ocr_text = page_ocr_text(page, dpi=ocr_dpi, ocr_lang=ocr_lang)
            if has_enough_text(ocr_text, ocr_min_chars):
                raw = ocr_text
                used_ocr = True
                ocr_pages += 1

        clean = sanitize_text(raw)
        all_text_parts.append(clean if clean else "")

    doc.close()

    full_text = "\n\n--- PAGE BREAK ---\n\n".join(all_text_parts).strip()
    return {
        "text": full_text,
        "pages": total_pages,
        "total_words": len(full_text.split()),
        "ocr_pages": ocr_pages
    }


def main():
    ap = argparse.ArgumentParser(description="PDF → testo (ricorsivo su tutte le cartelle).")
    ap.add_argument("--input-dir", type=str, default="data/pdfs",
                    help="Cartella radice contenente sottocartelle con i PDF (ricerca ricorsiva).")
    ap.add_argument("--output-dir", type=str, default="data/output/text",
                    help="Dove salvare i .txt estratti.")
    ap.add_argument("--enable-ocr", action="store_true",
                    help="Abilita fallback OCR per pagine con poco testo.")
    ap.add_argument("--ocr-lang", type=str, default="eng+ita+deu+fra+spa",
                    help="Lingue OCR per Tesseract.")
    ap.add_argument("--ocr-min-chars", type=int, default=40,
                    help="Soglia caratteri pagina per attivare OCR fallback.")
    ap.add_argument("--ocr-dpi", type=int, default=300,
                    help="DPI di rasterizzazione (via PyMuPDF).")
    ap.add_argument("--tesseract-cmd", type=str, default="",
                    help="(Opzionale) Percorso eseguibile Tesseract, se non è nel PATH.")
    args = ap.parse_args()

    if args.tesseract_cmd:
        import pytesseract
        pytesseract.pytesseract.tesseract_cmd = args.tesseract_cmd

    in_root = Path(args.input_dir)
    out_root = Path(args.output_dir)
    out_root.mkdir(parents=True, exist_ok=True)

    pdfs = find_pdfs(in_root)
    if not pdfs:
        print(f"Nessun PDF trovato in: {in_root.resolve()}")
        return

    index_rows = []
    for pdf in pdfs:
        parent_name = safe_name(pdf.parent.name) or "ROOT"
        base_name = safe_name(pdf.stem)
        out_name = f"{parent_name}__{base_name}.txt"
        out_path = out_root / out_name

        try:
            res = extract_text_from_pdf(
                pdf_path=pdf,
                enable_ocr=args.enable_ocr,
                ocr_lang=args.ocr_lang,
                ocr_min_chars=args.ocr_min_chars,
                ocr_dpi=args.ocr_dpi
            )
            out_path.write_text(res["text"], encoding="utf-8")
            ocr_ratio = (res["ocr_pages"] / res["pages"]) if res["pages"] else 0.0
            print(f"[OK] {pdf} -> {out_path.name}  (pages={res['pages']}, words={res['total_words']}, ocr_ratio={ocr_ratio:.2f})")

            index_rows.append({
                "pdf_path": str(pdf),
                "txt_path": str(out_path),
                "pages": res["pages"],
                "total_words": res["total_words"],
                "ocr_pages": res["ocr_pages"],
                "ocr_ratio": f"{ocr_ratio:.4f}"
            })

        except Exception as e:
            print(f"[ERR] {pdf}: {e}")
            index_rows.append({
                "pdf_path": str(pdf),
                "txt_path": "",
                "pages": "",
                "total_words": "",
                "ocr_pages": "",
                "ocr_ratio": "",
                "error": str(e)
            })

    idx_csv = out_root / "pdf_text_index.csv"
    fieldnames = ["pdf_path", "txt_path", "pages", "total_words", "ocr_pages", "ocr_ratio", "error"]
    with idx_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in index_rows:
            for k in fieldnames:
                r.setdefault(k, "")
            writer.writerow(r)

    print(f"\nIndice scritto in: {idx_csv.resolve()}")


if __name__ == "__main__":
    main()
