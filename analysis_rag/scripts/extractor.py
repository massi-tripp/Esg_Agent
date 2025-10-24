# analysis_rag/extractor/extract_activities.py
import re, csv, json, argparse
from pathlib import Path
from typing import List, Dict, Any, Tuple
import yaml

PAGE_SPLIT = re.compile(r"\n?\s*--- PAGE BREAK ---\s*\n?", re.IGNORECASE)
SENT_SPLIT = re.compile(r"(?<=[\.\!\?])\s+(?=[A-ZÀ-ÖØ-Þ])")

ACTION_VERBS = [
    # EN
    "implement", "implemented", "implementing", "install", "installed", "deploy", "deployed",
    "launch", "launched", "introduce", "introduced", "adopt", "adopted", "source", "sourced",
    "procure", "procured", "purchase", "purchased", "certify", "certified", "train", "trained",
    "upgrade", "upgraded", "retrofit", "retrofitted", "improve", "improved", "reduce", "reduced",
    "establish", "established", "set up", "rolled out",
    # IT
    "implementato", "implementare", "installato", "installare", "lanciato", "introdotto",
    "adottato", "adottare", "approvvigionato", "certificato", "formazione", "formare",
    "migliorato", "ridotto", "attuato",
    # DE
    "implementiert", "eingeführt", "installiert", "beschafft", "zertifiziert", "geschult",
    "verbessert", "reduziert",
    # ES
    "implementado", "instalado", "desplegado", "lanzado", "introducido", "adoptado",
    "certificado", "capacitación", "mejorado", "reducido",
    # FR
    "mis en œuvre", "installé", "déployé", "lancé", "introduit", "adopté", "certifié",
    "formé", "amélioré", "réduit"
]

CONTEXT_WORDS = [
    # EN
    "access to renewable", "availability of renewable", "incentives", "subsidies",
    "charging infrastructure", "well-functioning infrastructure", "dependency", "dependencies",
    "depends on", "if the availability", "risk", "uncertainty",
    # IT
    "accesso alle rinnovabili", "disponibilità di energia rinnovabile", "incentivi", "sussidi",
    "infrastruttura di ricarica", "dipendenza", "dipendenze", "se la disponibilità",
    # DE/ES/FR chiave
    "verfügbarkeit", "infrastruktur", "dependencia", "disponibilidad", "infrastructure", "disponibilité"
]

UNITS_HINT = ["gwh","mwh","kwh","twh","tco2e","kt","mt","m3","m³","gj","tj","%"]

# parole “prodotto/tecnico” che non c'entrano con DEI
PRODUCT_TECH_HINTS = ["fuel consumption", "energy efficiency", "co2", "scope 1", "scope 2", "renewable fuel", "powertrain"]

def load_taxonomy(path: Path) -> Dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    acts = [l for l in data["labels"] if l["id"].startswith("act_")]
    anchors = data.get("section_anchors", {})
    return {"activities": acts, "anchors": anchors}

def page_sentences(text: str) -> List[Tuple[int, str]]:
    pages = PAGE_SPLIT.split(text)
    out: List[Tuple[int,str]] = []
    for pidx, page_txt in enumerate(pages, start=1):
        norm = " ".join(page_txt.split())
        if not norm:
            continue
        sents = SENT_SPLIT.split(norm)
        for s in sents:
            s = s.strip()
            if s:
                out.append((pidx, s))
    return out

def looks_like_table(sent: str) -> bool:
    digits = sum(c.isdigit() for c in sent)
    length = len(sent)
    units_hits = sum(1 for u in UNITS_HINT if u in sent.lower())
    many_separators = sent.count("  ") > 3 or sent.count("|") > 1
    return (length > 350 and digits > 20) or units_hits >= 3 or many_separators

def has_action_verbs(sent: str) -> bool:
    s = sent.lower()
    return any(v in s for v in ACTION_VERBS)

def has_context_penalty(sent: str) -> bool:
    s = sent.lower()
    return any(w in s for w in CONTEXT_WORDS)

def score_sentence(sent: str, synonyms: List[str], anchors_for_cat: List[List[str]],
                   require_synonym_hit: bool) -> float:
    s_low = sent.lower()
    score = 0.0
    syn_hit = False
    # sinonimi
    for syn in synonyms:
        if syn.lower() in s_low:
            score += 1.0
            syn_hit = True
    if require_synonym_hit and not syn_hit:
        return -999.0  # scarta subito

    # anchor boost
    for group in anchors_for_cat or []:
        for a in group:
            if a.lower() in s_low:
                score += 0.3
                break
    # azione
    if has_action_verbs(sent):
        score += 0.7
    # penalità contesto (più alta)
    if has_context_penalty(sent):
        score -= 0.8
    # penalità frasi eccessive/tabellari
    if looks_like_table(sent):
        score -= 1.0
    # penalità lunghezza estrema
    if len(sent) > 600:
        score -= 0.4
    return score

def normalize_for_dedupe(s: str) -> str:
    s = s.lower()
    s = re.sub(r"\d+", "", s)
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def guess_company_from_filename(txt_path: Path) -> str:
    base = txt_path.stem
    if "__" in base:
        return base.split("__")[0].strip()
    return txt_path.parent.name

def main():
    ap = argparse.ArgumentParser(description="Estrazione attività ESG (rule-based) dai testi .txt")
    ap.add_argument("--texts-dir", type=str, default="analysis_rag/data/output/text_images", help="Cartella con i .txt")
    ap.add_argument("--taxonomy", type=str, default="analysis_rag/configs/taxonomy.yaml")
    ap.add_argument("--out-dir", type=str, default="analysis_rag/data/interim/activities")
    ap.add_argument("--min-score", type=float, default=1.8, help="Soglia match (synonyms + anchors + action verbs)")
    ap.add_argument("--max_sentences_per_label", type=int, default=8, help="Top frasi per label/doc")
    ap.add_argument("--require-synonym-hit", action="store_true", help="Accetta frasi solo se contengono almeno un sinonimo della label")
    args = ap.parse_args()

    txt_dir = Path(args.texts_dir)
    if (txt_dir / "text").exists():
        txt_dir = txt_dir / "text"

    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    tax = load_taxonomy(Path(args.taxonomy))
    anchors_by_cat = tax["anchors"]

    txt_files = list(txt_dir.glob("*.txt"))
    if not txt_files:
        print(f"Nessun .txt trovato in {txt_dir}"); return

    jsonl_path = out_dir / "activities_extracted.jsonl"
    csv_path = out_dir / "activities_counts.csv"
    details = []
    counts: Dict[Tuple[str,str], int] = {}

    for txt in txt_files:
        company = guess_company_from_filename(txt)
        text = txt.read_text(encoding="utf-8", errors="ignore")
        sents = page_sentences(text)

        seen_norm_by_label: Dict[str, set] = {}

        for lab in tax["activities"]:
            cat = lab["category"]
            synonyms = lab.get("synonyms", [])
            anchor_groups = anchors_by_cat.get(cat, [])

            scored: List[Tuple[float,int,str]] = []
            for page_idx, sent in sents:
                # blocco speciale: evita che frasi “prodotto/tecniche” entrino in DEI
                if lab["id"] == "act_soc_diversity_inclusion":
                    if any(h in sent.lower() for h in PRODUCT_TECH_HINTS):
                        continue

                sc = score_sentence(sent, synonyms, anchor_groups, args.require_synonym_hit)
                if sc >= args.min_score:
                    scored.append((sc, page_idx, sent))

            scored.sort(key=lambda x: (-x[0], x[1]))

            kept = 0
            seen_norm = seen_norm_by_label.setdefault(lab["id"], set())
            for sc, page_idx, sent in scored:
                norm = normalize_for_dedupe(sent)
                if norm in seen_norm:
                    continue
                seen_norm.add(norm)
                details.append({
                    "company": company,
                    "txt": str(txt),
                    "page": page_idx,
                    "label_id": lab["id"],
                    "category": cat,
                    "score": round(sc, 3),
                    "sentence": sent
                })
                counts[(company, lab["id"])] = counts.get((company, lab["id"]), 0) + 1
                kept += 1
                if kept >= args.max_sentences_per_label:
                    break

    with jsonl_path.open("w", encoding="utf-8") as f:
        for r in details:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["company", "label_id", "count"])
        for (company, label_id), c in sorted(counts.items()):
            w.writerow([company, label_id, c])

    print(f"[OK] Salvati: {jsonl_path} e {csv_path}")

if __name__ == "__main__":
    main()
