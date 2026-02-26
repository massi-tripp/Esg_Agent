import re
import pandas as pd

INPUT_PATH = r"C:\Universita\TESI\esg_agent\benchmark\attività_totali.xlsx"
SHEET_NAME = "report_corretti"
OUTPUT_PATH = r"C:\Universita\TESI\esg_agent\benchmark\attività_totali_clean.xlsx"

LEFT_COL_NAME = "environmental_objective_codes"


def _split_on_comma(x: object) -> list[str]:
    if pd.isna(x):
        return []
    s = str(x).strip()
    if not s:
        return []
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p]


def _extract_letters(part: str) -> str:
    """
    Estrae la parte alfabetica (es. 'CCM 8.1' -> 'CCM', 'CCM8.1' -> 'CCM').
    Se ci sono più token alfabetici, li concatena senza spazi (caso raro).
    """
    toks = re.findall(r"[A-Za-z]+", part)
    if not toks:
        return ""
    return "".join(toks).upper()


def _extract_number(part: str) -> str:
    """
    Estrae il primo numero con possibile decimale '.' (es. 'CCM 8.1' -> '8.1').
    """
    m = re.search(r"\d+(?:\.\d+)?", part)
    return m.group(0) if m else ""


def transform_sub_activity_code(cell: object) -> tuple[str, str]:
    """
    Ritorna (letters_joined, numbers_joined) con join ' / '.
    """
    parts = _split_on_comma(cell)

    letters_out = []
    numbers_out = []

    for p in parts:
        letters_out.append(_extract_letters(p))
        numbers_out.append(_extract_number(p))

    # Se la cella aveva un solo codice senza virgole, parts sarà len 1 -> ok.
    # Rimuove eventuali stringhe vuote ai bordi ma preserva posizione:
    # (Qui preferisco mantenerle coerenti: se manca lettera/numero, resta vuoto.)
    letters_joined = " / ".join(letters_out) if letters_out else ""
    numbers_joined = " / ".join(numbers_out) if numbers_out else ""

    return letters_joined, numbers_joined


# =====================
# READ
# =====================
df = pd.read_excel(INPUT_PATH, sheet_name=SHEET_NAME)

if "Sub_activity_code" not in df.columns:
    raise KeyError("Manca la colonna 'Sub_activity_code' nel foglio report_corretti")

# =====================
# TRANSFORM
# =====================
# calcola le 2 colonne da Sub_activity_code
tmp = df["Sub_activity_code"].apply(transform_sub_activity_code)

df[LEFT_COL_NAME] = tmp.apply(lambda t: t[0])
df["Sub_activity_code"] = tmp.apply(lambda t: t[1])

# sposta la colonna LEFT_COL_NAME a sinistra di Sub_activity_code (o come preferisci)
cols = list(df.columns)
cols.remove(LEFT_COL_NAME)
sub_idx = cols.index("Sub_activity_code")
cols = cols[:sub_idx] + [LEFT_COL_NAME] + cols[sub_idx:]
df = df[cols]

# =====================
# EXPORT
# =====================
with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
    df.to_excel(writer, sheet_name=SHEET_NAME, index=False)

print(f"OK! File esportato in: {OUTPUT_PATH}")
