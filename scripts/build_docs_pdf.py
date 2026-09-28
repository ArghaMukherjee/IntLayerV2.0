"""Renders the Markdown docs to PDF (docs/*.pdf). Requires: pip install reportlab

    python scripts/build_docs_pdf.py

Supports the Markdown used in docs/: headings, paragraphs, bullet and numbered lists, tables,
fenced code, **bold**, `code` and [links](...). A `<!-- diagram:NAME -->` comment replaces the
next code block with a vector diagram. Unicode fonts are taken from macOS when available.
"""
import math
import re
from datetime import date
from pathlib import Path
from xml.sax.saxutils import escape

from reportlab.graphics.shapes import Drawing, Line, Polygon, Rect, String
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (BaseDocTemplate, CondPageBreak, Frame, KeepTogether, NextPageTemplate, PageBreak,
                                PageTemplate, Paragraph, Spacer, Table, TableStyle, XPreformatted)
from reportlab.platypus.tableofcontents import TableOfContents

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"

# ------------------------------------------------------------------ fonts
SANS, SANS_B, MONO = "Helvetica", "Helvetica-Bold", "Courier"
for path, name in (("/System/Library/Fonts/Supplemental/Arial Unicode.ttf", "UniSans"),):
    if Path(path).exists():
        pdfmetrics.registerFont(TTFont(name, path))
        SANS = name   # body text: wide Unicode coverage (arrows, box drawing); headings stay Helvetica-Bold
if Path("/System/Library/Fonts/Menlo.ttc").exists():
    pdfmetrics.registerFont(TTFont("Menlo", "/System/Library/Fonts/Menlo.ttc", subfontIndex=0))
    MONO = "Menlo"

RED = colors.HexColor("#d8192a")
INK = colors.HexColor("#16181b")
MUTED = colors.HexColor("#5f646b")
LINE = colors.HexColor("#d7d9dd")
PALE = colors.HexColor("#f4f4f2")
PG = colors.HexColor("#336791")
BQ = colors.HexColor("#4285f4")
DARK = colors.HexColor("#111111")

BODY = ParagraphStyle("body", fontName=SANS, fontSize=9.3, leading=13.4, textColor=INK, spaceAfter=5)
SMALL = ParagraphStyle("small", parent=BODY, fontSize=7.8, leading=10.2, spaceAfter=0)
HEAD_CELL = ParagraphStyle("hc", parent=SMALL, textColor=colors.white)
H1 = ParagraphStyle("h1", parent=BODY, fontName=SANS_B, fontSize=18, leading=22, textColor=DARK, spaceBefore=4, spaceAfter=10)
H2 = ParagraphStyle("h2", parent=BODY, fontName=SANS_B, fontSize=12.5, leading=16, textColor=RED, spaceBefore=10, spaceAfter=5)
H3 = ParagraphStyle("h3", parent=BODY, fontName=SANS_B, fontSize=10.5, leading=14, textColor=DARK, spaceBefore=6, spaceAfter=3)
CODE = ParagraphStyle("code", fontName=MONO, fontSize=7.2, leading=9.4, textColor=INK)
BULLET = ParagraphStyle("bullet", parent=BODY, leftIndent=13, bulletIndent=3, spaceAfter=2)
CAPTION = ParagraphStyle("cap", parent=SMALL, textColor=MUTED, alignment=1, spaceBefore=2, spaceAfter=8)
W, H = A4
MARGIN = 17 * mm
CW = W - 2 * MARGIN


def inline(text: str) -> str:
    """Markdown inline markup -> reportlab paragraph markup."""
    parts = re.split(r"(`[^`]+`)", text)
    out = []
    for p in parts:
        if p.startswith("`") and p.endswith("`") and len(p) > 1:
            out.append(f'<font face="{MONO}" size="8">{escape(p[1:-1])}</font>')
        else:
            p = escape(p)
            p = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", p)
            p = re.sub(r"\*\*([^*]+)\*\*", r'<font color="#000000"><b>\1</b></font>', p)
            out.append(p)
    return "".join(out)


# ------------------------------------------------------------------ diagrams
def box(d, x, y, w, h, title, sub=(), fill=PALE, stroke=DARK, tcol=DARK, size=8.5):
    d.add(Rect(x, y, w, h, rx=5, ry=5, fillColor=fill, strokeColor=stroke, strokeWidth=1))
    total = size + len(sub) * 9
    ty = y + h / 2 + total / 2 - size
    d.add(String(x + w / 2, ty, title, fontName=SANS_B, fontSize=size, fillColor=tcol, textAnchor="middle"))
    for i, s in enumerate(sub):
        d.add(String(x + w / 2, ty - (i + 1) * 9, s, fontName=SANS, fontSize=6.6, fillColor=MUTED, textAnchor="middle"))


def arrow(d, x1, y1, x2, y2, color=INK, dashed=False, both=False):
    ln = Line(x1, y1, x2, y2, strokeColor=color, strokeWidth=1)
    if dashed:
        ln.strokeDashArray = [3, 2]
    d.add(ln)
    for ax, ay, bx, by in [(x1, y1, x2, y2)] + ([(x2, y2, x1, y1)] if both else []):
        a = math.atan2(by - ay, bx - ax)
        d.add(Polygon([bx, by, bx - 6 * math.cos(a - .4), by - 6 * math.sin(a - .4), bx - 6 * math.cos(a + .4),
                       by - 6 * math.sin(a + .4)], fillColor=color, strokeColor=color, strokeWidth=.5))


def label(d, x, y, text, size=6.3, anchor="middle"):
    w = len(text) * size * .52 + 4
    ox = {"middle": -w / 2, "start": -2, "end": -w + 2}[anchor]
    d.add(Rect(x + ox, y - 2, w, size + 3, fillColor=colors.white, strokeColor=None))
    d.add(String(x, y, text, fontName=SANS, fontSize=size, fillColor=MUTED, textAnchor=anchor))


def diagram_architecture():
    d = Drawing(CW, 330)
    box(d, 20, 262, 120, 50, "WWMG_Source", ["source system · :8002"], colors.HexColor("#fdecee"), RED, RED)
    box(d, 345, 262, 120, 50, "MyCareHub", ["member portal · :8003"], colors.HexColor("#fdecee"), RED, RED)
    box(d, 130, 170, 225, 58, "Integration Layer (il-api :8000)", ["auth · JSON Schema validation · routing",
                                                                    "idempotency · API log · callbacks"], PALE, DARK)
    arrow(d, 80, 262, 170, 228, RED)
    arrow(d, 405, 262, 315, 228, RED)
    label(d, 145, 252, "POST/GET /v1/requests", anchor="start")
    label(d, 340, 252, "POST/GET /v1/requests", anchor="end")
    arrow(d, 150, 228, 60, 262, MUTED, dashed=True)
    arrow(d, 335, 228, 425, 262, MUTED, dashed=True)
    label(d, 60, 236, "signed callback")
    label(d, 425, 236, "signed callback")
    box(d, 40, 70, 170, 62, "PostgreSQL 16", ["control plane: registry, routes, idempotency", "queue for 'postgres' workflows",
                                              "API log · console · data warehouse"], colors.HexColor("#e8eef5"), PG, PG)
    box(d, 275, 70, 170, 62, "BigQuery", ["dataset integration", "queue for 'bigquery' workflows",
                                           "emulator locally / GCP in production"], colors.HexColor("#e8f0fe"), BQ, BQ)
    arrow(d, 190, 170, 125, 132, PG, both=True)
    arrow(d, 295, 170, 360, 132, BQ, both=True)
    label(d, 142, 152, "SQL functions")
    label(d, 345, 152, "BigQuery DML")
    arrow(d, 210, 101, 275, 101, MUTED, dashed=True, both=True)
    label(d, 242, 106, "NOTIFY wake-ups")
    box(d, 130, 5, 225, 40, "ConsumerApplication (:8001)", ["one queue worker per backend · shared handlers"], PALE, DARK)
    arrow(d, 150, 45, 110, 70, PG, both=True)
    arrow(d, 335, 45, 375, 70, BQ, both=True)
    d.add(String(4, 30, "il-scheduler: reapers, partitions,", fontName=SANS, fontSize=6.6, fillColor=MUTED))
    d.add(String(4, 21, "warehouse loads (PG + BQ), traffic jobs", fontName=SANS, fontSize=6.6, fillColor=MUTED))
    d.add(String(CW - 4, 30, "console :8080 observes", fontName=SANS, fontSize=6.6, fillColor=MUTED, textAnchor="end"))
    d.add(String(CW - 4, 21, "and controls everything", fontName=SANS, fontSize=6.6, fillColor=MUTED, textAnchor="end"))
    return d, "Figure: V2 system architecture"


def diagram_sequence():
    lanes = {"Source app": 35, "Integration Layer": 150, "PostgreSQL": 265, "BigQuery": 360, "ConsumerApp": 455}
    msgs = [("Source app", "Integration Layer", "1. POST /v1/requests (X-API-Key)"),
            ("Integration Layer", "Integration Layer", "2. validate JSON Schema, read storage_backend"),
            ("Integration Layer", "PostgreSQL", "3a. submit_request()  (postgres workflow)"),
            ("Integration Layer", "PostgreSQL", "3b. register_bq_request() (route + idempotency)"),
            ("Integration Layer", "BigQuery", "3b. INSERT request (bigquery workflow)"),
            ("Integration Layer", "Source app", "4. 202 {request_id, storage_backend}"),
            ("PostgreSQL", "ConsumerApp", "5. NOTIFY request_new"),
            ("ConsumerApp", "BigQuery", "6. claim (token UPDATE) / SKIP LOCKED in PG"),
            ("ConsumerApp", "ConsumerApp", "7. handler + output schema check"),
            ("ConsumerApp", "BigQuery", "8. complete / fail"),
            ("ConsumerApp", "PostgreSQL", "9. NOTIFY request_done"),
            ("Integration Layer", "BigQuery", "10. claim callback"),
            ("Integration Layer", "Source app", "11. POST callback_url (X-IL-Signature)")]
    step = 21
    hgt = 40 + len(msgs) * step
    d = Drawing(CW, hgt)
    top = hgt - 6
    for name, x in lanes.items():
        col = PG if name == "PostgreSQL" else BQ if name == "BigQuery" else RED if name == "Source app" else DARK
        d.add(Rect(x - 42, top - 18, 84, 18, rx=4, ry=4, fillColor=colors.white, strokeColor=col))
        d.add(String(x, top - 12, name, fontName=SANS_B, fontSize=7.4, fillColor=col, textAnchor="middle"))
        ln = Line(x, top - 18, x, 3, strokeColor=LINE)
        ln.strokeDashArray = [2, 2]
        d.add(ln)
    y = top - 34
    for a, b, text in msgs:
        xa, xb = lanes[a], lanes[b]
        if a == b:
            d.add(Rect(xa - 3, y - 5, 6, 10, fillColor=MUTED, strokeColor=None))
            label(d, xa - 8 if xa > CW / 2 else xa + 8, y - 2, text, 6.3, "end" if xa > CW / 2 else "start")
        else:
            arrow(d, xa, y, xb, y, BQ if "BigQuery" in (a, b) else INK, dashed="NOTIFY" in text or text.startswith("4"))
            label(d, (xa + xb) / 2, y + 3, text, 6.1)
        y -= step
    return d, "Figure: request flow for both backends (3a = PostgreSQL workflow, 3b = BigQuery workflow)"


DIAGRAMS = {"architecture": diagram_architecture, "sequence": diagram_sequence}


# ------------------------------------------------------------------ markdown -> flowables
def md_table(lines):
    rows = [[c.strip() for c in l.strip().strip("|").split("|")] for l in lines if not re.match(r"^\|?\s*:?-{2,}", l.strip().strip("|").split("|")[0].strip() or "--")]
    rows = [r for r in rows if not all(re.fullmatch(r":?-+:?", c) for c in r)]
    n = max(len(r) for r in rows)
    data = [[Paragraph(inline(c), HEAD_CELL if i == 0 else SMALL) for c in r + [""] * (n - len(r))] for i, r in enumerate(rows)]
    lens = [max(len(re.sub(r"[`*]", "", r[j])) if j < len(r) else 0 for r in rows) for j in range(n)]
    lens = [min(max(l, 8), 60) for l in lens]
    widths = [CW * l / sum(lens) for l in lens]
    t = Table(data, colWidths=widths, repeatRows=1)
    style = [("GRID", (0, 0), (-1, -1), .4, LINE), ("BACKGROUND", (0, 0), (-1, 0), DARK), ("VALIGN", (0, 0), (-1, -1), "TOP"),
             ("LEFTPADDING", (0, 0), (-1, -1), 4), ("RIGHTPADDING", (0, 0), (-1, -1), 4),
             ("TOPPADDING", (0, 0), (-1, -1), 3), ("BOTTOMPADDING", (0, 0), (-1, -1), 3)]
    style += [("BACKGROUND", (0, i), (-1, i), PALE) for i in range(2, len(data), 2)]
    t.setStyle(TableStyle(style))
    return [t, Spacer(1, 7)]


def wrap_code(text: str, width: int = 112) -> str:
    """Hard-wraps long lines (continuations indented) so code never runs off the page."""
    out = []
    for line in text.split("\n"):
        while len(line) > width:
            cut = line.rfind(" ", 0, width)
            cut = cut if cut > width // 2 else width
            out.append(line[:cut] + " \\")
            line = "    " + line[cut:].lstrip()
        out.append(line)
    return "\n".join(out)


def code_block(text):
    t = Table([[XPreformatted(escape(wrap_code(text)), CODE)]], colWidths=[CW])
    t.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), PALE), ("BOX", (0, 0), (-1, -1), .5, LINE),
                           ("LINEBEFORE", (0, 0), (0, -1), 2, RED), ("LEFTPADDING", (0, 0), (-1, -1), 7),
                           ("TOPPADDING", (0, 0), (-1, -1), 5), ("BOTTOMPADDING", (0, 0), (-1, -1), 5)]))
    return [t, Spacer(1, 7)]


def markdown(path: Path, first_h1_page_break=False):
    out, lines, i, pending_diagram = [], path.read_text().splitlines(), 0, None
    para: list[str] = []

    def flush():
        if para:
            out.append(Paragraph(inline(" ".join(para)), BODY))
            para.clear()

    while i < len(lines):
        line = lines[i]
        s = line.strip()
        m = re.match(r"<!--\s*diagram:(\w+)\s*-->", s)
        if m:
            flush(); pending_diagram = m.group(1); i += 1; continue
        if s.startswith("<!--"):
            flush(); i += 1; continue
        if s.startswith("```"):
            flush()
            j = i + 1
            while j < len(lines) and not lines[j].strip().startswith("```"):
                j += 1
            block = "\n".join(lines[i + 1:j])
            if pending_diagram and pending_diagram in DIAGRAMS:
                d, cap = DIAGRAMS[pending_diagram]()
                out.append(KeepTogether([d, Paragraph(cap, CAPTION)]))
            else:
                out += code_block(block)
            pending_diagram = None
            i = j + 1
            continue
        if s.startswith("|"):
            flush()
            j = i
            while j < len(lines) and lines[j].strip().startswith("|"):
                j += 1
            out += md_table(lines[i:j])
            i = j
            continue
        h = re.match(r"^(#{1,3})\s+(.*)", s)
        if h:
            flush()
            level = len(h.group(1))
            if level == 1:
                if first_h1_page_break:
                    out.append(PageBreak())
                first_h1_page_break = True
                out.append(Paragraph(inline(h.group(2)), H1))
            else:
                out.append(CondPageBreak(55 * mm))
                out.append(Paragraph(inline(h.group(2)), H2 if level == 2 else H3))
            i += 1
            continue
        b = re.match(r"^(\s*)([-*]|\d+\.)\s+(.*)", line)
        if b:
            flush()
            indent = len(b.group(1)) // 2
            item = [b.group(3)]
            j = i + 1
            while j < len(lines) and lines[j].startswith("  " * (indent + 1)) and not re.match(r"^\s*([-*]|\d+\.)\s", lines[j]) and lines[j].strip():
                item.append(lines[j].strip()); j += 1
            bullet = "•" if b.group(2) in "-*" else b.group(2)
            st = ParagraphStyle("b", parent=BULLET, leftIndent=13 + indent * 12, bulletIndent=3 + indent * 12)
            out.append(Paragraph(inline(" ".join(item)), st, bulletText=bullet))
            i = j
            continue
        if not s:
            flush()
        else:
            para.append(s)
        i += 1
    flush()
    return out


class Doc(BaseDocTemplate):
    def __init__(self, path, title, subtitle):
        super().__init__(str(path), pagesize=A4, leftMargin=MARGIN, rightMargin=MARGIN, topMargin=22 * mm,
                         bottomMargin=18 * mm, title=title, author="Integration Layer V2.0")
        self.doc_title, self.subtitle = title, subtitle
        frame = Frame(MARGIN, 18 * mm, CW, H - 40 * mm)
        self.addPageTemplates([PageTemplate("cover", [frame], onPage=self.cover), PageTemplate("body", [frame], onPage=self.page)])

    def afterFlowable(self, f):
        if isinstance(f, Paragraph) and f.style.name in ("h1", "h2"):
            key = f"k{self.seq.nextf('k')}"
            self.canv.bookmarkPage(key)
            level = 0 if f.style.name == "h1" else 1
            self.canv.addOutlineEntry(f.getPlainText(), key, level=level, closed=level > 0)
            self.notify("TOCEntry", (level, f.getPlainText(), self.page, key))

    def cover(self, c, _):
        c.saveState()
        c.setFillColor(colors.HexColor("#070707")); c.rect(0, H - 100 * mm, W, 100 * mm, stroke=0, fill=1)
        c.setFillColor(RED); c.rect(0, H - 103 * mm, W, 3 * mm, stroke=0, fill=1)
        c.setFillColor(colors.white); c.setFont(SANS, 11)
        c.drawString(MARGIN, H - 30 * mm, "INTEGRATION LAYER V2.0")
        c.setFont(SANS_B, 28); c.drawString(MARGIN, H - 48 * mm, self.doc_title)
        c.setFont(SANS, 12.5); c.drawString(MARGIN, H - 60 * mm, self.subtitle)
        c.setFillColor(colors.HexColor("#ff6b73")); c.setFont(SANS, 10)
        c.drawString(MARGIN, H - 84 * mm, f"Version 2.0  |  {date.today():%d %B %Y}")
        c.restoreState()

    def page(self, c, doc):
        c.saveState()
        c.setStrokeColor(LINE); c.line(MARGIN, H - 15 * mm, W - MARGIN, H - 15 * mm); c.line(MARGIN, 13 * mm, W - MARGIN, 13 * mm)
        c.setFont(SANS, 7.8); c.setFillColor(MUTED)
        c.drawString(MARGIN, H - 13 * mm, f"Integration Layer V2.0: {self.doc_title}")
        c.drawString(MARGIN, 9 * mm, "WWMG_Source · MyCareHub → Integration Layer → PostgreSQL | BigQuery → ConsumerApplication")
        c.drawRightString(W - MARGIN, 9 * mm, f"Page {doc.page}")
        c.setFillColor(RED); c.rect(W - MARGIN - 18, H - 13.6 * mm, 18, 2.2, stroke=0, fill=1)
        c.restoreState()


def build(out_name, title, subtitle, sources):
    story = [NextPageTemplate("body"), Spacer(1, 100 * mm)]
    story.append(Paragraph(f"Contents: {', '.join(p.stem.replace('_', ' ').title() for p in sources)}", BODY))
    story.append(PageBreak())
    toc = TableOfContents()
    toc.levelStyles = [ParagraphStyle("t0", fontName=SANS_B, fontSize=10, leading=15, textColor=DARK),
                       ParagraphStyle("t1", fontName=SANS, fontSize=8.8, leading=12.5, leftIndent=12, textColor=INK)]
    story += [Paragraph("Contents", ParagraphStyle("toch", parent=H1)), toc, PageBreak()]
    for n, src in enumerate(sources):
        story += markdown(src, first_h1_page_break=n > 0)
    doc = Doc(DOCS / out_name, title, subtitle)
    doc.multiBuild(story)
    print("written", DOCS / out_name)


if __name__ == "__main__":
    build("IntLayer_V2_Architecture.pdf", "Architecture", "WWMG_Source · MyCareHub · PostgreSQL | BigQuery · ConsumerApplication",
          [DOCS / "ARCHITECTURE.md", DOCS / "IMPLEMENTATION_PLAN.md"])
    build("IntLayer_V2_Runbook_and_API.pdf", "Runbook and API Reference", "Step-by-step launch commands and every API",
          [DOCS / "RUNBOOK.md", DOCS / "API_REFERENCE.md"])
