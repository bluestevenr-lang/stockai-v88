"""One restrained visual hierarchy; recommendation semantics stay in the engine."""
CSS = '''<style>
[data-testid="stAppViewContainer"] .stMarkdown p,
[data-testid="stAppViewContainer"] .stMarkdown li{font-size:13px;line-height:1.5}
[data-testid="stAppViewContainer"] [data-testid="stMarkdownContainer"] p,
[data-testid="stAppViewContainer"] [data-testid="stMarkdownContainer"] li{font-size:13px!important;line-height:1.5}
[data-testid="stAppViewContainer"] [data-testid="stExpander"] [data-testid="stMarkdownContainer"] p,
[data-testid="stAppViewContainer"] [data-testid="stExpander"] [data-testid="stMarkdownContainer"] li{font-size:12px!important;line-height:1.45}
[data-testid="stAppViewContainer"] [data-testid="stAlert"] p,
[data-testid="stAppViewContainer"] details > summary p{font-size:12px!important;line-height:1.45!important}
[data-testid="stAppViewContainer"] h1{font-size:20px!important;line-height:1.3!important}
[data-testid="stAppViewContainer"] h2{font-size:17px!important;line-height:1.35!important}
[data-testid="stAppViewContainer"] h3{font-size:15px!important;line-height:1.4!important}
[data-testid="stAppViewContainer"] [data-testid="stMetricValue"]{font-size:20px!important;line-height:1.25}
[data-testid="stAppViewContainer"] [data-testid="stMetricLabel"] p{font-size:12px!important}
.v88-scan-primary td,.v88-scan-primary th{font-variant-numeric:tabular-nums}
.v88-grade-table tbody tr:hover{background:#f1f5f9}
.v88-grade-table td a{font-weight:650}
.v88-operational-note{font-size:12px;line-height:1.45;color:#64748b;margin:4px 0}
@media(max-width:600px){[data-testid="stAppViewContainer"] .block-container{padding-left:8px;padding-right:8px}}
@media(prefers-reduced-motion:reduce){[data-testid="stStatusWidget"]::after{animation:none!important}}

/* Operational hierarchy: color is always paired with words/icons. */
:root{--v88-blue:#2563eb;--v88-green:#15803d;--v88-amber:#b45309;--v88-red:#b91c1c;--v88-muted:#64748b}
[data-testid="stAppViewContainer"] [data-testid="stCaptionContainer"] p,
[data-testid="stAppViewContainer"] small{font-size:11px!important;color:var(--v88-muted);line-height:1.4!important}
[data-testid="stAppViewContainer"] details>summary{cursor:pointer;color:#475569;padding:3px 0;font-size:11px!important;line-height:1.45!important}
[data-testid="stAppViewContainer"] details[open]>summary{color:#1d4ed8;font-weight:600}
[data-testid="stAppViewContainer"] [data-testid="stExpander"]{background:#ffffff;border:1px solid #e2e8f0;border-left:3px solid #93c5fd;border-radius:6px}
[data-testid="stAppViewContainer"] [data-testid="stAlert"]{padding:8px 12px;border-radius:6px;font-size:12px!important;box-shadow:none}
[data-testid="stAppViewContainer"] [data-testid="stMetric"]{background:#f8fafc;border:1px solid #e2e8f0;border-radius:7px;padding:8px}
.v88-quick-nav a{background:#eff6ff;color:#1d4ed8;border:1px solid #dbeafe;border-radius:5px;padding:4px 8px}
.v88-runtime{background:#f1f5f9;border-radius:5px;padding:5px 8px}
.v88-grade-table{min-width:1800px}
.v88-grade-table td:first-child{min-width:140px}
.v88-grade-table td:nth-child(2){min-width:150px}
.v88-grade-table th{background:#edf4ff;color:#334155;font-size:12px!important}
.v88-grade-table td{vertical-align:top;font-size:12px!important;line-height:1.45!important}
.v88-grade-table tbody tr:nth-child(even){background:#f8fafc}
.v88-watch-row[data-paused="true"] td:nth-child(2){border-left:3px solid #94a3b8;color:#64748b;background:#f8fafc}
.v88-watch-row[data-tier="1A"] td:nth-child(2){border-left:3px solid #3b82f6;background:#eff6ff;color:#1d4ed8}
.v88-watch-row[data-tier="2A"] td:nth-child(2){border-left:3px solid #d97706;background:#fffbeb;color:#92400e}
.v88-watch-row[data-tier="3A"] td:nth-child(2){border-left:3px solid #16a34a;background:#f0fdf4;color:#166534}
.v88-deep-synthesis{border-left:3px solid #3b82f6!important;background:#f8fbff}
.v88-joint-review>summary{color:#7c3aed!important}
@media(max-width:600px){[data-testid="stAppViewContainer"] [data-testid="stCaptionContainer"] p,
[data-testid="stAppViewContainer"] details>summary{font-size:12px!important}}
</style>'''
