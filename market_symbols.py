"""Explicit share-class aliases, retaining class identity across quote providers."""
import re
ALIASES={'BRK-A':'BRK_A','BRK.A':'BRK_A','BRK-B':'BRK_B','BRK.B':'BRK_B',
         'BF-A':'BF_A','BF.A':'BF_A','BF-B':'BF_B','BF.B':'BF_B'}

def canonical(code):
    code=str(code).upper().replace('.SH','.SS')
    if code.endswith('.HK'):return (code[:-3].lstrip('0') or '0')+'.HK'
    if code in ALIASES:return ALIASES[code]
    # US provider punctuation, preserving every class/series character. Never
    # strip a suffix down to its parent common stock.
    preferred=re.fullmatch(r'([A-Z]+)-P([A-Z]+)',code)
    if preferred:return preferred[1]+'$'+preferred[2]
    for yahoo_suffix,exchange_suffix in [('-WT','.W'),('-UN','.U')]:
        if re.fullmatch(r'[A-Z]+'+yahoo_suffix,code):return code[:-len(yahoo_suffix)]+exchange_suffix
    share_class=re.fullmatch(r'([A-Z]+)-([AB])',code)
    if share_class:return share_class[1]+'.'+share_class[2]
    return code

def yahoo_symbol(code):
    code=canonical(code)
    if code.endswith('.HK'):return code[:-3].zfill(4)+'.HK'
    if re.fullmatch(r'[A-Z]+\$[A-Z]+',code):return code.replace('$','-P')
    for suffix,target in [('.W','-WT'),('.U','-UN'),('.A','-A'),('.B','-B')]:
        if re.fullmatch(r'[A-Z]+'+re.escape(suffix),code):return code[:-len(suffix)]+target
    return {'BRK_A':'BRK-A','BRK_B':'BRK-B','BF_A':'BF-A','BF_B':'BF-B'}.get(code,code)
