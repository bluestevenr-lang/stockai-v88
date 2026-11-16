"""Pure company-evidence predicates shared by producers and read-only views."""
def primary_business(q):
    n=q.get('nontechnical') or {}
    return n.get('has_current_non_technical_support') is True

def vendor_business(q):
    return (q.get('fundamentals') or {}).get('status')=='support'

def business(q):
    f=q.get('fundamentals') or {}
    if f.get('status')=='counterevidence' and (f.get('earnings_quality') or {}).get('score_basis')=='recurring':return False
    # Original-document evidence or a current vendor financial report with
    # positive quality. Vendor counterevidence never counts as support.
    return primary_business(q) or vendor_business(q)

