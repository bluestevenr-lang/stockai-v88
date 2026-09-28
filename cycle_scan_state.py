"""A pending market refresh cannot erase an independently published stock cycle."""
from copy import deepcopy

def select_cycle(incoming, previous):
    incoming=incoming or {};previous=previous or {}
    # An explicit successful empty pool is a user removal, not a failed refresh.
    if incoming.get('status')=='complete' and incoming.get('pool_count')==0:return deepcopy(incoming)
    if incoming.get('stocks') and incoming.get('status')!='pending':
        # Embedded market snapshots can lag the standalone producer.
        if previous.get('stocks') and previous.get('analysis_time','')>incoming.get('analysis_time',''):
            return deepcopy(previous)
        return deepcopy(incoming)
    if previous.get('stocks'):
        result=deepcopy(previous)
        result['refresh_status']='pending' if incoming.get('status')=='pending' else 'retained'
        result['refresh_note']='市场刷新中，保留上次个股记录；行情日期逐只列示'
        return result
    return deepcopy(incoming)


def read_cycle(incoming, data):
    import json
    from pathlib import Path
    try:previous=json.loads((Path(data)/'cycle_scan.json').read_text())
    except (OSError,ValueError):previous={}
    return select_cycle(incoming,previous)
