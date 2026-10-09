"""Show persisted data immediately; cloud synchronization never blocks a page."""
from pathlib import Path
import subprocess, sys, time, json, os
from datetime import datetime, timezone
from v88_paths import core_root


def schedule_sync():
    base=core_root();script=base/'src/autonomous_sync.py'
    if not script.exists():return False
    stamp=base/'data/.autonomous_sync_attempt'
    lock=base/'data/.autonomous_sync_ui_lock'
    try:
        # O_EXCL makes simultaneous browser sessions start at most one worker.
        try:fd=os.open(lock,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
        except FileExistsError:
            if time.time()-lock.stat().st_mtime>150:lock.unlink(missing_ok=True)
            return False
        os.close(fd)
        if stamp.exists() and time.time()-stamp.stat().st_mtime<60:
            lock.unlink(missing_ok=True);return False
        stamp.touch()
        log=base/'logs/autonomous_sync.log';log.parent.mkdir(exist_ok=True)
        with log.open('a') as out:
            subprocess.Popen([sys.executable,str(Path(__file__).resolve()),'--worker',str(base)],cwd=base,
                stdout=out,stderr=out,start_new_session=True)
        return True
    except OSError:
        lock.unlink(missing_ok=True)
        return False


def _worker(base):
    base=Path(base);state=base/'data/autonomous_sync_ui_status.json'
    def write(status):
        tmp=state.with_suffix('.tmp')
        tmp.write_text(json.dumps({'status':status,'at':datetime.now(timezone.utc).isoformat()}));tmp.replace(state)
    try:
        write('running')
        result=subprocess.run([sys.executable,str(base/'src/autonomous_sync.py')],cwd=base,timeout=120)
        write('complete' if result.returncode==0 else 'failed')
    except (OSError,subprocess.TimeoutExpired):write('failed')
    finally:(base/'data/.autonomous_sync_ui_lock').unlink(missing_ok=True)


def sync_on_open():
    import streamlit as st
    st.session_state.setdefault('_v88_opened_at',datetime.now(timezone.utc).isoformat())
    schedule_sync()
    # The fragment reads small local files only. No network call on the UI thread.
    @st.fragment(run_every=5)
    def sync_status():
        schedule_sync()
        from module_freshness import read,display,inventory,html
        base=core_root()/'data'
        state=read(base/'autonomous_sync_ui_status.json')
        receipt=read(base/'autonomous_sync_status.json')
        board=read(base/'strategy_board.json')
        running=(base/'.autonomous_sync_ui_lock').exists()
        if running:
            st.progress(0.5,text='核心数据加载进度 1/2 · 本机结果已读取；正在后台核对 GitHub 最新发布')
        elif state.get('status')=='failed':
            st.warning('云端核对暂未成功，当前显示已保存结果；下次自动重试。')
        else:
            st.progress(1.0 if receipt else 0.5,text='核心数据加载进度 2/2 · 已读取结果；云端核对完成' if receipt else '核心数据加载进度 1/2 · 本机结果已读取；等待云端核对')
        st.caption('北京时间 · 本次打开 '+display(st.session_state['_v88_opened_at'])+
                   ' · 最近云端核对 '+display(receipt.get('at'))+' · 云端发布 '+display(receipt.get('cloud_generated_at')))
        if board:
            rows=board.get('rows',[])
            cells=[]
            for market in ('A股','港股','美股'):
                counts=' / '.join(f'{tier} {sum(r.get("market")==market and r.get("strategy_tier")==tier and not r.get("grade_pending") for r in rows)}只' for tier in ('3A','2A','1A'))
                cells.append({'市场':market,'当前策略名单':counts})
            from html import escape
            summary='<table style="width:100%;border-collapse:collapse;font-size:16px;line-height:1.8"><thead><tr><th style="text-align:left">市场</th><th style="text-align:left">当前策略名单 · 研究候选与入场状态见下方</th></tr></thead><tbody>'
            for cell in cells:
                summary+='<tr style="border-top:1px solid #dce4ee"><td style="padding:7px">'+escape(cell['市场'])+'</td><td>'+escape(cell['当前策略名单'])+'</td></tr>'
            st.html(summary+'</tbody></table>')
            st.html(html('strategy_board.json',board))
        with st.expander('🕒 全模块更新时间 / 缓存时间（北京时间）'):
            st.caption('计算时间是结果版本；缓存时间是本机文件写入时间。刷新页面不会改写这两项。云端盘中每30分钟计划更新，网页每60秒核对；数据源日期见各模块。')
            st.dataframe([{'模块':r['module'],'计算完成':r['generated'],'结果年龄':r['age'],'缓存写入':r['cached'],'缓存年龄':r['cache_age']} for r in inventory()],hide_index=True,use_container_width=True)
    sync_status()

if __name__=='__main__' and len(sys.argv)>2 and sys.argv[1]=='--worker':_worker(sys.argv[2])
