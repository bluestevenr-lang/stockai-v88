"""Reuse bounded-lifetime client resources across Streamlit reruns."""
import atexit
import streamlit as st


@st.cache_resource(show_spinner=False)
def http_session(direct=True):
    import requests
    session=requests.Session()
    session.trust_env=not direct
    atexit.register(session.close)
    return session


@st.cache_resource(show_spinner=False)
def yahoo_session():
    try:
        from curl_cffi.requests import Session
    except ImportError:
        return None
    # yfinance shares its data client; all callers must share one session too.
    session=Session(impersonate='chrome')
    atexit.register(session.close)
    return session
