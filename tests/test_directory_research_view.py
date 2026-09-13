from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from market_directory_view import research_index_caption, research_index_report


def test_caption_separates_queued_active_and_expired_without_claiming_grades():
    text=research_index_caption({'directory_total':15000,'status_counts':{
        'in_active_review_lane':1000,'queued_for_admission':10000,'source_expired':100,
        'missing_history':2000,'fact_rejected':1900}})
    assert '15,000只' in text and '当前审核池1,000只' in text and '分批接入10,000只' in text
    assert '来源过期100只' in text and '未审不授级' in text


def test_absent_optional_index_does_not_create_files(tmp_path):
    assert research_index_report(tmp_path) is None
    assert not list(tmp_path.iterdir())
