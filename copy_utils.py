# -*- coding: utf-8 -*-
"""
复制和报告生成工具 (Copy & Report Utils)
V90 - 2026-02-09

功能：
1. 一键复制文本
2. 生成汇总报告
3. 格式化数据导出
4. 【V90新增】个股分析分享卡片生成（PIL，iPhone尺寸）
"""

import streamlit as st
from datetime import datetime
import pandas as pd
from typing import Dict, List, Any
import io
import os
import html

class CopyUtils:
    """复制工具类"""
    
    @staticmethod
    def create_copy_button(content: str, button_text: str = "📋 复制", key: str = None) -> None:
        """
        创建复制按钮
        
        参数:
            content: 要复制的内容
            button_text: 按钮文本
            key: 按钮唯一key
        
        【V91.11】修复：st.markdown 不执行 JS，改用 components.html 在 iframe 中渲染，复制功能可正常执行
        """
        if not content:
            return
        if not key:
            import hashlib
            key = f"copy_{hashlib.md5(content[:50].encode()).hexdigest()[:8]}"
        # 安全 key：仅保留字母数字下划线
        safe_key = "".join(c if c.isalnum() or c == "_" else "_" for c in key)[:32]
        
        # 内容转义，避免 HTML 注入
        escaped = html.escape(content)
        
        # 使用 components.html 在 iframe 中渲染，JavaScript 可执行，Clipboard API 可用
        copy_html = f"""
        <!DOCTYPE html>
        <html>
        <head><meta charset="utf-8"></head>
        <body style="margin:0;padding:8px;">
        <div id="copy_content_{safe_key}" style="display:none;">{escaped}</div>
        <button id="copy_btn_{safe_key}" onclick="copyToClipboard_{safe_key}()" 
                style="background-color:#3b82f6;color:white;border:none;padding:0.5rem 1rem;border-radius:4px;cursor:pointer;font-size:14px;">
            {html.escape(button_text)}
        </button>
        <script>
        function copyToClipboard_{safe_key}() {{
            var el = document.getElementById('copy_content_{safe_key}');
            var text = el ? el.textContent : '';
            function succeed() {{
                var btn = document.getElementById('copy_btn_{safe_key}');
                if (btn) {{
                    btn.textContent = '✅ 已复制';
                    btn.style.backgroundColor = '#10b981';
                    setTimeout(function() {{
                        btn.textContent = '{html.escape(button_text)}';
                        btn.style.backgroundColor = '#3b82f6';
                    }}, 2000);
                }}
            }}
            if (navigator.clipboard && navigator.clipboard.writeText) {{
                navigator.clipboard.writeText(text).then(succeed).catch(function() {{
                    fallbackCopy();
                }});
            }} else {{
                fallbackCopy();
            }}
            function fallbackCopy() {{
                var ta = document.createElement('textarea');
                ta.value = text;
                ta.style.position = 'fixed'; ta.style.left = '-9999px';
                document.body.appendChild(ta);
                ta.select();
                try {{
                    if (document.execCommand('copy')) succeed();
                    else alert('复制失败，请手动全选后 Ctrl+C');
                }} catch (e) {{
                    alert('复制失败: ' + e);
                }}
                document.body.removeChild(ta);
            }}
        }}
        </script>
        </body>
        </html>
        """
        try:
            import streamlit.components.v1 as components
            components.html(copy_html, height=56, scrolling=False)
        except Exception:
            # 降级：文本框 + 手动复制
            st.text_area("📋 复制内容（全选后 Ctrl+C/Cmd+C）", value=content[:5000], height=80, key=f"copy_fallback_{safe_key}")
    
    @staticmethod
    def create_copy_textbox(content: str, label: str = "📋 复制内容（全选Ctrl+C/Cmd+C）", height: int = 150) -> None:
        """
        创建可复制的文本框
        
        参数:
            content: 要复制的内容
            label: 文本框标签
            height: 文本框高度
        """
        st.text_area(
            label,
            value=content,
            height=height,
            help="全选内容后按 Ctrl+C (Windows) 或 Cmd+C (Mac) 复制",
            key=f"copybox_{hash(content[:20])}"
        )


    @staticmethod
    def render_markdown_with_section_copy(content: str, key_prefix: str = "sec") -> None:
        """
        渲染AI生成的Markdown内容，每个标题旁带「复制」按钮。
        - ## 大标题 → 点击复制该大标题下的所有内容（含子段落）
        - ### 小标题 → 点击复制该小标题下的段落
        
        参数:
            content: AI生成的markdown全文
            key_prefix: 用于生成唯一ID的前缀
        """
        import re, hashlib
        
        if not content or not content.strip():
            return
        
        # ── 按行拆分，识别标题层级 ──
        lines = content.split('\n')
        sections = []       # [(level, title, body_text, full_text_with_title)]
        current_level = 0
        current_title = ""
        current_body_lines = []
        
        for line in lines:
            heading_match = re.match(r'^(#{1,4})\s+(.+)', line)
            if heading_match:
                # 保存上一个段落
                if current_title:
                    body = '\n'.join(current_body_lines).strip()
                    full = f"{'#' * current_level} {current_title}\n{body}"
                    sections.append((current_level, current_title, body, full))
                elif current_body_lines:
                    # 标题前的前言文字
                    body = '\n'.join(current_body_lines).strip()
                    if body:
                        sections.append((0, "", body, body))
                
                current_level = len(heading_match.group(1))
                current_title = heading_match.group(2).strip()
                current_body_lines = []
            else:
                current_body_lines.append(line)
        
        # 最后一个段落
        if current_title:
            body = '\n'.join(current_body_lines).strip()
            full = f"{'#' * current_level} {current_title}\n{body}"
            sections.append((current_level, current_title, body, full))
        elif current_body_lines:
            body = '\n'.join(current_body_lines).strip()
            if body:
                sections.append((0, "", body, body))
        
        if not sections:
            st.markdown(content)
            return
        
        # ── 构建"大标题"到其所有子内容的映射 ──
        major_map = {}  # idx -> full_text (包含子段落)
        i = 0
        while i < len(sections):
            lvl, title, body, full = sections[i]
            if lvl > 0 and lvl <= 2:
                # 大标题（# 或 ##），收集其后所有更低层级的内容
                collected = [full]
                j = i + 1
                while j < len(sections):
                    sub_lvl = sections[j][0]
                    if sub_lvl > 0 and sub_lvl <= lvl:
                        break  # 遇到同级或更高级标题，停止
                    collected.append(sections[j][3])
                    j += 1
                major_map[i] = '\n\n'.join(collected)
            i += 1
        
        uid = hashlib.md5(key_prefix.encode()).hexdigest()[:6]
        
        try:
            import streamlit.components.v1 as components
        except Exception:
            components = None
        
        for idx, (lvl, title, body, full) in enumerate(sections):
            btn_id = f"btn_{uid}_{idx}"
            
            if lvl == 0:
                st.markdown(body)
                continue
            
            if idx in major_map:
                copy_text = major_map[idx]
            else:
                copy_text = full
            
            safe_copy = copy_text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;').replace("'", "&#39;")
            
            # 标题行：左列标题，右列复制按钮（components.html 使 JS 可执行）
            tag = f"h{min(lvl + 1, 6)}"
            head_col, btn_col = st.columns([6, 1])
            with head_col:
                st.markdown(f"<{tag} style='margin: 0.8rem 0 0.3rem 0;'>{html.escape(title)}</{tag}>", unsafe_allow_html=True)
            with btn_col:
                if components:
                    copy_btn_html = f"""
                    <!DOCTYPE html><html><head><meta charset="utf-8"></head><body style="margin:0;padding:4px 0;">
                    <div id="copy_content_{btn_id}" style="display:none;">{safe_copy}</div>
                    <button id="{btn_id}" onclick="(function(){{
                        var el = document.getElementById('copy_content_{btn_id}');
                        var t = el ? el.textContent : '';
                        function ok(){{ var b=document.getElementById('{btn_id}'); if(b){{ b.textContent='✓'; b.style.background='#10b981'; setTimeout(function(){{ b.textContent='📋'; b.style.background='#3b82f6'; }}, 1500); }} }}
                        if (navigator.clipboard && navigator.clipboard.writeText) {{
                            navigator.clipboard.writeText(t).then(ok).catch(function(){{
                                var ta=document.createElement('textarea'); ta.value=t; ta.style.position='fixed'; ta.style.left='-9999px';
                                document.body.appendChild(ta); ta.select();
                                try {{ document.execCommand('copy') && ok(); }} catch(e){{}}
                                document.body.removeChild(ta);
                            }});
                        }} else {{
                            var ta=document.createElement('textarea'); ta.value=t; ta.style.position='fixed'; ta.style.left='-9999px';
                            document.body.appendChild(ta); ta.select();
                            try {{ document.execCommand('copy') && ok(); }} catch(e){{}}
                            document.body.removeChild(ta);
                        }}
                    }})();" style="background:#3b82f6;color:white;border:none;padding:4px 10px;border-radius:6px;cursor:pointer;font-size:12px;">📋</button>
                    </body></html>
                    """
                    components.html(copy_btn_html, height=40, scrolling=False)
            
            if body:
                st.markdown(body)


class ReportGenerator:
    """报告生成器"""
    
    @staticmethod
    def generate_stock_summary_report(
        code: str,
        name: str,
        stock_data: pd.DataFrame,
        quant_metrics: Dict = None,
        risk_metrics: Dict = None,
        alpha_factors: Dict = None,
        institutional_research: Dict = None,
        sentiment_analysis: Dict = None
    ) -> str:
        """
        生成个股汇总报告
        
        参数:
            code: 股票代码
            name: 股票名称
            stock_data: 股票数据
            quant_metrics: 量化指标
            risk_metrics: 风险指标
            alpha_factors: Alpha因子
            institutional_research: 机构研究
            sentiment_analysis: 舆情分析
        
        返回:
            Markdown格式的完整报告
        """
        report_lines = []
        
        # 标题
        report_lines.append(f"# 📊 {name} ({code}) - 投资分析报告")
        report_lines.append(f"\n**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("\n" + "="*60 + "\n")
        
        # 1. 基本行情
        if stock_data is not None and len(stock_data) > 0:
            report_lines.append("\n## 📈 基本行情")
            report_lines.append("")
            
            current_price = stock_data['Close'].iloc[-1]
            prev_close = stock_data['Close'].iloc[-2] if len(stock_data) > 1 else current_price
            change = current_price - prev_close
            change_pct = (change / prev_close * 100) if prev_close > 0 else 0
            
            report_lines.append(f"- **当前价格**: ${current_price:.2f}")
            report_lines.append(f"- **涨跌幅**: {change_pct:+.2f}% ({change:+.2f})")
            report_lines.append(f"- **成交量**: {stock_data['Volume'].iloc[-1]:,.0f}")
            
            # 均线
            if len(stock_data) >= 20:
                ma5 = stock_data['Close'].rolling(5).mean().iloc[-1]
                ma20 = stock_data['Close'].rolling(20).mean().iloc[-1]
                report_lines.append(f"- **MA5**: ${ma5:.2f}")
                report_lines.append(f"- **MA20**: ${ma20:.2f}")
                
                if current_price > ma20:
                    report_lines.append(f"- **趋势**: 📈 多头（价格在MA20上方）")
                else:
                    report_lines.append(f"- **趋势**: 📉 空头（价格在MA20下方）")
        
        # 2. 量化指标
        if quant_metrics:
            report_lines.append("\n## 📊 量化指标")
            report_lines.append("")
            
            sharpe = quant_metrics.get('sharpe_ratio', 'N/A')
            max_dd = quant_metrics.get('max_drawdown', 'N/A')
            win_rate = quant_metrics.get('win_rate', 'N/A')
            
            report_lines.append(f"- **夏普比率**: {sharpe}")
            report_lines.append(f"- **最大回撤**: {max_dd}")
            report_lines.append(f"- **胜率**: {win_rate}")
            
            rsi = quant_metrics.get('rsi', 'N/A')
            if rsi != 'N/A':
                try:
                    rsi_val = float(rsi)
                    if rsi_val > 70:
                        rsi_status = "⚠️ 超买"
                    elif rsi_val < 30:
                        rsi_status = "✅ 超卖"
                    else:
                        rsi_status = "📊 中性"
                    report_lines.append(f"- **RSI(14)**: {rsi} {rsi_status}")
                except:
                    report_lines.append(f"- **RSI(14)**: {rsi}")
        
        # 3. 风险指标
        if risk_metrics:
            report_lines.append("\n## ⚠️ 风险指标")
            report_lines.append("")
            
            alpha = risk_metrics.get('alpha', 'N/A')
            beta = risk_metrics.get('beta', 'N/A')
            volatility = risk_metrics.get('volatility', 'N/A')
            
            report_lines.append(f"- **Alpha**: {alpha}")
            report_lines.append(f"- **Beta**: {beta}")
            report_lines.append(f"- **波动率**: {volatility}")
            
            risk_grade = risk_metrics.get('risk_grade', 'N/A')
            report_lines.append(f"- **风险等级**: {risk_grade}")
        
        # 4. Alpha因子
        if alpha_factors:
            report_lines.append("\n## 💎 Alpha因子")
            report_lines.append("")
            
            for factor_name, factor_value in alpha_factors.items():
                if isinstance(factor_value, dict):
                    score = factor_value.get('score', 'N/A')
                    signal = factor_value.get('signal', '')
                    report_lines.append(f"- **{factor_name}**: {score} {signal}")
                else:
                    report_lines.append(f"- **{factor_name}**: {factor_value}")
        
        # 5. 机构研究
        if institutional_research:
            report_lines.append("\n## 🏦 机构研究")
            report_lines.append("")
            
            # 市场前瞻
            if 'market_outlook' in institutional_research:
                outlook = institutional_research['market_outlook']
                report_lines.append("### 🔮 市场前瞻")
                report_lines.append(f"- **趋势方向**: {outlook.get('trend_direction', 'N/A')}")
                report_lines.append(f"- **趋势强度**: {outlook.get('strength_score', 'N/A')}/100")
                report_lines.append(f"- **操作建议**: {outlook.get('recommendation', 'N/A')}")
                report_lines.append(f"- **置信度**: {outlook.get('confidence', 'N/A')}%")
            
            # 个股研究
            if 'stock_research' in institutional_research:
                research = institutional_research['stock_research']
                report_lines.append("\n### 📊 个股深度研究")
                
                # 趋势分析
                if 'trend_analysis' in research:
                    trend = research['trend_analysis']
                    report_lines.append(f"- **趋势状态**: {trend.get('status', 'N/A')}")
                    report_lines.append(f"- **趋势评分**: {trend.get('score', 'N/A')}/100")
                
                # 价格目标
                if 'price_target' in research:
                    target = research['price_target']
                    report_lines.append(f"- **目标高位**: ${target.get('target_high', 0):.2f}")
                    report_lines.append(f"- **目标低位**: ${target.get('target_low', 0):.2f}")
                    report_lines.append(f"- **止损价**: ${target.get('stop_loss', 0):.2f}")
            
            # 机会雷达
            if 'opportunity' in institutional_research:
                opp = institutional_research['opportunity']
                report_lines.append("\n### 🎯 机会雷达")
                report_lines.append(f"- **机会等级**: {opp.get('opportunity_level', 'N/A')}")
                report_lines.append(f"- **入场时机**: {opp.get('entry_timing', 'N/A')}")
                report_lines.append(f"- **建议仓位**: {opp.get('position_size_suggestion', 'N/A')}")
                report_lines.append(f"- **最优入场价**: ${opp.get('optimal_entry_price', 0):.2f}")
            
            # 风险预警
            if 'risk_warning' in institutional_research:
                risk = institutional_research['risk_warning']
                report_lines.append("\n### ⚠️ 风险预警")
                report_lines.append(f"- **风险等级**: {risk.get('risk_level', 'N/A')}")
                report_lines.append(f"- **风险评分**: {risk.get('risk_score', 'N/A')}/100")
                report_lines.append(f"- **止损价**: ${risk.get('stop_loss_price', 0):.2f}")
        
        # 6. 舆情分析
        if sentiment_analysis:
            report_lines.append("\n## 📰 舆情分析")
            report_lines.append("")
            
            metrics = sentiment_analysis.get('metrics', {})
            report_lines.append(f"- **舆情评分**: {metrics.get('sentiment_score', 'N/A')}/100")
            report_lines.append(f"- **舆情等级**: {metrics.get('sentiment_level', 'N/A')}")
            report_lines.append(f"- **短期影响**: {metrics.get('short_term_impact', 'N/A')}")
            
            # AI分析摘要
            response = sentiment_analysis.get('response', '')
            if response:
                report_lines.append("\n### 📑 AI舆情摘要")
                # 提取前300字作为摘要
                summary = response[:300] + "..." if len(response) > 300 else response
                report_lines.append(summary)
        
        # 7. 综合评级
        report_lines.append("\n## 🎖️ 综合评级")
        report_lines.append("")
        
        # 计算综合评分
        score_components = []
        
        if quant_metrics:
            sharpe = quant_metrics.get('sharpe_ratio', 'N/A')
            if sharpe != 'N/A':
                try:
                    sharpe_val = float(str(sharpe).replace('%', ''))
                    if sharpe_val > 1:
                        score_components.append(('量化指标', 80))
                    elif sharpe_val > 0:
                        score_components.append(('量化指标', 60))
                    else:
                        score_components.append(('量化指标', 40))
                except:
                    pass
        
        if sentiment_analysis:
            metrics = sentiment_analysis.get('metrics', {})
            sent_score = metrics.get('sentiment_score', 50)
            score_components.append(('舆情面', sent_score))
        
        if institutional_research and 'opportunity' in institutional_research:
            opp_level = institutional_research['opportunity'].get('opportunity_level', '中')
            if '高' in opp_level:
                score_components.append(('机会面', 80))
            elif '中' in opp_level:
                score_components.append(('机会面', 60))
            else:
                score_components.append(('机会面', 40))
        
        if score_components:
            avg_score = sum(s for _, s in score_components) / len(score_components)
            report_lines.append(f"- **综合评分**: {avg_score:.1f}/100")
            report_lines.append("")
            for name, score in score_components:
                report_lines.append(f"  - {name}: {score}/100")
            
            # 投资建议
            if avg_score >= 75:
                recommendation = "🟢 **强烈推荐** - 技术面、基本面、舆情面均较优"
            elif avg_score >= 60:
                recommendation = "🟡 **建议关注** - 整体表现尚可，可适当配置"
            elif avg_score >= 45:
                recommendation = "⚪ **中性观望** - 存在一定风险，建议观望"
            else:
                recommendation = "🔴 **谨慎对待** - 多项指标偏弱，建议回避"
            
            report_lines.append(f"\n- **投资建议**: {recommendation}")
        
        # 8. 风险提示
        report_lines.append("\n## ⚠️ 风险提示")
        report_lines.append("")
        report_lines.append("1. 本报告仅供参考，不构成投资建议")
        report_lines.append("2. 投资有风险，入市需谨慎")
        report_lines.append("3. 请结合自身风险承受能力做出投资决策")
        report_lines.append("4. 建议设置止损，控制仓位")
        
        # 9. 免责声明
        report_lines.append("\n" + "-"*60)
        report_lines.append("*本报告由StockAI系统自动生成，数据来源于公开市场信息和AI分析*")
        report_lines.append(f"*生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
        
        return "\n".join(report_lines)
    
    @staticmethod
    def generate_market_summary_report(market_name: str, market_data: Dict) -> str:
        """
        生成市场汇总报告
        
        参数:
            market_name: 市场名称（美股/港股/A股）
            market_data: 市场数据
        
        返回:
            Markdown格式的市场报告
        """
        report_lines = []
        
        # 标题
        report_lines.append(f"# 🌍 {market_name}市场分析报告")
        report_lines.append(f"\n**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("\n" + "="*60 + "\n")
        
        # 市场体制
        if market_data:
            report_lines.append("## 📊 市场体制")
            report_lines.append("")
            
            verdict = market_data.get('verdict', 'Unknown')
            if verdict == 'Risk On':
                verdict_icon = "🟢"
                verdict_desc = "风险偏好（建议进攻）"
            elif verdict == 'Risk Off':
                verdict_icon = "🔴"
                verdict_desc = "风险规避（建议防守）"
            else:
                verdict_icon = "🟡"
                verdict_desc = "中性观望"
            
            report_lines.append(f"- **市场体制**: {verdict_icon} {verdict} ({verdict_desc})")
            report_lines.append(f"- **裁决依据**: {market_data.get('reason', 'N/A')}")
            
            # 技术指标
            if 'spy_price' in market_data:
                report_lines.append(f"\n### 📈 技术指标")
                report_lines.append(f"- **SPY价格**: ${market_data.get('spy_price', 0):.2f}")
                report_lines.append(f"- **MA50**: ${market_data.get('ma50', 0):.2f}")
                report_lines.append(f"- **MA200**: ${market_data.get('ma200', 0):.2f}")
                report_lines.append(f"- **VIX指数**: {market_data.get('vix_level', 0):.2f}")
                report_lines.append(f"- **VIX状态**: {market_data.get('vix_status', 'N/A')}")
                report_lines.append(f"- **股债相关性**: {market_data.get('correlation', 0):.2f}")
            
            elif 'index_level' in market_data:
                report_lines.append(f"\n### 📈 技术指标")
                report_lines.append(f"- **指数点位**: {market_data.get('index_level', 0):.0f}")
                report_lines.append(f"- **MA50**: {market_data.get('ma50', 0):.0f}")
                report_lines.append(f"- **MA200**: {market_data.get('ma200', 0):.0f}")
                report_lines.append(f"- **波动率**: {market_data.get('volatility', 0):.1f}%")
                report_lines.append(f"- **波动状态**: {market_data.get('vol_status', 'N/A')}")
        
        # 投资策略
        report_lines.append("\n## 🎯 投资策略")
        report_lines.append("")
        
        if verdict == 'Risk On':
            report_lines.append("- **仓位建议**: 重仓配置")
            report_lines.append("- **板块配置**: 进攻性板块（科技、消费、金融）")
            report_lines.append("- **风控措施**: 适当放宽止损，追随趋势")
        elif verdict == 'Risk Off':
            report_lines.append("- **仓位建议**: 轻仓或空仓")
            report_lines.append("- **板块配置**: 防御性板块（公用事业、医疗保健、必需消费品）")
            report_lines.append("- **风控措施**: 严格止损，保护利润")
        else:
            report_lines.append("- **仓位建议**: 中等仓位")
            report_lines.append("- **板块配置**: 均衡配置")
            report_lines.append("- **风控措施**: 正常止损，观察趋势")
        
        # 风险提示
        report_lines.append("\n## ⚠️ 风险提示")
        report_lines.append("")
        report_lines.append("1. 市场体制可能快速变化，需密切关注")
        report_lines.append("2. 宏观数据和突发事件可能改变市场格局")
        report_lines.append("3. 建议分散投资，不要集中持仓")
        report_lines.append("4. 定期评估风险，及时调整策略")
        
        # 免责声明
        report_lines.append("\n" + "-"*60)
        report_lines.append("*本报告由StockAI系统自动生成*")
        report_lines.append(f"*生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
        
        return "\n".join(report_lines)


class ShareCardGenerator:
    """
    【V90.2 白底专业版】个股分析分享卡片
    iPhone 17 Pro Max: 1170 x 2532 px
    设计：白色背景 + 彩色卡片 + 走势图 + 评分 + AI简报 + 全覆盖布局
    """
    W = 1170
    H = 2532
    PAD = 45  # 统一左右边距

    @classmethod
    def _font(cls, size):
        from PIL import ImageFont
        for fp in ["/System/Library/Fonts/PingFang.ttc",
                   "/System/Library/Fonts/STHeiti Light.ttc",
                   "/System/Library/Fonts/Helvetica.ttc"]:
            if os.path.exists(fp):
                try:
                    return ImageFont.truetype(fp, size)
                except Exception:
                    continue
        return ImageFont.load_default()

    @classmethod
    def _rrect(cls, draw, box, fill, r=20, outline=None):
        """圆角矩形"""
        x1, y1, x2, y2 = box
        draw.rounded_rectangle(box, radius=r, fill=fill, outline=outline)

    @classmethod
    def _gradient_bg(cls, img):
        """绘制全屏深色渐变背景（兼容旧调用）"""
        cls._gradient_bg_adaptive(img, cls.W, cls.H)

    @classmethod
    def _sparkline(cls, draw, prices, box, up_color, down_color):
        """绘制迷你走势折线图（带渐变填充区域）"""
        if not prices or len(prices) < 3:
            return
        x1, y1, x2, y2 = box
        w = x2 - x1
        h = y2 - y1
        mn, mx = min(prices), max(prices)
        if mx == mn:
            mx = mn + 1
        pts = []
        for i, p in enumerate(prices):
            px = x1 + int(i / (len(prices) - 1) * w)
            py = y2 - int((p - mn) / (mx - mn) * h)
            pts.append((px, py))
        # 判断涨跌色
        line_color = up_color if prices[-1] >= prices[0] else down_color
        fade_color = (line_color[0], line_color[1], line_color[2])
        # 填充区域（半透明效果用渐变线模拟）
        for i in range(len(pts) - 1):
            ax, ay = pts[i]
            bx, by = pts[i + 1]
            # 从折线到底部画竖线（渐变）
            for lx in range(ax, bx + 1):
                if bx == ax:
                    ly = ay
                else:
                    ly = ay + (by - ay) * (lx - ax) // (bx - ax)
                for fy in range(ly, y2, 4):
                    alpha_ratio = 1 - (fy - ly) / max(y2 - ly, 1)
                    fc = (fade_color[0], fade_color[1], fade_color[2])
                    if alpha_ratio > 0.3:
                        draw.point((lx, fy), fill=fc)
        # 折线本身
        for i in range(len(pts) - 1):
            draw.line([pts[i], pts[i + 1]], fill=line_color, width=4)
        # 最后一个点高亮
        lx, ly = pts[-1]
        draw.ellipse([lx - 8, ly - 8, lx + 8, ly + 8], fill=line_color)
        draw.ellipse([lx - 4, ly - 4, lx + 4, ly + 4], fill=(255, 255, 255))

    @classmethod
    def _score_arc(cls, draw, center, radius, score, max_score=100):
        """绘制评分仪表盘弧线"""
        cx, cy = center
        import math
        # 底色弧（灰）
        for deg in range(-210, 30, 2):
            rad = math.radians(deg)
            x = cx + int(radius * math.cos(rad))
            y = cy + int(radius * math.sin(rad))
            draw.ellipse([x - 3, y - 3, x + 3, y + 3], fill=(40, 50, 70))
        # 评分弧（彩色）
        ratio = score / max_score if max_score > 0 else 0
        end_deg = -210 + int(240 * ratio)
        color = (16, 185, 129) if score >= 70 else ((245, 158, 11) if score >= 50 else (239, 68, 68))
        for deg in range(-210, end_deg, 2):
            rad = math.radians(deg)
            x = cx + int(radius * math.cos(rad))
            y = cy + int(radius * math.sin(rad))
            draw.ellipse([x - 5, y - 5, x + 5, y + 5], fill=color)
        return color

    @classmethod
    def _price_range_bar(cls, draw, box, stop_loss, entry, tp1, tp2):
        """绘制价格区间条（止损—入场—止盈1—止盈2）"""
        x1, y1, x2, y2 = box
        w = x2 - x1
        h = y2 - y1
        all_prices = [p for p in [stop_loss, entry, tp1, tp2] if p > 0]
        if len(all_prices) < 2:
            return
        mn = min(all_prices) * 0.995
        mx = max(all_prices) * 1.005
        rng = mx - mn if mx > mn else 1

        # 背景条
        cls._rrect(draw, (x1, y1 + h // 2 - 8, x2, y1 + h // 2 + 8), (40, 50, 70), r=8)

        def _xpos(price):
            return x1 + int((price - mn) / rng * w)

        # 止损到入场区（红色渐变）
        if stop_loss > 0:
            sx = _xpos(stop_loss)
            ex = _xpos(entry)
            cls._rrect(draw, (sx, y1 + h // 2 - 8, ex, y1 + h // 2 + 8), (239, 68, 68), r=8)
        # 入场到止盈1（绿色渐变）
        if tp1 > 0:
            sx = _xpos(entry)
            ex = _xpos(tp1)
            cls._rrect(draw, (sx, y1 + h // 2 - 8, ex, y1 + h // 2 + 8), (16, 185, 129), r=8)
        # 止盈1到止盈2（蓝色渐变）
        if tp2 > 0 and tp1 > 0:
            sx = _xpos(tp1)
            ex = _xpos(tp2)
            cls._rrect(draw, (sx, y1 + h // 2 - 8, ex, y1 + h // 2 + 8), (59, 130, 246), r=8)

        font_s = cls._font(24)
        font_v = cls._font(28)
        # 标注点
        labels = []
        if stop_loss > 0:
            labels.append((stop_loss, f"{stop_loss:.1f}", "止损", (239, 68, 68)))
        labels.append((entry, f"{entry:.1f}", "入场", (255, 255, 255)))
        if tp1 > 0:
            labels.append((tp1, f"{tp1:.1f}", "止盈1", (16, 185, 129)))
        if tp2 > 0:
            labels.append((tp2, f"{tp2:.1f}", "止盈2", (59, 130, 246)))

        for price, val_str, lbl, color in labels:
            px = _xpos(price)
            # 圆点
            draw.ellipse([px - 10, y1 + h // 2 - 10, px + 10, y1 + h // 2 + 10], fill=color)
            draw.ellipse([px - 5, y1 + h // 2 - 5, px + 5, y1 + h // 2 + 5], fill=(15, 20, 40))
            # 价格标注（上方）
            draw.text((px - 40, y1), val_str, fill=color, font=font_v)
            # 标签（下方）
            draw.text((px - 25, y1 + h - 30), lbl, fill=(148, 163, 184), font=font_s)

    @classmethod
    def _wrapped(cls, draw, text, pos, max_w, fill, font, spacing=10, max_lines=5):
        """自动换行"""
        x, y = pos
        line = ""
        n = 0
        for ch in text:
            test = line + ch
            bx = draw.textbbox((0, 0), test, font=font)
            if bx[2] - bx[0] <= max_w:
                line = test
            else:
                draw.text((x, y), line, fill=fill, font=font)
                y += bx[3] - bx[1] + spacing
                line = ch
                n += 1
                if n >= max_lines:
                    line += "..."
                    break
        if line:
            draw.text((x, y), line, fill=fill, font=font)
        return y

    @classmethod
    def generate_stock_card(cls, code, price, change_pct,
                            score=0, suggestion="",
                            vwap=0, vwap_dev=0, atr=0,
                            stop_loss=0, kelly_pct=0, risk_grade="",
                            entry_grade="", ai_stop_loss=0,
                            ai_tp1=0, ai_tp2=0, ai_strategy="",
                            macro_verdict="", position_cap=80,
                            pre_mortem_risks=None,
                            recent_prices=None,
                            market_brief="") -> bytes:
        """
        【V90.2白底版】生成专业分享卡片（白底+彩色卡片+填满整个画布）
        新增: market_brief参数用于AI市场简报
        """
        from PIL import Image, ImageDraw

        W, P = cls.W, cls.PAD
        CW = W - P * 2

        # 字体
        f120 = cls._font(120)
        f60 = cls._font(60)
        f52 = cls._font(52)
        f44 = cls._font(44)
        f40 = cls._font(40)
        f38 = cls._font(38)
        f32 = cls._font(32)
        f28 = cls._font(28)
        f24 = cls._font(24)
        f20 = cls._font(20)

        # 白底配色
        BG_WHITE = (255, 255, 255)
        TEXT_DARK = (30, 41, 59)       # 主文字深蓝灰
        TEXT_MID = (71, 85, 105)       # 次级文字
        TEXT_LIGHT = (148, 163, 184)   # 浅灰
        CARD_BG = (249, 250, 251)      # 卡片浅灰底
        CARD_BORDER = (226, 232, 240)  # 卡片边框
        GREEN = (16, 185, 129)
        RED = (239, 68, 68)
        GOLD = (234, 179, 8)
        BLUE = (59, 130, 246)
        PURPLE = (139, 92, 246)
        AMBER = (245, 158, 11)
        CYAN = (6, 182, 212)
        ORANGE = (251, 146, 60)

        # ──  动态计算高度（确保填满） ──
        y_calc = 70 + 520 + 35 + 440 + 35  # 品牌+主卡+间距+指标+间距
        
        has_ai = ai_stop_loss > 0 or ai_tp1 > 0
        if has_ai:
            y_calc += 500 + 35
        
        if ai_strategy:
            y_calc += 320 + 35
        
        if market_brief:
            y_calc += 450 + 35  # AI市场简报区
        
        if pre_mortem_risks and len(pre_mortem_risks) > 0:
            y_calc += 70 + min(len(pre_mortem_risks), 3) * 105 + 35
        
        # 持仓建议 + 交易纪律（新增）
        y_calc += 400 + 35
        
        # 底部水印
        y_calc += 180

        # 确保至少2532或内容撑大
        H = max(cls.H, y_calc)

        img = Image.new('RGB', (W, H), BG_WHITE)
        cls._gradient_bg_adaptive(img, W, H)
        draw = ImageDraw.Draw(img)

        y = 55

        # ════════════════════════════════════════════════════════════
        # 顶部品牌栏
        # ════════════════════════════════════════════════════════════
        draw.text((P, y), "StockAI", fill=PURPLE, font=f60)
        draw.text((P + 310, y + 16), "机构级智能分析", fill=TEXT_MID, font=f32)
        now_str = datetime.now().strftime("%Y/%m/%d  %H:%M")
        draw.text((W - P - 350, y + 16), now_str, fill=TEXT_LIGHT, font=f28)
        y += 90
        
        # 分隔线（紫色渐变）
        for i in range(4):
            alpha_ratio = (4 - i) / 4
            c = (int(139 + (255 - 139) * (1 - alpha_ratio)), 
                 int(92 + (163 - 92) * (1 - alpha_ratio)), 
                 246)
            draw.line([(P, y + i), (W - P, y + i)], fill=c)
        y += 20

        # ════════════════════════════════════════════════════════════
        # 主卡片 —— 代码+价格+走势+评分+宏观
        # ════════════════════════════════════════════════════════════
        main_h = 520
        # 绘制卡片阴影（模拟浮起效果）
        shadow_offset = 6
        cls._rrect(draw, (P + shadow_offset, y + shadow_offset, W - P + shadow_offset, y + main_h + shadow_offset), 
                   (200, 200, 200), r=30)
        cls._rrect(draw, (P, y, W - P, y + main_h), CARD_BG, r=30, outline=CARD_BORDER)

        # 股票代码
        draw.text((P + 40, y + 28), code, fill=TEXT_DARK, font=f60)

        # 大价格
        price_c = GREEN if change_pct >= 0 else RED
        draw.text((P + 40, y + 110), f"{price:.2f}", fill=price_c, font=f120)
        
        # 涨跌标签
        sign = "+" if change_pct >= 0 else ""
        chg_txt = f"{sign}{change_pct:.2f}%"
        chg_x1 = P + 40
        chg_y1 = y + 260
        chg_x2 = P + 270
        chg_y2 = y + 320
        cls._rrect(draw, (chg_x1, chg_y1, chg_x2, chg_y2), price_c, r=12)
        draw.text((chg_x1 + 50, chg_y1 + 12), chg_txt, fill=BG_WHITE, font=f38)

        # 右侧走势图
        spark_x1 = W // 2 + 15
        spark_box = (spark_x1, y + 35, W - P - 40, y + 250)
        if recent_prices and len(recent_prices) >= 5:
            cls._sparkline(draw, recent_prices, spark_box, GREEN, RED)
            _hi = max(recent_prices)
            _lo = min(recent_prices)
            draw.text((spark_x1, y + 260), f"近{len(recent_prices)}日走势", fill=TEXT_LIGHT, font=f24)
            draw.text((spark_x1, y + 295), f"高 {_hi:.2f}   低 {_lo:.2f}", fill=TEXT_MID, font=f28)
        else:
            draw.text((spark_x1 + 100, y + 140), "暂无走势数据", fill=TEXT_LIGHT, font=f32)

        # 底部行：评分+宏观
        bottom_y = y + 350

        # 评分仪表盘
        if score > 0:
            arc_cx = P + 150
            arc_cy = bottom_y + 70
            arc_color = cls._score_arc(draw, (arc_cx, arc_cy), 65, score)
            draw.text((arc_cx - 38, arc_cy - 22), f"{score}", fill=arc_color, font=f52)
            draw.text((arc_cx + 60, arc_cy - 14), "/100", fill=TEXT_LIGHT, font=f28)
            _sug = suggestion[:10] if suggestion else ""
            if _sug:
                draw.text((arc_cx + 150, arc_cy - 12), _sug, fill=TEXT_MID, font=f38)

        # 宏观环境标签
        if macro_verdict:
            v_c = GREEN if "On" in macro_verdict or "偏好" in macro_verdict else (RED if "Off" in macro_verdict or "避险" in macro_verdict else AMBER)
            _mv = macro_verdict[:16]
            mac_x1 = spark_x1
            mac_y1 = bottom_y + 12
            mac_x2 = W - P - 40
            mac_y2 = bottom_y + 155
            cls._rrect(draw, (mac_x1, mac_y1, mac_x2, mac_y2), BG_WHITE, r=18, outline=v_c)
            draw.text((mac_x1 + 22, mac_y1 + 14), "宏观环境", fill=TEXT_LIGHT, font=f24)
            draw.text((mac_x1 + 22, mac_y1 + 50), _mv, fill=v_c, font=f38)
            draw.text((mac_x1 + 22, mac_y1 + 98), f"建议仓位 ≤ {position_cap}%", fill=TEXT_MID, font=f28)

        y += main_h + 35

        # ════════════════════════════════════════════════════════════
        # 六宫格核心指标
        # ════════════════════════════════════════════════════════════
        ind_h = 440
        cls._rrect(draw, (P + shadow_offset, y + shadow_offset, W - P + shadow_offset, y + ind_h + shadow_offset), 
                   (200, 200, 200), r=30)
        cls._rrect(draw, (P, y, W - P, y + ind_h), CARD_BG, r=30, outline=CARD_BORDER)
        draw.text((P + 40, y + 20), "核心指标", fill=TEXT_DARK, font=f44)
        draw.line([(P + 230, y + 42), (W - P - 40, y + 42)], fill=CARD_BORDER, width=3)

        col_w = CW // 3
        row_h = 170
        metrics = [
            ("VWAP", f"{vwap:.2f}" if vwap else "--", "机构成本线", GOLD),
            ("偏离度", f"{vwap_dev:+.1f}%" if vwap_dev else "--", "距VWAP偏离", GREEN if vwap_dev and vwap_dev > 0 else RED),
            ("ATR(14)", f"{atr:.2f}" if atr else "--", "日均波幅", BLUE),
            ("硬止损", f"{stop_loss:.2f}" if stop_loss else "--", "基于ATR", RED),
            ("Kelly仓位", f"{kelly_pct:.1f}%" if kelly_pct else "--", "最优配比", PURPLE),
            ("风险等级", (risk_grade[:6] if risk_grade else "--"), "A最优", AMBER),
        ]
        for i, (label, val, desc, color) in enumerate(metrics):
            col = i % 3
            row = i // 3
            mx = P + 40 + col * col_w
            my = y + 75 + row * row_h
            draw.text((mx, my), label, fill=TEXT_LIGHT, font=f28)
            draw.text((mx, my + 38), str(val), fill=color, font=f52)
            draw.text((mx, my + 100), desc, fill=TEXT_MID, font=f24)
            # 进度条
            bar_w = col_w - 75
            draw.line([(mx, my + 130), (mx + bar_w, my + 130)], fill=CARD_BORDER, width=6)
            if i == 4 and kelly_pct:
                fill_w = int(bar_w * min(kelly_pct / 100, 1))
                if fill_w > 0:
                    draw.line([(mx, my + 130), (mx + fill_w, my + 130)], fill=color, width=6)

        y += ind_h + 35

        # ════════════════════════════════════════════════════════════
        # AI止损止盈
        # ════════════════════════════════════════════════════════════
        if has_ai:
            ai_h = 500
            cls._rrect(draw, (P + shadow_offset, y + shadow_offset, W - P + shadow_offset, y + ai_h + shadow_offset), 
                       (200, 200, 200), r=30)
            cls._rrect(draw, (P, y, W - P, y + ai_h), CARD_BG, r=30, outline=CARD_BORDER)
            draw.text((P + 40, y + 20), "AI 止损止盈方案", fill=CYAN, font=f44)
            draw.line([(P + 380, y + 42), (W - P - 40, y + 42)], fill=CARD_BORDER, width=3)

            if entry_grade:
                g_c = GREEN if 'A' in entry_grade else (BLUE if 'B' in entry_grade else (AMBER if 'C' in entry_grade else RED))
                cls._rrect(draw, (W - P - 240, y + 12, W - P - 40, y + 62), g_c, r=12)
                draw.text((W - P - 225, y + 20), f"入场 {entry_grade[:3]}", fill=BG_WHITE, font=f32)

            ty = y + 80
            third = CW // 3
            pairs = []
            if ai_stop_loss > 0:
                pct = ((price - ai_stop_loss) / price * 100) if price > 0 else 0
                pairs.append(("止损", f"{ai_stop_loss:.2f}", f"-{pct:.1f}%", RED))
            if ai_tp1 > 0:
                pct = ((ai_tp1 - price) / price * 100) if price > 0 else 0
                pairs.append(("止盈 保守", f"{ai_tp1:.2f}", f"+{pct:.1f}%", GREEN))
            if ai_tp2 > 0:
                pct = ((ai_tp2 - price) / price * 100) if price > 0 else 0
                pairs.append(("止盈 激进", f"{ai_tp2:.2f}", f"+{pct:.1f}%", BLUE))

            for idx, (lbl, val, pct_s, clr) in enumerate(pairs):
                mx = P + 40 + idx * third
                draw.text((mx, ty), lbl, fill=TEXT_LIGHT, font=f28)
                draw.text((mx, ty + 40), val, fill=clr, font=f52)
                draw.text((mx, ty + 105), pct_s, fill=clr, font=f38)

            ty += 170
            if ai_stop_loss > 0 and ai_tp1 > 0:
                sl_s = price - ai_stop_loss
                tp_s = ai_tp1 - price
                rr = tp_s / sl_s if sl_s > 0 else 0
                rr_c = GREEN if rr >= 2 else (AMBER if rr >= 1.5 else RED)
                draw.text((P + 40, ty), "盈亏比", fill=TEXT_LIGHT, font=f28)
                draw.text((P + 170, ty - 8), f"{rr:.1f} : 1", fill=rr_c, font=f44)
                rr_desc = "优秀" if rr >= 2 else ("合格" if rr >= 1.5 else "偏低")
                draw.text((P + 380, ty), rr_desc, fill=rr_c, font=f32)

            bar_y = ty + 70
            cls._price_range_bar(draw, (P + 40, bar_y, W - P - 40, bar_y + 110),
                                 ai_stop_loss, price, ai_tp1, ai_tp2)
            y += ai_h + 35

        # ════════════════════════════════════════════════════════════
        # AI策略总结
        # ════════════════════════════════════════════════════════════
        if ai_strategy:
            strat_h = 320
            cls._rrect(draw, (P + shadow_offset, y + shadow_offset, W - P + shadow_offset, y + strat_h + shadow_offset), 
                       (200, 200, 200), r=30)
            cls._rrect(draw, (P, y, W - P, y + strat_h), CARD_BG, r=30, outline=CARD_BORDER)
            # 紫色侧边条
            draw.rectangle([(P, y + 25), (P + 8, y + strat_h - 25)], fill=PURPLE)
            draw.text((P + 40, y + 22), "AI 策略总结", fill=PURPLE, font=f44)
            draw.line([(P + 290, y + 44), (W - P - 40, y + 44)], fill=CARD_BORDER, width=3)
            cls._wrapped(draw, ai_strategy, (P + 40, y + 75), CW - 80, TEXT_DARK, f32, spacing=18, max_lines=6)
            y += strat_h + 35

        # ════════════════════════════════════════════════════════════
        # AI市场简报（新增）
        # ════════════════════════════════════════════════════════════
        if market_brief:
            brief_h = 450
            cls._rrect(draw, (P + shadow_offset, y + shadow_offset, W - P + shadow_offset, y + brief_h + shadow_offset), 
                       (200, 200, 200), r=30)
            cls._rrect(draw, (P, y, W - P, y + brief_h), (255, 252, 245), r=30, outline=ORANGE)
            # 橙色侧边条
            draw.rectangle([(P, y + 25), (P + 8, y + brief_h - 25)], fill=ORANGE)
            draw.text((P + 40, y + 22), "AI 市场简报", fill=ORANGE, font=f44)
            draw.line([(P + 290, y + 44), (W - P - 40, y + 44)], fill=(251, 220, 180), width=3)
            # 市场简报内容（多行）
            cls._wrapped(draw, market_brief, (P + 40, y + 75), CW - 80, TEXT_DARK, f28, spacing=16, max_lines=12)
            y += brief_h + 35

        # ════════════════════════════════════════════════════════════
        # 风控官警告
        # ════════════════════════════════════════════════════════════
        if pre_mortem_risks and len(pre_mortem_risks) > 0:
            n_risks = min(len(pre_mortem_risks), 3)
            rh = 70 + n_risks * 105
            cls._rrect(draw, (P + shadow_offset, y + shadow_offset, W - P + shadow_offset, y + rh + shadow_offset), 
                       (200, 200, 200), r=30)
            cls._rrect(draw, (P, y, W - P, y + rh), (254, 242, 242), r=30, outline=RED)
            # 红色侧边条
            draw.rectangle([(P, y + 25), (P + 8, y + rh - 25)], fill=RED)
            draw.text((P + 40, y + 20), "风控官警告", fill=RED, font=f44)
            draw.line([(P + 260, y + 42), (W - P - 40, y + 42)], fill=(254, 202, 202), width=3)
            ry = y + 75
            for i in range(n_risks):
                txt = pre_mortem_risks[i]
                txt = txt[:48] + "..." if len(txt) > 48 else txt
                # 编号圆点
                cx = P + 68
                cy = ry + 18
                draw.ellipse([cx - 16, cy - 16, cx + 16, cy + 16], fill=RED)
                draw.text((cx - 8, cy - 14), f"{i + 1}", fill=BG_WHITE, font=f24)
                draw.text((P + 100, ry), txt, fill=ORANGE, font=f32)
                ry += 105
            y += rh + 35

        # ════════════════════════════════════════════════════════════
        # 持仓建议 + 交易纪律（新增填充空白）
        # ════════════════════════════════════════════════════════════
        disc_h = 400
        cls._rrect(draw, (P + shadow_offset, y + shadow_offset, W - P + shadow_offset, y + disc_h + shadow_offset), 
                   (200, 200, 200), r=30)
        cls._rrect(draw, (P, y, W - P, y + disc_h), (240, 253, 244), r=30, outline=GREEN)
        draw.text((P + 40, y + 20), "交易纪律 & 持仓建议", fill=GREEN, font=f44)
        draw.line([(P + 450, y + 42), (W - P - 40, y + 42)], fill=(167, 243, 208), width=3)

        disc_y = y + 75
        # 4宫格纪律
        disciplines = [
            ("仓位管理", f"本次建议 {kelly_pct:.1f}%" if kelly_pct else "建议 20-30%", "分批建仓，不满仓"),
            ("严守止损", f"{stop_loss:.2f}" if stop_loss else "设置止损", "触及立即平仓"),
            ("止盈策略", "分批止盈" if ai_tp1 else "设置目标位", "不贪婪不恐惧"),
            ("风险控制", f"单笔最大亏损 {kelly_pct * 0.5:.1f}%" if kelly_pct else "控制回撤", "保护本金第一"),
        ]
        half = CW // 2
        for idx, (title, val, desc) in enumerate(disciplines):
            col = idx % 2
            row = idx // 2
            dx = P + 40 + col * half
            dy = disc_y + row * 145
            draw.text((dx, dy), title, fill=TEXT_LIGHT, font=f28)
            draw.text((dx, dy + 38), val, fill=GREEN, font=f40)
            draw.text((dx, dy + 92), desc, fill=TEXT_MID, font=f24)

        y += disc_h + 35

        # ════════════════════════════════════════════════════════════
        # 底部品牌 + 免责
        # ════════════════════════════════════════════════════════════
        footer_h = 180
        actual_h = max(H, y + footer_h)
        footer_y = actual_h - footer_h

        # 如果高度超过初始，重建画布
        if actual_h > H:
            new_img = Image.new('RGB', (W, actual_h), BG_WHITE)
            cls._gradient_bg_adaptive(new_img, W, actual_h)
            new_draw = ImageDraw.Draw(new_img)
            new_img.paste(img, (0, 0))
            img = new_img
            draw = new_draw
            H = actual_h

        # 分隔线
        for i in range(4):
            alpha_ratio = i / 4
            c = (int(148 + (59 - 148) * alpha_ratio), 
                 int(163 + (130 - 163) * alpha_ratio), 
                 246)
            draw.line([(P, footer_y + i), (W - P, footer_y + i)], fill=c)
        footer_y += 24
        draw.text((P, footer_y), "StockAI", fill=PURPLE, font=f44)
        draw.text((P + 220, footer_y + 10), "机构级智能分析系统", fill=TEXT_LIGHT, font=f28)
        footer_y += 60
        draw.text((P, footer_y), "以上内容由AI生成，仅供参考，不构成投资建议", fill=TEXT_MID, font=f24)
        draw.text((W - P - 320, footer_y), datetime.now().strftime("%Y-%m-%d %H:%M"), fill=TEXT_LIGHT, font=f24)

        # 最终裁剪
        if H != cls.H:
            img = img.crop((0, 0, W, actual_h))

        buf = io.BytesIO()
        img.save(buf, format='PNG', quality=95)
        buf.seek(0)
        return buf.getvalue()

    @classmethod
    def _gradient_bg_adaptive(cls, img, w, h):
        """白色渐变背景（顶部浅紫→底部纯白）"""
        from PIL import ImageDraw
        draw = ImageDraw.Draw(img)
        for i in range(h):
            ratio = i / h
            # 从浅紫(248,246,252) 渐变到 纯白(255,255,255)
            r = int(248 + 7 * ratio)
            g = int(246 + 9 * ratio)
            b = int(252 + 3 * ratio)
            draw.line([(0, i), (w, i)], fill=(r, g, b))
