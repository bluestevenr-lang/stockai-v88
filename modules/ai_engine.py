"""
AI分析引擎模块 - Gemini API集成 + Prompt管理
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
功能：
  - Gemini API调用
  - Prompt模板管理
  - 上下文对话支持
  - 错误重试机制
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import logging
import time
from typing import Optional, List, Dict, Any

from .config import GEMINI_API_KEY, GEMINI_MODEL_NAME

# 尝试导入Gemini API
try:
    import google.generativeai as genai
    HAS_GEMINI = True
    
    # 配置API
    if GEMINI_API_KEY:
        genai.configure(api_key=GEMINI_API_KEY)
except ImportError:
    HAS_GEMINI = False
    logging.warning("google-generativeai未安装，AI功能将不可用")


# ═══════════════════════════════════════════════════════════════
# Prompt模板管理
# ═══════════════════════════════════════════════════════════════

PROMPT_TEMPLATES = {
    # 市场简报
    "market_brief": """作为资深金融分析师，生成今日市场简报：

【市场概况】
- 美股：{us_status}
- 港股：{hk_status}
- A股：{cn_status}

【重要新闻】（至少2条，每条包含：新闻内容、影响分析、趋势判断）
{news_data}

【推荐股票】（每个市场3只，包含：股票名称、推荐理由、目标价）
- 美股：{us_stocks}
- 港股：{hk_stocks}
- A股：{cn_stocks}

要求：
1. 直接输出分析，无需开场白和结束语
2. 新闻分析要深入，不要一句话概括
3. 股票推荐要有详细理由和具体目标价
4. 语言简洁专业，避免冗余

请生成简报：""",

    # 个股舆情分析
    "stock_sentiment": """作为资深金融分析师，对 {stock_code} 进行全面舆情分析：

【当前行情数据】
- 当前价格: {current_price}
- 5日涨跌: {change_5d}%
- 20日涨跌: {change_20d}%
- RSI: {rsi}

【分析要求】
1. **舆情综述**：当前市场情绪和热度
2. **关键事件**：最近影响股价的重要事件
3. **机构观点**：主流机构的看法和评级
4. **风险提示**：需要关注的风险点
5. **操作建议**：具体的买卖建议和目标价

要求：
- 分析要具体，有数据支撑
- 避免模糊表述
- 给出明确的操作建议
- 字数控制在500字以内

请生成分析报告：""",

    # 行业分析
    "industry_analysis": """作为资深行业分析师，对 {market} {industry} 行业进行深度分析：

【分析维度】
1. **行业现状**：当前发展阶段和市场规模
2. **重要新闻**：最近影响行业的重大事件
3. **趋势判断**：未来3-6个月的发展趋势
4. **投资机会**：值得关注的细分领域
5. **推荐股票**：3-5只优质标的及理由

要求：
- 分析要专业深入
- 数据要准确
- 推荐要有依据
- 字数控制在600字以内

请生成行业分析：""",

    # 股票对比
    "stock_comparison": """作为专业投资顾问，请对比分析以下股票：

{stock_summary}

请给出：
1. 🏆 **综合排名**：从投资价值角度排序，说明理由
2. 📊 **各自优势**：每只股票的核心优势和适合的投资者
3. ⚠️ **风险对比**：各自的主要风险点
4. 💡 **配置建议**：如何组合配置这些股票

要求：
- 对比要客观公正
- 建议要具体可操作
- 字数控制在400字以内

请生成对比分析：""",

    # 通用问答
    "general_qa": """{stock_code} 当前价格 {current_price}。

问题：{question}

要求：
- 回答要简洁专业
- 给出具体建议
- 字数控制在200字以内

请回答：""",
}


# ═══════════════════════════════════════════════════════════════
# 核心API调用函数
# ═══════════════════════════════════════════════════════════════

def call_gemini_api(
    prompt: str,
    model_name: Optional[str] = None,
    max_retries: int = 3,
    retry_delay: float = 1.0
) -> str:
    """
    调用Gemini API生成内容
    
    Args:
        prompt: 提示词
        model_name: 模型名称（默认使用配置的模型）
        max_retries: 最大重试次数
        retry_delay: 重试延迟（秒）
        
    Returns:
        生成的文本，失败返回错误消息
    """
    if not HAS_GEMINI:
        return "❌ Gemini API未安装，请运行: pip install google-generativeai"
    
    if not GEMINI_API_KEY:
        return "❌ 未配置 Gemini API Key"
    
    model_name = model_name or GEMINI_MODEL_NAME
    
    for attempt in range(max_retries):
        try:
            model = genai.GenerativeModel(model_name)
            response = model.generate_content(prompt)
            
            if response and response.text:
                return response.text
            else:
                logging.warning(f"Gemini API返回空内容 (尝试 {attempt + 1}/{max_retries})")
        
        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)[:100]}"
            logging.error(f"Gemini API调用失败 (尝试 {attempt + 1}/{max_retries}): {error_msg}")
            
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))  # 递增延迟
                continue
            else:
                return f"❌ AI分析失败: {error_msg}"
    
    return "❌ AI分析失败: 达到最大重试次数"


# ═══════════════════════════════════════════════════════════════
# 高级功能：上下文对话
# ═══════════════════════════════════════════════════════════════

class ConversationContext:
    """
    对话上下文管理器
    
    功能：
    - 保存最近N轮对话历史
    - 自动构建上下文prompt
    - 支持清除历史
    """
    
    def __init__(self, max_history: int = 5):
        """
        初始化对话上下文
        
        Args:
            max_history: 最大保存历史轮数
        """
        self.max_history = max_history
        self.history: List[Dict[str, str]] = []
    
    def add_exchange(self, user_input: str, ai_response: str):
        """
        添加一轮对话
        
        Args:
            user_input: 用户输入
            ai_response: AI回复
        """
        self.history.append({
            'user': user_input,
            'assistant': ai_response
        })
        
        # 保持历史数量限制
        if len(self.history) > self.max_history:
            self.history.pop(0)
    
    def build_context_prompt(self, current_question: str) -> str:
        """
        构建包含上下文的prompt
        
        Args:
            current_question: 当前问题
            
        Returns:
            包含历史的完整prompt
        """
        if not self.history:
            return current_question
        
        context_parts = ["以下是对话历史：\n"]
        
        for idx, exchange in enumerate(self.history, 1):
            context_parts.append(f"第{idx}轮：")
            context_parts.append(f"用户: {exchange['user']}")
            context_parts.append(f"助手: {exchange['assistant'][:100]}...\n")  # 截断长回复
        
        context_parts.append(f"\n当前问题：{current_question}")
        
        return "\n".join(context_parts)
    
    def clear(self):
        """清除对话历史"""
        self.history.clear()
    
    def get_history(self) -> List[Dict[str, str]]:
        """获取对话历史"""
        return self.history.copy()


# ═══════════════════════════════════════════════════════════════
# 便捷函数：使用模板生成内容
# ═══════════════════════════════════════════════════════════════

def generate_market_brief(
    us_status: str,
    hk_status: str,
    cn_status: str,
    news_data: str,
    us_stocks: str,
    hk_stocks: str,
    cn_stocks: str
) -> str:
    """
    生成市场简报
    
    Args:
        us_status: 美股状态
        hk_status: 港股状态
        cn_status: A股状态
        news_data: 新闻数据
        us_stocks: 美股推荐
        hk_stocks: 港股推荐
        cn_stocks: A股推荐
        
    Returns:
        市场简报文本
    """
    prompt = PROMPT_TEMPLATES["market_brief"].format(
        us_status=us_status,
        hk_status=hk_status,
        cn_status=cn_status,
        news_data=news_data,
        us_stocks=us_stocks,
        hk_stocks=hk_stocks,
        cn_stocks=cn_stocks
    )
    
    return call_gemini_api(prompt)


def generate_stock_sentiment(
    stock_code: str,
    current_price: float,
    change_5d: float,
    change_20d: float,
    rsi: float
) -> str:
    """
    生成个股舆情分析
    
    Args:
        stock_code: 股票代码
        current_price: 当前价格
        change_5d: 5日涨跌幅
        change_20d: 20日涨跌幅
        rsi: RSI指标
        
    Returns:
        舆情分析文本
    """
    prompt = PROMPT_TEMPLATES["stock_sentiment"].format(
        stock_code=stock_code,
        current_price=f"{current_price:.2f}",
        change_5d=f"{change_5d:.2f}",
        change_20d=f"{change_20d:.2f}",
        rsi=f"{rsi:.1f}"
    )
    
    return call_gemini_api(prompt)


def generate_industry_analysis(
    market: str,
    industry: str
) -> str:
    """
    生成行业分析
    
    Args:
        market: 市场（美股/港股/A股）
        industry: 行业名称
        
    Returns:
        行业分析文本
    """
    prompt = PROMPT_TEMPLATES["industry_analysis"].format(
        market=market,
        industry=industry
    )
    
    return call_gemini_api(prompt)


def generate_stock_comparison(
    stock_summary: str
) -> str:
    """
    生成股票对比分析
    
    Args:
        stock_summary: 股票汇总信息
        
    Returns:
        对比分析文本
    """
    prompt = PROMPT_TEMPLATES["stock_comparison"].format(
        stock_summary=stock_summary
    )
    
    return call_gemini_api(prompt)


def generate_qa_response(
    stock_code: str,
    current_price: float,
    question: str
) -> str:
    """
    生成问答回复
    
    Args:
        stock_code: 股票代码
        current_price: 当前价格
        question: 用户问题
        
    Returns:
        回答文本
    """
    prompt = PROMPT_TEMPLATES["general_qa"].format(
        stock_code=stock_code,
        current_price=f"{current_price:.2f}",
        question=question
    )
    
    return call_gemini_api(prompt)


# ═══════════════════════════════════════════════════════════════
# 全局对话上下文实例
# ═══════════════════════════════════════════════════════════════

_global_conversation_context: Optional[ConversationContext] = None


def get_conversation_context() -> ConversationContext:
    """
    获取全局对话上下文（单例模式）
    
    Returns:
        对话上下文实例
    """
    global _global_conversation_context
    
    if _global_conversation_context is None:
        _global_conversation_context = ConversationContext(max_history=5)
    
    return _global_conversation_context
