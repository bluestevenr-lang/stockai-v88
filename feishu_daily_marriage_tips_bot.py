#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
31 天高密度婚姻修养 · 飞书卡片机器人（本地预览 / 审批 / 发送）

- 周期仅 31 天一轮（非 365）：晨课《道德经》线索、晚课《人性的弱点》递进脉络。
- 未 approve 的 slot，send 一律拒绝（严禁未确认发送）。
- 状态持久化：同目录 feishu_marriage_tips_state.json

环境变量：FEISHU_WEBHOOK_URL（仅 send 时需要）

命令：preview / regen / approve / unapprove / send / bootstrap
详见 python3 feishu_daily_marriage_tips_bot.py -h
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field, asdict
from datetime import date
from pathlib import Path
from typing import Any, Literal

try:
    import requests
except ImportError:
    requests = None  # type: ignore

STATE_PATH = Path(__file__).resolve().parent / "feishu_marriage_tips_state.json"
Slot = Literal["morning", "evening"]


# ═══════════════════════════════════════════════════════════════════════════
# 31 天课程主题框架（递进路径，非随机鸡汤）
# ═══════════════════════════════════════════════════════════════════════════

MORNING_PLAN: dict[int, dict[str, str]] = {
    1: {"theme": "无名与标签", "chapter": "第1–2章：道可道、美恶相生", "arc": "认识语言如何绑架亲密"},
    2: {"theme": "不争与家务权力", "chapter": "第3–5章：不尚贤、橐籥、天地不仁", "arc": "从「谁更对」转向「怎么过」"},
    3: {"theme": "虚与倾听", "chapter": "第6–8章：谷神、天长地久、上善若水", "arc": "留出空间，柔能承物"},
    4: {"theme": "知止与收声", "chapter": "第9–10章：持盈、载营魄抱一", "arc": "情绪峰值处刹车"},
    5: {"theme": "去眩与降噪", "chapter": "第11–13章：无用之用、五色、宠辱若惊", "arc": "少比较、少刺激，心才稳"},
    6: {"theme": "致虚守静", "chapter": "第14–16章：古之道、致虚极", "arc": "争执前先清空预设"},
    7: {"theme": "朴素与少私", "chapter": "第17–19章：太上、大道废、见素抱朴", "arc": "减控制、减面子工程"},
    8: {"theme": "曲全与让步", "chapter": "第20–22章：众人熙熙、孔德、曲则全", "arc": "非原则处弯一弯"},
    9: {"theme": "希言与自然", "chapter": "第23–25章：希言、道法自然、有物混成", "arc": "少唠叨，顺势沟通"},
    10: {"theme": "重静与守雌", "chapter": "第26–28章：轻则失根、善行无辙、知雄守雌", "arc": "稳重、低声、守柔"},
    11: {"theme": "物壮则老", "chapter": "第29–31章：取天下、物壮则老、兵者不祥", "arc": "别把关系逼到极限"},
    12: {"theme": "自知与自知者明", "chapter": "第32–34章：无名、自知者明、大道泛兮", "arc": "先认自己的盲点"},
    13: {"theme": "无为而无不为", "chapter": "第35–37章：执大象、道常无为", "arc": "抓大放小，守住底线即可"},
    14: {"theme": "上德不德", "chapter": "第38–40章：上德、失道、反者道之动", "arc": "少表演「我是好人」"},
    15: {"theme": "柔弱与穿坚", "chapter": "第41–43章：上士闻道、三生万物、天下至柔", "arc": "软话、慢语速的力量"},
    16: {"theme": "名身与大成若缺", "chapter": "第44–46章：名与身、大成若缺、天下有道", "arc": "要人还是要赢"},
    17: {"theme": "为道日损", "chapter": "第47–49章：不出户、为学日益、圣人无常心", "arc": "减掉一句狠话、一次翻旧账"},
    18: {"theme": "出生入死与见小", "chapter": "第50–52章：出生入死、天下有始、见小守柔", "arc": "看见细微裂痕，早补"},
    19: {"theme": "善建不拔", "chapter": "第53–55章：大道、善建者、含德之厚", "arc": "信任靠日常堆叠"},
    20: {"theme": "知不言", "chapter": "第56–58章：知者不言、以正治国", "arc": "沉默有时比道理大"},
    21: {"theme": "烹鲜与处下", "chapter": "第59–61章：啬、烹小鲜、大者宜下", "arc": "少折腾对方，强者先低头"},
    22: {"theme": "图难于易", "chapter": "第62–64章：万物之奥、图难于易、合抱之木", "arc": "从五分钟家务、一句道歉开始"},
    23: {"theme": "善下为王", "chapter": "第65–67章：古之善为道者、江海百谷王", "arc": "道歉快的人，往往更清醒"},
    24: {"theme": "不争之德", "chapter": "第68–70章：善为士、用兵、吾言甚易知", "arc": "把「赢嘴仗」换成「和」"},
    25: {"theme": "知不知", "chapter": "第71–73章：知不知、民不畏威、天网", "arc": "承认不懂，比假装全懂安全"},
    26: {"theme": "柔弱胜刚强", "chapter": "第74–76章：民不畏死、天之道、柔弱胜刚强", "arc": "硬碰硬碎，柔能续"},
    27: {"theme": "张弓与补不足", "chapter": "第77章：天之道损有余补不足", "arc": "动态分工，谁累谁少扛一点"},
    28: {"theme": "和大怨", "chapter": "第78–79章：莫柔弱于水、和大怨", "arc": "小怨当天化，不养大怨"},
    29: {"theme": "小国寡民", "chapter": "第80章：小国寡民", "arc": "把家过成小单位的清简"},
    30: {"theme": "信言不美", "chapter": "第81章：信言不美", "arc": "真话要有温度"},
    31: {"theme": "月课总收束·和光同尘", "chapter": "回顾第4、56章等：挫锐解纷、和光同尘", "arc": "三十一天，回到日常里的柔与真"},
}

EVENING_PLAN: dict[int, dict[str, str]] = {
    1: {"theme": "人性底色：被尊重", "arc": "理解：批评触发防御", "focus": "停止把人钉在错处"},
    2: {"theme": "被看见的需要", "arc": "共情先于建议", "focus": "先复述感受再谈办法"},
    3: {"theme": "赞赏的具体性", "arc": "从挑剔本能到具体肯定", "focus": "夸行为与过程，不空泛"},
    4: {"theme": "引起渴望，不施压", "arc": "请求挂在对方动机上", "focus": "你想被怎样对待，先示范"},
    5: {"theme": "真诚兴趣，非查岗", "arc": "好奇对方的一天", "focus": "十分钟无手机倾听"},
    6: {"theme": "称呼与距离", "arc": "名字/昵称拉回「我们」", "focus": "禁止用「喂」开场一天"},
    7: {"theme": "听他把话说完", "arc": "打断=否定存在感", "focus": "复述最后一句话再回应"},
    8: {"theme": "共同话题的余地", "arc": "各留一点「回血区」", "focus": "五分钟只聊对方兴趣"},
    9: {"theme": "重要感：被需要", "arc": "合伙人对谈，非上下级", "focus": "一句「这事没你不行」要具体"},
    10: {"theme": "避免争论", "arc": "赢道理输关系", "focus": "先找共识句再补分歧"},
    11: {"theme": "永不说你错了", "arc": "体面留面子", "focus": "改写成「我可能没听懂」"},
    12: {"theme": "快速认错", "arc": "止损姿态", "focus": "认错不加「但是」"},
    13: {"theme": "友善开场", "arc": "硬话软包装", "focus": "请求前先一句真诚感谢"},
    14: {"theme": "让对方说是", "arc": "小共识堆大门", "focus": "三个只能答「对」的小问题开场"},
    15: {"theme": "让对方多说", "arc": "抱怨常是求助", "focus": "「还有吗」直到说完"},
    16: {"theme": "主意像他的", "arc": "控制感与安全", "focus": "只提问，让对方说出办法"},
    17: {"theme": "真正换位思考", "arc": "沉默与唠叨的背面", "focus": "写「若我是TA」三行"},
    18: {"theme": "同情意念", "arc": "先同频再建议", "focus": "「要不要听我一个小建议」"},
    19: {"theme": "高尚动机", "arc": "绑定共同在乎的价值", "focus": "为孩子/健康/养老共识开口"},
    20: {"theme": "仪式感表达", "arc": "文字比吼叫持久", "focus": "便利贴一句具体感谢"},
    21: {"theme": "共同挑战", "arc": "对外对事不对人", "focus": "设一个小目标一起完成"},
    22: {"theme": "批评三明治", "arc": "先肯定再谈改进", "focus": "两句具体夸再提一点请求"},
    23: {"theme": "间接提醒", "arc": "保全面子", "focus": "「我们上次好像…」替代「你又」"},
    24: {"theme": "先谈自己的错", "arc": "降低对方防卫", "focus": "一句「我也有责任」"},
    25: {"theme": "提问代替命令", "arc": "尊重选择权", "focus": "命令句改问句十次"},
    26: {"theme": "保全面子", "arc": "当众只抬不贬", "focus": "外人面前一条具体夸奖"},
    27: {"theme": "称赞进步", "arc": "婚姻靠小步迭代", "focus": "只夸「比上次好」的点"},
    28: {"theme": "降低行动门槛", "arc": "五分钟启动", "focus": "计时共同家务五分钟"},
    29: {"theme": "亲密里的尊重", "arc": "钱性育儿都要能谈", "focus": "半小时不被打扰，只谈需要"},
    30: {"theme": "从抱怨到请求", "arc": "月中小结", "focus": "撕抱怨清单，贴请求清单"},
    31: {"theme": "真诚为底色", "arc": "月课结业", "focus": "各写三条：下月想成的「合伙人」样子"},
}

# 每日正文：与 MORNING_PLAN / EVENING_PLAN 按天严格对齐（递进路径，非随机池）
MORNING_BODY: dict[int, dict[str, str]] = {
    1: {"insight": "「名」一落，人就扁。亲密里最大的暴力，常常是轻率的命名：懒、自私、冷漠。经典提醒：可名之名，非常名。", "marriage": "把「你是一个怎样的人」换成「这一刻你经历了什么」。标签关闭对话，处境打开对话。", "action": "今晚冲突起时，先问一句：「你今天最难的一段是什么？」听完十秒再回应。", "reflection": "我今天是否用一句话否定了对方的全部努力？能否只批评一件事、一个时刻？", "footnote": "晨课修己：不必引经据典说服对方，克制标签即是示范。"},
    2: {"insight": "家务之争常是权力之争的替身。不争，不是不做，而是不把家变成法庭。", "marriage": "分工可以谈，尊严不能踩。谈事用「我们需要什么安排」，不用「你凭什么」。", "action": "列出一件本周固定家务，只问：「你愿意哪天扛？我配合你。」", "reflection": "我争的是公平，还是被看见？若后者，我有没有直接说「我需要你看见我」？", "footnote": "从「谁更对」迁到「怎么过」，是本轮前半周的主线。"},
    3: {"insight": "虚能容。家里太满——话太满、安排太满——就没有呼吸缝。水在低处，故能承。", "marriage": "给对方半小时「什么都不解释」的发呆时间，不是冷暴力，是留白。", "action": "今晚留一段不评价、不给建议的倾听，只复述对方最后一句话。", "reflection": "我是否用「为你好」塞满了对方的空隙？能否少一句？", "footnote": "柔不是软塌，是有边界的承托。"},
    4: {"insight": "持盈不如已。情绪到顶点，再追加一句往往致命。知止，是成年人的牙齿。", "marriage": "嘴边的离婚、分手、后悔，多为峰值语言。先收声，再给关系留余地。", "action": "约定一句「暂停语」：任一方说出，双方沉默五分钟。", "reflection": "我上一次「说绝」的话，若收回半句，会不会结果不同？", "footnote": "刹车比加油更需要技术。"},
    5: {"insight": "外界刺激越多，心越盲。婚姻不是秀场，不必用别人的生活当裁判。", "marriage": "少比较「别人家老公/老婆」，比较只会偷走你们自己的刻度。", "action": "今天减少一条与婚恋相关的信息流（群/短视频），把省下的时间给对面一个人。", "reflection": "我今天是否把「羡慕」当成了「攻击伴侣」的武器？", "footnote": "降噪，是让注意力回到身边人。"},
    6: {"insight": "致虚极，不是空心，是把成见倒掉。争执里九成是旧剧本。", "marriage": "开口前问自己：这是此刻的事实，还是我在翻旧账？", "action": "争执前写三行：事实 / 我的感受 / 我的请求。只带纸面进对话。", "reflection": "我有没有把「一次失误」说成「一贯人品」？", "footnote": "静不是冷，是给自己留判断距离。"},
    7: {"insight": "素朴少欲，关系才轻。控制欲常以「为你好」出现。", "marriage": "把三条「你应该」改成三条「我希望我们可以」。", "action": "今天允许对方用「与你不同」的方式完成一件小事，不纠正过程。", "reflection": "我坚持的是原则，还是面子？", "footnote": "少私，是少把自己放在世界中心。"},
    8: {"insight": "曲则全。非原则问题，弯一弯，全的是关系，不是输了你。", "marriage": "若争的是口气，让一次。若争的是底线，弯之后仍要清晰说底线。", "action": "今天主动让一件非原则小事，并明确告诉自己：这是策略，不是卑微。", "reflection": "我怕让的是什么？怕失去尊重，还是怕失去控制？", "footnote": "曲全的前提：底线事先想清楚。"},
    9: {"insight": "希言自然。话多不贵，话准才贵。顺势比逆势说服省力。", "marriage": "对方疲惫时，缩短句子，降低目标，只解决眼前半步。", "action": "把一段超过三句的指责，压成一句事实 + 一句感受 + 一句请求。", "reflection": "我的话里，建议是否多过倾听？", "footnote": "自然，是尊重节律，不是放任。"},
    10: {"insight": "重为轻根，静为躁君。声量与权力感不等同。", "marriage": "低声说话不是示弱，是把对方从「对抗模式」里拉出来。", "action": "今晚刻意把音量降低一格，语速放慢一成。", "reflection": "我是否用大声掩盖不确定？", "footnote": "守雌：守的是柔中的定，不是无原则。"},
    11: {"insight": "物壮则老，走极端则早折。婚姻里的「必须马上谈清楚」有时是暴力。", "marriage": "复杂议题拆成两次谈：第一次只收集信息，第二次再定方案。", "action": "把今晚要谈的议题，砍掉一半，只留最重要一件。", "reflection": "我是在解决问题，还是在发泄焦虑？", "footnote": "极限施压，往往换来极限防御。"},
    12: {"insight": "自知者明。看见对方的错容易，看见自己的盲难。", "marriage": "先承认「我这句话也伤人」，再讨论对方。", "action": "写五条「我在关系里可改进的点」，选一条今天就做。", "reflection": "我要求对方的，自己是否先做到七成？", "footnote": "明，不是自我鞭挞，是清醒。"},
    13: {"insight": "无为：不是不作为，是不乱为。抓大放小，家才有秩序。", "marriage": "只守三条底线（如尊重、安全、重大财务），其余协商弹性。", "action": "列出家中「可放手」的三件小事，本周不评论对方做法。", "reflection": "我的焦虑有多少来自「非我不可」？", "footnote": "无为而无不为，是优先级艺术。"},
    14: {"insight": "上德不德，是不表演道德优越感。亲密最怕「我比你正确」。", "marriage": "把「我教你」换成「我们一起试试」。", "action": "今天不纠正对方的「小习惯」一次，只观察。", "reflection": "我是否在关系里需要「道德高地」？", "footnote": "德在行间，不在嘴上。"},
    15: {"insight": "天下至柔，驰骋至坚。柔是节奏与耐心，不是无骨。", "marriage": "硬碰硬时，先递水、先坐近半寸，再说话。", "action": "用书面写一段软话（短信或纸条），当面说不出口的那句道歉或感谢。", "reflection": "我怕柔，是不是怕显得好欺负？底线是否已事先说清？", "footnote": "柔的前提，仍是边界。"},
    16: {"insight": "名与身孰亲？吵赢与睡个好觉孰重？", "marriage": "把「我要赢」改成「我要关系还能明天继续谈」。", "action": "睡前冲突时，选择「存档」：只约定明天继续的时间点。", "reflection": "我最近为哪些「非原则胜利」付了过大代价？", "footnote": "大成若缺，接受关系与自我都不完美。"},
    17: {"insight": "为道日损：减一句狠话、减一次翻旧账，就是实修。", "marriage": "旧账清单写下来，撕掉，只留当前议题。", "action": "今天禁止「上次你也」句式，发现一次就自我叫停。", "reflection": "我留旧账，是为了公正，还是为了永远占上风？", "footnote": "损的是冗余的自我防卫。"},
    18: {"insight": "见小曰明。裂缝小时补，成本最低。", "marriage": "冷淡三天与冷淡三周，修复难度不同。敏锐是责任。", "action": "点名一件「虽小但我介意」的事，用温和句式提出。", "reflection": "我是否习惯「忍到爆发」？", "footnote": "小不是琐碎，是征兆。"},
    19: {"insight": "善建者不拔。信任是重复的可预期，不是一次感动。", "marriage": "小事守信：说到的时间、答应的电话、承诺的分工。", "action": "兑现一个本周曾跳票的小承诺。", "reflection": "对方不信任我，是性格问题，还是我的累积跳票？", "footnote": "信任无法靠话术购买。"},
    20: {"insight": "知者不言：有时沉默是给对方空间整理，不是冷暴力——须与逃避区分。", "marriage": "说清：「我需要二十分钟想想，再回复你。」比突然沉默安全。", "action": "练习一次「定时回复」的沉默，而不是无限期冷脸。", "reflection": "我的沉默，是整理，还是惩罚？", "footnote": "不言与冷暴力的界限在于是否交代时间与诚意。"},
    21: {"insight": "治大国若烹小鲜：少翻、少折腾。家也怕「一天一新政」。", "marriage": "规则与习惯变动，给对方适应期，不搞突然袭击。", "action": "本周只改一个小习惯，其余维持稳定。", "reflection": "我是否用「改变对方」缓解自己的不安全感？", "footnote": "大者宜下：能扛的人先低头，是强不是弱。"},
    22: {"insight": "图难于其易。修复从五分钟家务、一句道歉开始。", "marriage": "把「彻底谈谈」拆成「先做好一件具体事」。", "action": "共同完成一件不超过十分钟的家务，全程不评价。", "reflection": "我是不是总想「一次性解决所有问题」？", "footnote": "毫末之功，累积为木。"},
    23: {"insight": "江海善下：先道歉的人，常常更清楚自己要什么关系。", "marriage": "道歉针对具体言行，不附带「但是你也」。", "action": "今天就一句具体道歉：「那句话伤了你，是我方式不对。」", "reflection": "我把道歉当认输，还是当止损？", "footnote": "下，是位置选择，不是人格矮化。"},
    24: {"insight": "不争之德：家里不争冠军，争共处。", "marriage": "把「你错了」换成「我们怎么避免下次」。", "action": "对方反驳时，只接「这里我同意你」的一点，再谈分歧。", "reflection": "我赢了话头的那几次，关系是更近还是更远？", "footnote": "和为贵，不是无是非，而是排序。"},
    25: {"insight": "知不知：承认不懂，比假装全懂更能建立安全。", "marriage": "「这件事我不确定，我想听你」比瞎指挥更尊重。", "action": "主动说一次「这方面你比我懂，我听你的」。", "reflection": "我是否怕被看扁，所以在不懂时也硬撑？", "footnote": "天网恢恢：透支尊重，终会还账。"},
    26: {"insight": "柔弱胜刚强：持续温和比一次性爆发更有塑造力。", "marriage": "同样底线，用稳定语气重复，比吼一次更有效。", "action": "重复表达同一边界两次，中间隔二十四小时，语气保持一致。", "reflection": "我的「刚」，有多少是恐惧？", "footnote": "柔是长期主义。"},
    27: {"insight": "天之道，损有余补不足。家庭分工宜动态，不必永久刻死。", "marriage": "这周谁更累，另一方多补一块，不记账到羞辱程度。", "action": "直接问：「这周你需要我多扛哪一块？」", "reflection": "我把公平当「绝对平均」还是「动态平衡」？", "footnote": "补不足，是合伙，不是施舍。"},
    28: {"insight": "和大怨必有余怨，故不如早和解小怨。", "marriage": "当天小摩擦当天收尾，哪怕只是「我先为语气道歉」。", "action": "处理一件拖了三天的冷战余波，开口第一句用软话。", "reflection": "我用冷战惩罚对方时，惩罚的是谁？", "footnote": "和解是能力，不是软弱。"},
    29: {"insight": "小国寡民：家宜简，简则亲。欲望与社交过载会偷走亲密。", "marriage": "减少一项外部消耗，把时间还给二人或全家餐桌。", "action": "本周安排一次无手机的一小时共处。", "reflection": "我们最后一次「无聊地待着」是什么时候？", "footnote": "简，是密度，不是寒酸。"},
    30: {"insight": "信言不美：真话不必好听，但必须干净、具体、可执行。", "marriage": "批评附三条：事实、影响、请求。不用贬损人格。", "action": "把一句刺耳的真话，改写成「事实+感受+请求」版本再说。", "reflection": "我说真话，是为了对方好，还是为了发泄？", "footnote": "美言不信：警惕过度甜腻回避问题。"},
    31: {"insight": "和光同尘：与日常摩擦共存，不追求圣洁人设，而追求稳定温柔。", "marriage": "三十一天不是终点，是起式。把最有效的三条习惯留下。", "action": "两人各写三条「下月继续保留的练习」，贴冰箱。", "reflection": "这一个月，我哪一次退让最值得？哪一次坚持底线最必要？", "footnote": "课程止，修习不止。"},
}

EVENING_BODY: dict[int, dict[str, str]] = {
    1: {"insight": "人性：被批评时，大脑先保命，再听内容。婚姻里，批评启动防御，防御关闭共情。", "marriage": "把「你又」改成「我看到……我感到……我需要……」。", "action": "今天零「人格评价」，只描述事件与感受。", "reflection": "我的话里，有多少比例在证明「我对」？", "footnote": "尊重不是客气，是给对方心理安全。"},
    2: {"insight": "共情不是同意，是让对方感到「我的感受合理」。", "marriage": "先复述：「所以你是觉得……对吗？」再补建议。", "action": "对方倾诉时，禁止打断三次以上，结束前只复述不评判。", "reflection": "我是否急着「解决」，所以否定了对方的情绪？", "footnote": "被看见，是亲密的氧气。"},
    3: {"insight": "赞赏要落到行为与过程，否则像敷衍。", "marriage": "「你今天陪孩子那半小时很耐心」比「你真棒」有效。", "action": "三条具体夸奖，不得重复用词。", "reflection": "我是否只会挑剔，忘了强化「做对的事」？", "footnote": "强化什么，什么就会多出现一点。"},
    4: {"insight": "请求若与对方的渴望同向，阻力小。", "marriage": "把你的需要，接到他在乎的价值上：健康、孩子、体面、休息。", "action": "提请求前，先问：「你最近最想我支持哪一点？」", "reflection": "我是否习惯「单向宣布」而非「双向对齐」？", "footnote": "引起渴望，不是操纵，是共建。"},
    5: {"insight": "真诚兴趣与查岗只差一线：前者关心人，后者控制人。", "marriage": "问「今天最消耗你的是啥」比「你干嘛去了」安全。", "action": "十分钟面对面，手机静音翻面。", "reflection": "我的关心，对方感到温暖还是窒息？", "footnote": "倾听是礼物，不是审讯。"},
    6: {"insight": "称呼疏远，关系就远。名字是最便宜的亲密投资。", "marriage": "严肃谈话也用名或昵称，不用「喂」。", "action": "今天第一句话用名字或昵称开头。", "reflection": "我们有多久没有「好好叫一次对方」？", "footnote": "形式影响心理距离。"},
    7: {"insight": "打断传递的信息是：你的话不重要。", "marriage": "复述最后一句话，再回应。强制降速。", "action": "对方说话时，手指不碰手机。", "reflection": "我打断，是因为急，还是因为不屑？", "footnote": "让对方说完，是体面。"},
    8: {"insight": "各有一块「回血区」，家才有弹性。", "marriage": "支持对方无害的小爱好，不讽刺、不贬低。", "action": "五分钟只聊对方兴趣，不拐到家务。", "reflection": "我是否轻视对方的快乐来源？", "footnote": "尊重差异，不是纵容伤害。"},
    9: {"insight": "重要感来自「被需要」与「被认可」，不是来自被使唤。", "marriage": "具体说「哪件事非你不可」。", "action": "一句「这件事没你不行」+ 具体原因。", "reflection": "我是否只会在需要劳动力时才热情？", "footnote": "合伙感，靠共建，不靠命令。"},
    10: {"insight": "争论升级时，先找共识句，辩论变商量。", "marriage": "「都想孩子好」「都想省钱」——从重叠处开刀。", "action": "开口前三句只谈共识，第四句才谈分歧。", "reflection": "我是否享受辩论快感，多过在乎结果？", "footnote": "避免争论，不是回避问题。"},
    11: {"insight": "「你错了」三个字，是面子战争的发令枪。", "marriage": "改「我可能没听懂，你是说……？」", "action": "今天禁用「你错了」，改用澄清句。", "reflection": "我必须证明对方错，背后是什么恐惧？", "footnote": "留面子，是留关系。"},
    12: {"insight": "快速认错，缩短战争时长。认错是止损，不是定性全责。", "marriage": "认错到具体言行，不追加「但是你也」。", "action": "主动认一件小事的态度问题。", "reflection": "我把认错当输吗？其实是给双方台阶。", "footnote": "真诚，要快，要干净。"},
    13: {"insight": "友善开场，降低对方肾上腺素。", "marriage": "感谢 + 请求，比命令 + 指责成功率高。", "action": "每个请求前，加一句真诚感谢（不虚假）。", "reflection": "我是否习惯「默认对方该做」？", "footnote": "礼貌在亲密里依然有效。"},
    14: {"insight": "让对方连续说是，是打开合作通道的技巧，不是套路。", "marriage": "三个对方必认同的小事实开场，再谈敏感议题。", "action": "设计三个「只能答对」的小问题，再提主请求。", "reflection": "我是否常「一上来就对立」？", "footnote": "共识是门，不是操纵。"},
    15: {"insight": "抱怨长，说明倾诉未完结。打断抱怨，只会变冷战。", "marriage": "「还有吗」问到对方说没有。", "action": "只听与追问，不给方案，直到对方索要方案。", "reflection": "我给方案，是不是为了结束对话，而不是帮助对方？", "footnote": "有时倾听即解决。"},
    16: {"insight": "控制感与安全：让对方以为主意来自他，执行意愿更高。", "marriage": "问「你觉得怎样更好」五次，再补充你的边界。", "action": "今天一个重要决定，让对方先说方案，你只补约束条件。", "reflection": "我是否必须「我的想法当唯一答案」？", "footnote": "合作不是认输。"},
    17: {"insight": "换位思考写三行，比心里「懂」可靠。", "marriage": "写：若我是TA，我最怕 / 最累 / 最想要。", "action": "把三行念给对方听，允许对方纠正。", "reflection": "我写的三行，有多少还是站在道德审判席上？", "footnote": "换位是练习，不是表演。"},
    18: {"insight": "同情意念：先进入同一频道，建议才进得去。", "marriage": "「要不要听我一个小建议？」比直接砸方案礼貌。", "action": "给建议前，先征得同意。对方拒绝，就停。", "reflection": "我是否把「为你好」当越界许可证？", "footnote": "边界感，是成熟的温柔。"},
    19: {"insight": "高尚动机：绑定二人都在乎的价值，减少「你为我做」的对抗感。", "marriage": "谈钱、性、育儿，用「我们都希望……」起头。", "action": "选一个敏感话题，用共同价值开场谈五分钟。", "reflection": "我是否习惯用羞耻或恐惧施压？", "footnote": "动机正，话才可以硬。"},
    20: {"insight": "文字可降情绪强度，给彼此思考缝。", "marriage": "便利贴、短信，适合道歉与感谢，不适合复杂审判。", "action": "写一条不超过五十字的具体感谢，放对方明天可见处。", "reflection": "我是否只会用嘴说，不会用「留痕的温柔」？", "footnote": "仪式感，是记忆的锚。"},
    21: {"insight": "共同挑战，把「你对我不对」变成「我们一起对外」。", "marriage": "设一个小目标：运动、储蓄、早睡，完成比完美重要。", "action": "共同完成一件可量化的小事，庆祝方式极简。", "reflection": "我们多久没有「同一阵营」的体验？", "footnote": "对外对事，不对人。"},
    22: {"insight": "三明治：肯定—建议—肯定。中间那层要具体、短、可执行。", "marriage": "批评只谈行为，不谈人格；只谈一次，不连环轰炸。", "action": "两句具体夸 + 一点小请求 + 再一句肯定。", "reflection": "我的「建议」里，有没有藏着贬低？", "footnote": "纠正为共建，不为羞辱。"},
    23: {"insight": "间接提醒，保全面子，尤其在外人、孩子面前。", "marriage": "用「我们上次好像……」代替「你又……」。", "action": "当众只抬不贬。回家再谈细节。", "reflection": "我是否曾用「让别人听见」来施压？", "footnote": "体面，是长期存款。"},
    24: {"insight": "先谈自己的错，降低对方防卫，再谈共同改进。", "marriage": "「我语气急了」比「你态度差」安全。", "action": "开口前先一句自我责任，不加但是。", "reflection": "我是否认为「先认错就输了」？", "footnote": "先手道歉，常常是强者。"},
    25: {"insight": "提问赋予选择权；命令触发反抗。", "marriage": "「你方便时……？」「你觉得呢？」", "action": "统计十次，把祈使句改问句。", "reflection": "我是否用命令掩盖不安？", "footnote": "尊重形式，影响实质。"},
    26: {"insight": "面子在外，里子在内。外人面前贬伴侣，回家难补。", "marriage": "外人前一条具体夸奖，回家再谈分歧。", "action": "今天若有外人在场，只说好话，坏话带回家关门说。", "reflection": "我是否用「吐槽伴侣」换社交货币？", "footnote": "联盟感，靠共同对外形象维护。"},
    27: {"insight": "称赞进步，强化轨迹，不强化「你应该本来就会」。", "marriage": "「比上次好」比「你终于对了」好听。", "action": "只夸「较上次改进」的一个点。", "reflection": "我是否吝于承认对方的努力？", "footnote": "婚姻靠迭代，不靠一次性完美。"},
    28: {"insight": "降低门槛，启动比完美重要。", "marriage": "五分钟计时，开始最难的那件小事。", "action": "共同家务计时五分钟，铃响可停。", "reflection": "我是否用「一次做到位」吓退了双方？", "footnote": "小行动破大僵局。"},
    29: {"insight": "钱、性、育儿，需要专门时空谈，不靠睡前顺带。", "marriage": "约半小时，不谈家务细节，只谈需求与边界。", "action": "预约明天一段不被打扰的谈话，议题只一个。", "reflection": "我是否总在疲惫边缘谈最重的话题？", "footnote": "尊重，包括尊重议题的重量。"},
    30: {"insight": "从抱怨到请求：抱怨是情绪，请求是路径。", "marriage": "把「你从不管」改成「我希望每周一次你来……」。", "action": "写三条请求替换三条抱怨，贴出来。", "reflection": "我抱怨，是希望改变，还是希望得到道歉？", "footnote": "请求需要勇气，抱怨只需惯性。"},
    31: {"insight": "三十一天，技巧终将褪色，真诚与克制会留下。", "marriage": "共同写下：下月我们要成为什么样的合伙人（各三条）。", "action": "交换阅读，圈出重合的一条，作为下月唯一重点。", "reflection": "这月最难执行的一条是什么？难在哪里？", "footnote": "课程止，修习不止；家宅有光，各自珍重。"},
}


def _render_morning(day: int) -> dict[str, str]:
    p = MORNING_PLAN[day]
    b = MORNING_BODY[day]
    return {
        "title": f"道德经·晨课｜{p['theme']}",
        "progress": f"第 {day} / 31 天｜31 天高密度婚姻修养（晨）｜学习路径：{p['arc']}",
        "theme_today": f"{p['theme']}（{p['chapter']}）",
        "insight": f"{b['insight']} 与今日路径「{p['arc']}」相扣；经典作修己之绳，不作谈资。",
        "marriage": b["marriage"],
        "action": b["action"],
        "reflection": b["reflection"],
        "footnote": b["footnote"],
    }


def _render_evening(day: int) -> dict[str, str]:
    p = EVENING_PLAN[day]
    b = EVENING_BODY[day]
    return {
        "title": f"人性的弱点·晚课｜{p['theme']}",
        "progress": f"第 {day} / 31 天｜31 天高密度婚姻修养（晚）｜递进：{p['arc']}",
        "theme_today": f"{p['theme']}（今日焦点：{p['focus']}）",
        "insight": f"{b['insight']} 与今日焦点「{p['focus']}」一致；以下为亲密关系原创提炼，非机械摘抄译本。",
        "marriage": b["marriage"],
        "action": b["action"],
        "reflection": b["reflection"],
        "footnote": b["footnote"],
    }


def build_card_payload(slot: Slot, day: int) -> dict[str, str]:
    if slot == "morning":
        return _render_morning(day)
    return _render_evening(day)


# ═══════════════════════════════════════════════════════════════════════════
# 状态存储
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class SlotState:
    content: dict[str, str] = field(default_factory=dict)
    approved: bool = False
    sent: bool = False
    preview_text: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict[str, Any]) -> "SlotState":
        return SlotState(
            content=d.get("content", {}),
            approved=bool(d.get("approved", False)),
            sent=bool(d.get("sent", False)),
            preview_text=d.get("preview_text", ""),
        )


@dataclass
class DayState:
    morning: SlotState = field(default_factory=SlotState)
    evening: SlotState = field(default_factory=SlotState)

    def to_dict(self) -> dict[str, Any]:
        return {"morning": self.morning.to_dict(), "evening": self.evening.to_dict()}

    @staticmethod
    def from_dict(d: dict[str, Any]) -> "DayState":
        return DayState(
            morning=SlotState.from_dict(d.get("morning", {})),
            evening=SlotState.from_dict(d.get("evening", {})),
        )


def load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {"version": 1, "days": {}}
    return json.loads(STATE_PATH.read_text(encoding="utf-8"))


def save_state(data: dict[str, Any]) -> None:
    STATE_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def get_day_state(data: dict[str, Any], day: int) -> DayState:
    key = str(day)
    raw = data.setdefault("days", {}).get(key, {})
    return DayState.from_dict(raw) if raw else DayState()


def set_day_state(data: dict[str, Any], day: int, ds: DayState) -> None:
    data.setdefault("days", {})[str(day)] = ds.to_dict()


def day_from_date(s: str) -> int:
    d = date.fromisoformat(s)
    dd = d.day
    if dd < 1 or dd > 31:
        raise ValueError("日期对应日必须在 1–31 之间用于课表索引")
    return min(max(dd, 1), 31)


def format_preview(slot: Slot, day: int, content: dict[str, str]) -> str:
    label = "晨｜道德经" if slot == "morning" else "晚｜人性的弱点"
    lines = [
        f"═══ {label} · 第 {day}/31 天 ═══",
        f"【标题】{content['title']}",
        f"【进度】{content['progress']}",
        f"【今日主题】{content['theme_today']}",
        f"【核心启发】{content['insight']}",
        f"【婚姻落地】{content['marriage']}",
        f"【今天行动】{content['action']}",
        f"【自我反思】{content['reflection']}",
        f"【温和备注】{content['footnote']}",
    ]
    return "\n".join(lines)


def build_feishu_send_header(day: int, slot: Slot, content: dict[str, str]) -> str:
    """实发卡片顶栏：天数 + 进度一眼可见（飞书 header 宜短）。"""
    emoji = "🌅" if slot == "morning" else "🌙"
    kind = "道德经·晨" if slot == "morning" else "人性弱点·晚"
    # 主题取括号前一小段，避免顶栏过长
    theme = content.get("theme_today", "")
    if "（" in theme:
        theme = theme.split("（", 1)[0].strip()
    theme = (theme[:14] + "…") if len(theme) > 15 else theme
    line = f"{emoji} 第{day}/31天｜{kind}｜{theme}" if theme else f"{emoji} 第{day}/31天｜{kind}"
    return line[:99] if len(line) > 99 else line


def build_feishu_interactive(
    card_header: str,
    content: dict[str, str],
    template: str,
    *,
    day: int,
    slot: Slot,
) -> dict[str, Any]:
    """template: blue / purple；实发时带天数与序列进度首屏说明。"""
    slot_line = "晨课｜《道德经》线索" if slot == "morning" else "晚课｜《人性的弱点》递进"
    prev_n = day - 1
    prev_txt = (
        f"前序 **{prev_n}** 讲（第 1–{prev_n} 天）已铺垫学习路径。"
        if prev_n > 0
        else "本轮第 1 讲，为后续 30 讲奠基。"
    )
    progress_block = (
        f"**天数**：第 **{day}** / **31** 天\n"
        f"**序列**：连续第 **{day}** 讲（共 31 讲）· {slot_line}\n"
        f"**进度说明**：{prev_txt}\n"
        f"**课表摘要**：{content['progress']}"
    )
    sections = [
        ("天数与学习进度", progress_block),
        ("今日主题", content["theme_today"]),
        ("核心启发", content["insight"]),
        ("婚姻中的落地解释", content["marriage"]),
        ("今天行动", content["action"]),
        ("自我反思", content["reflection"]),
        ("温和备注", content["footnote"]),
    ]
    elements: list[dict[str, Any]] = []
    for title, body in sections:
        elements.append(
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**{title}**\n{body}"}}
        )
        elements.append({"tag": "hr"})

    return {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {"title": {"tag": "plain_text", "content": card_header}, "template": template},
            "elements": elements[:-1] if elements else elements,
        },
    }


def feishu_send(webhook: str, payload: dict[str, Any]) -> None:
    if requests is None:
        print("需要: pip install requests", file=sys.stderr)
        sys.exit(1)
    r = requests.post(webhook, json=payload, timeout=30)
    r.raise_for_status()
    try:
        j = r.json()
    except json.JSONDecodeError:
        return
    if isinstance(j, dict) and j.get("code") not in (0, None):
        print(json.dumps(j, ensure_ascii=False), file=sys.stderr)
        sys.exit(1)


# ═══════════════════════════════════════════════════════════════════════════
# 命令实现
# ═══════════════════════════════════════════════════════════════════════════


def cmd_bootstrap(args: argparse.Namespace) -> None:
    n = min(max(args.days, 1), 31)
    data = load_state()
    data.setdefault("version", 1)
    for d in range(1, n + 1):
        ds = get_day_state(data, d)
        for slot in ("morning", "evening"):
            st = getattr(ds, slot)
            content = build_card_payload(slot, d)
            st.content = content
            st.preview_text = format_preview(slot, d, content)
            st.approved = False
            st.sent = False
        set_day_state(data, d, ds)
    save_state(data)
    print(f"bootstrap: 已写入第 1–{n} 天草稿（均未批准、未发送）")


def cmd_preview(args: argparse.Namespace) -> None:
    data = load_state()
    if args.day is not None:
        day = args.day
    elif args.date:
        day = day_from_date(args.date)
    else:
        print("preview 需要 --day 或 --date", file=sys.stderr)
        sys.exit(1)
    if day < 1 or day > 31:
        print("day 须在 1–31", file=sys.stderr)
        sys.exit(1)

    ds = get_day_state(data, day)
    for slot in ("morning", "evening"):
        st = getattr(ds, slot)
        if st.content:
            content = st.content
        else:
            content = build_card_payload(slot, day)
            st.content = content
            st.preview_text = format_preview(slot, day, content)
        print(st.preview_text)
        print()
    set_day_state(data, day, ds)
    save_state(data)


def _slots_for_arg(slot: str) -> list[Slot]:
    if slot == "both":
        return ["morning", "evening"]
    return [slot]  # type: ignore[return-value]


def cmd_regen(args: argparse.Namespace) -> None:
    day = args.day
    if day < 1 or day > 31:
        print("day 须在 1–31", file=sys.stderr)
        sys.exit(1)
    data = load_state()
    ds = get_day_state(data, day)
    for s in _slots_for_arg(args.slot):
        st = getattr(ds, s)
        content = build_card_payload(s, day)
        st.content = content
        st.preview_text = format_preview(s, day, content)
        st.approved = False
        st.sent = False
    set_day_state(data, day, ds)
    save_state(data)
    print(f"regen: 第 {day} 天 slot={args.slot} 已按课表重生成，对应 slot 批准/已发已清零")


def cmd_approve(args: argparse.Namespace) -> None:
    day = args.day
    data = load_state()
    ds = get_day_state(data, day)
    for s in _slots_for_arg(args.slot):
        getattr(ds, s).approved = True
    set_day_state(data, day, ds)
    save_state(data)
    print(f"approve: 第 {day} 天 slot={args.slot} 已标记为已批准（send 前仍须逐 slot 检查）")


def cmd_unapprove(args: argparse.Namespace) -> None:
    day = args.day
    data = load_state()
    ds = get_day_state(data, day)
    for s in _slots_for_arg(args.slot):
        getattr(ds, s).approved = False
    set_day_state(data, day, ds)
    save_state(data)
    print(f"unapprove: 第 {day} 天 slot={args.slot} 批准已撤销")


def cmd_send(args: argparse.Namespace) -> None:
    day = args.day
    slot: Slot = args.slot
    webhook = os.environ.get("FEISHU_WEBHOOK_URL", "").strip()
    if not webhook:
        print("未设置 FEISHU_WEBHOOK_URL", file=sys.stderr)
        sys.exit(1)

    data = load_state()
    ds = get_day_state(data, day)
    st = getattr(ds, slot)
    if not st.content:
        st.content = build_card_payload(slot, day)
        st.preview_text = format_preview(slot, day, st.content)
    if not st.approved:
        print("拒绝发送：该 slot 未批准。请先 approve --day N", file=sys.stderr)
        sys.exit(1)
    if st.sent:
        print("该 slot 已标记为已发送；若需重发请先编辑状态文件或扩展命令", file=sys.stderr)
        sys.exit(1)

    header = build_feishu_send_header(day, slot, st.content)
    template = "blue" if slot == "morning" else "purple"
    payload = build_feishu_interactive(header, st.content, template, day=day, slot=slot)
    feishu_send(webhook, payload)
    st.sent = True
    set_day_state(data, day, ds)
    save_state(data)
    print(f"send: 第 {day} 天 {slot} 已发送至飞书")


def main() -> None:
    epilog = """
本地测试示例（请先 export FEISHU_WEBHOOK_URL=你的 webhook；send 前必须 approve）：

  python3 feishu_daily_marriage_tips_bot.py bootstrap --days 31
  python3 feishu_daily_marriage_tips_bot.py preview --day 1
  python3 feishu_daily_marriage_tips_bot.py preview --date 2026-03-29
  python3 feishu_daily_marriage_tips_bot.py regen --day 1 --slot morning
  python3 feishu_daily_marriage_tips_bot.py approve --day 1 --slot morning
  python3 feishu_daily_marriage_tips_bot.py send --day 1 --slot morning
  python3 feishu_daily_marriage_tips_bot.py approve --day 1 --slot evening
  python3 feishu_daily_marriage_tips_bot.py send --day 1 --slot evening
  python3 feishu_daily_marriage_tips_bot.py unapprove --day 1 --slot both

状态文件：同目录 feishu_marriage_tips_state.json（勿提交含审批记录的敏感副本至公开仓库）。
"""
    parser = argparse.ArgumentParser(
        description="31 天高密度婚姻修养飞书卡片（预览·审批·发送）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_boot = sub.add_parser("bootstrap", help="初始化 1..N 天草稿")
    p_boot.add_argument("--days", type=int, default=31)
    p_boot.set_defaults(func=cmd_bootstrap)

    p_prev = sub.add_parser("preview", help="预览某天晨+晚（写入/更新 state）")
    g = p_prev.add_mutually_exclusive_group(required=True)
    g.add_argument("--day", type=int)
    g.add_argument("--date", type=str, help="YYYY-MM-DD，用「日」作为课表第几天（1–31）")
    p_prev.set_defaults(func=cmd_preview)

    p_reg = sub.add_parser("regen", help="按课表重生成指定 slot，并清空该 slot 批准/已发")
    p_reg.add_argument("--day", type=int, required=True)
    p_reg.add_argument("--slot", choices=["morning", "evening", "both"], default="both")
    p_reg.set_defaults(func=cmd_regen)

    p_app = sub.add_parser("approve", help="批准指定 slot（默认 both=晨+晚）")
    p_app.add_argument("--day", type=int, required=True)
    p_app.add_argument("--slot", choices=["morning", "evening", "both"], default="both")
    p_app.set_defaults(func=cmd_approve)

    p_un = sub.add_parser("unapprove", help="撤销指定 slot 批准")
    p_un.add_argument("--day", type=int, required=True)
    p_un.add_argument("--slot", choices=["morning", "evening", "both"], default="both")
    p_un.set_defaults(func=cmd_unapprove)

    p_send = sub.add_parser("send", help="发送已批准的某一 slot")
    p_send.add_argument("--day", type=int, required=True)
    p_send.add_argument("--slot", choices=["morning", "evening"], required=True)
    p_send.set_defaults(func=cmd_send)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
