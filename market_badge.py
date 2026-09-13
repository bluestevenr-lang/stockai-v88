"""Local vector market symbols; independent of Windows flag emoji support."""
from html import escape
import math
import base64


def _star(cx, cy, radius, color, rotation=-90):
    points=[]
    for i in range(10):
        angle=math.radians(rotation+i*36); r=radius if i%2==0 else radius*.382
        points.append(f'{cx+r*math.cos(angle):.2f},{cy+r*math.sin(angle):.2f}')
    return f'<polygon points="{" ".join(points)}" fill="{color}"/>'


def html(market, *, image_mode=False):
    market={'🇨🇳':'A股','CN':'A股','🇭🇰':'港股','HK':'港股','🇺🇸':'美股','US':'美股'}.get(market,market)
    if market not in ('A股','港股','美股'):return escape(str(market or ''))
    if market=='美股':
        art='<rect width="30" height="20" fill="#fff"/>'
        art+=''.join(f'<rect y="{i*20/13:.3f}" width="30" height="{20/13:.3f}" fill="#b22234"/>' for i in range(0,13,2))
        art+='<rect width="12" height="10.77" fill="#3c3b6e"/>'
        art+=''.join(_star(1+j*2+(i%2),1+i*1.08,.45,'#fff') for i in range(9) for j in range(6 if i%2==0 else 5))
    elif market=='A股':
        art='<rect width="30" height="20" fill="#de2910"/>'+_star(5,5,3,'#ffde00')
        art+=''.join(_star(x,y,1,'#ffde00',math.degrees(math.atan2(5-y,5-x))) for x,y in [(10,2),(12,4),(12,7),(10,9)])
    else:
        art='<rect width="30" height="20" fill="#de2910"/>'
        art+=''.join(f'<g transform="rotate({i*72} 15 10)"><path d="M15 10 C9 7 13 1 16 4 C18 7 14 7 15 10" fill="#fff"/>'+_star(14.5,5.8,.6,'#de2910')+'</g>' for i in range(5))
    title={'A股':'中国A股','港股':'中国香港股市','美股':'美国股市'}[market]
    svg=(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 30 20" width="21" height="14" role="img" aria-label="{title}" style="vertical-align:middle;flex-shrink:0;border-radius:2px"><title>{title}</title>{art}</svg>')
    # Streamlit's native st.html removes embedded SVG tags. An image data URI
    # keeps the same fixed local vector asset through that sanitizer.
    symbol=(f'<img src="data:image/svg+xml;base64,{base64.b64encode(svg.encode()).decode()}" width="21" height="14" alt="{title}" style="display:inline-block;vertical-align:middle;flex-shrink:0"/>' if image_mode else svg)
    return (f'<span class="v88-market-badge" aria-label="{title}" style="display:inline-flex;align-items:center;gap:3px;margin-right:4px;white-space:nowrap">'
            +symbol+f'<span style="font-size:10px;color:#475569">{market}</span></span>')
