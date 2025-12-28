import os
import json
import urllib.parse
import feedparser
import time
import gspread
import yfinance as yf  # 지수 데이터용
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

def main():
    print("--- 🚀 MEGA 마켓 인텔리전스 가동 ---")
    
    # 1. 구글 시트 연결
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds_json = json.loads(os.environ.get('GSPREAD_JSON'))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
        gc = gspread.authorize(creds)
        sh = gc.open("MarketNewsDB").sheet1
        print("✅ 구글 시트 연결 성공")
    except Exception as e:
        print(f"🚨 연결 실패: {e}")
        return

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    all_rows = []

    # 2. 실시간 지수 데이터 수집 (나스닥, S&P500, 엔비디아, 비트코인)
    print("📈 주요 지수 수집 중...")
    tickers = {"NASDAQ": "^IXIC", "S&P500": "^GSPC", "Nvidia": "NVDA", "Bitcoin": "BTC-USD"}
    
    for name, symbol in tickers.items():
        try:
            ticker = yf.Ticker(symbol)
            price = ticker.fast_info['last_price']
            all_rows.append([now_str, "MARKET_INDEX", f"{name} Price", f"{price:.2f}"])
        except:
            print(f"⚠️ {name} 지수 수집 건너뜀")

    # 3. 뉴스 데이터 수집 (키워드 확장)
    queries = [
        "Nasdaq 100", "S&P 500", "Nvidia Stock", "Fed FOMC", 
        "US Inflation CPI", "Bitcoin News", "Gold Oil Price", "Trump Economy"
    ]

    print(f"📡 {len(queries)}개 테마 뉴스 수집 중...")
    for q in queries:
        try:
            enc = urllib.parse.quote(q)
            # 최근 1시간 뉴스 수집
            url = f"https://news.google.com/rss/search?q={enc}+when:1h&hl=en-US&gl=US&ceid=US:en"
            feed = feedparser.parse(url)
            
            for e in feed.entries[:10]: # 키워드당 최대 10개
                all_rows.append([now_str, "NEWS", q, e.title])
            time.sleep(0.5)
        except:
            continue

    # 4. 시트 업데이트
    if all_rows:
        try:
            sh.append_rows(all_rows)
            print(f"📦 총 {len(all_rows)}개 데이터를 시트에 기록했습니다!")
        except Exception as e:
            print(f"❌ 기록 실패: {e}")

    print(f"--- ✨ 작업 종료 ({now_str}) ---")

if __name__ == "__main__":
    main()
