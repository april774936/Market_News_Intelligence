import os, json, urllib.parse, feedparser, time, gspread
from oauth2client.service_account import ServiceAccountCredentials

def main():
    print("--- 🚀 MEGA 뉴스 수집기 (시트 우회 모드) ---")
    
    # 1. 인증 및 시트 연결
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds_json = json.loads(os.environ.get('GSPREAD_JSON'))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
        gc = gspread.authorize(creds)
        
        # ⚠️ 중요: 구글 드라이브에서 'MarketNewsDB'라는 이름의 구글 시트를 미리 하나 만들고
        # 서비스 계정 이메일을 '편집자'로 초대해두어야 합니다.
        sh = gc.open("MarketNewsDB").sheet1
        print("✅ 구글 시트 연결 성공")
    except Exception as e:
        print(f"🚨 인증/시트 연결 오류: {e}")
        return

    queries = ["Nasdaq", "S&P 500", "Nvidia", "FOMC", "Fed", "Inflation", "Trump", "Bitcoin", "Gold", "Oil"]
    rows = []

    for q in queries:
        print(f"📡 {q} 수집 중...", end=" ")
        try:
            enc = urllib.parse.quote(q)
            url = f"https://news.google.com/rss/search?q={enc}+when:7d&hl=en-US&gl=US&ceid=US:en"
            feed = feedparser.parse(url)
            if feed.entries:
                print(f"성공 ({len(feed.entries)}개)")
                for e in feed.entries:
                    rows.append([e.published, q, e.title])
            time.sleep(0.3)
        except: print("에러")

    if not rows:
        print("🚨 수집 데이터 없음")
        return

    # 시트에 한꺼번에 업데이트 (append_rows는 용량 문제에서 비교적 자유롭습니다)
    try:
        sh.append_rows(rows)
        print(f"📤 {len(rows)}개 데이터를 구글 시트에 기록 완료!")
    except Exception as e:
        print(f"❌ 시트 기록 실패: {e}")

    print("--- ✨ 작업 종료 ---")

if __name__ == "__main__":
    main()
