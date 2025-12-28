import os, json, urllib.parse, feedparser, time, gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

def main():
    print("--- 🚀 실시간 마켓 데이터 수집 시작 ---")
    
    # 1. 구글 시트 연결
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds_json = json.loads(os.environ.get('GSPREAD_JSON'))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
        gc = gspread.authorize(creds)
        
        # 'MarketNewsDB' 시트 열기
        sh = gc.open("MarketNewsDB").sheet1
        print("✅ 구글 시트 연결 성공")
    except Exception as e:
        print(f"🚨 연결 실패: {e}")
        return

    # 2. 뉴스 수집 키워드
    queries = ["Nasdaq", "S&P 500", "Nvidia", "Fed", "Bitcoin"]
    rows = []
    
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for q in queries:
        print(f"📡 {q} 수집 중...", end=" ")
        try:
            enc = urllib.parse.quote(q)
            url = f"https://news.google.com/rss/search?q={enc}+when:1h&hl=en-US&gl=US&ceid=US:en"
            feed = feedparser.parse(url)
            if feed.entries:
                for e in feed.entries[:5]: # 키워드당 최근 5개만
                    rows.append([now_str, q, e.title])
                print(f"완료")
            else:
                print("새 뉴스 없음")
        except:
            print("오류")

    # 3. 시트에 데이터 추가
    if rows:
        try:
            sh.append_rows(rows)
            print(f"📤 {len(rows)}개 행을 시트에 추가했습니다!")
        except Exception as e:
            print(f"❌ 기록 실패: {e}")
    else:
        print("💡 기록할 새로운 뉴스가 없습니다.")

    print("--- ✨ 수집 프로세스 종료 ---")

if __name__ == "__main__":
    main()
