import os
import json
import urllib.parse
import feedparser
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

def main():
    print("--- 🚀 마켓 뉴스 수집기 (시트 기록 모드) 가동 ---")
    
    # 1. 구글 시트 인증 및 연결
    try:
        # 권한 범위 설정
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        
        # GitHub Secrets에서 인증 정보 로드
        creds_raw = os.environ.get('GSPREAD_JSON')
        if not creds_raw:
            print("🚨 에러: GSPREAD_JSON Secret을 찾을 수 없습니다.")
            return
            
        creds_json = json.loads(creds_raw)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
        gc = gspread.authorize(creds)
        
        # ⚠️ 구글 드라이브에 미리 만들어둔 시트 이름과 일치해야 합니다.
        SHEET_NAME = "MarketNewsDB" 
        sh = gc.open(SHEET_NAME).sheet1
        print(f"✅ 구글 시트 '{SHEET_NAME}' 연결 성공")
    except Exception as e:
        print(f"🚨 연결 실패: {e}")
        print("💡 팁: 시트 이름이 'MarketNewsDB'인지, 서비스 계정을 '편집자'로 초대했는지 확인하세요.")
        return

    # 2. 뉴스 수집 설정 (키워드 및 시간)
    queries = ["Nasdaq", "S&P 500", "Nvidia", "Fed", "Bitcoin", "Inflation", "Trump"]
    all_rows = []
    
    # 현재 시간 (한국 시간 등으로 맞추고 싶다면 나중에 조정 가능)
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    print(f"📡 총 {len(queries)}개 키워드 수집 시작...")

    for q in queries:
        try:
            enc = urllib.parse.quote(q)
            # 최근 1시간 이내의 뉴스만 수집 (정기 수집용)
            url = f"https://news.google.com/rss/search?q={enc}+when:1h&hl=en-US&gl=US&ceid=US:en"
            feed = feedparser.parse(url)
            
            if feed.entries:
                count = 0
                for e in feed.entries[:5]: # 키워드당 최대 5개만 수집
                    all_rows.append([now_str, q, e.title, e.link])
                    count += 1
                print(f"✅ {q}: {count}개 수집")
            else:
                print(f"⚠️ {q}: 새로운 뉴스 없음")
            
            time.sleep(0.5) # 차단 방지
        except Exception as e:
            print(f"❌ {q} 수집 중 에러: {e}")

    # 3. 데이터 시트에 쓰기
    if all_rows:
        try:
            # 시트 맨 아래에 데이터 추가
            sh.append_rows(all_rows)
            print(f"📦 총 {len(all_rows)}개의 행을 시트에 추가 완료!")
        except Exception as e:
            print(f"❌ 시트 기록 실패: {e}")
    else:
        print("💡 추가할 새로운 데이터가 없습니다.")

    print("--- ✨ 모든 작업 종료 ---")

if __name__ == "__main__":
    main()
