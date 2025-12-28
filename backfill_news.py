import os, json, urllib.parse, feedparser, time
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from oauth2client.service_account import ServiceAccountCredentials

def main():
    print("--- 🚀 뉴스 수집기 가동 시작 ---")
    
    # 1. 드라이브 인증
    try:
        scope = ["https://www.googleapis.com/auth/drive.file"]
        creds_json = json.loads(os.environ.get('GSPREAD_JSON'))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
        drive_service = build('drive', 'v3', credentials=creds)
        print("✅ 구글 드라이브 인증 성공")
    except Exception as e:
        print(f"🚨 인증 오류 발생: {e}")
        return

    FOLDER_ID = "1-aITCmfSiRZ1eNLnqvt071PyyqA9DjbT"
    
    # 2. 핵심 키워드 (검색 성공률을 높이기 위해 단순화)
    queries = ["Nasdaq", "S&P 500", "Nvidia", "FOMC", "Fed", "Inflation", "Trump", "Bitcoin"]
    all_data = []

    for q in queries:
        print(f"📡 {q} 수집 중...", end=" ")
        try:
            enc = urllib.parse.quote(q)
            # 안전하게 최근 7일치 요청
            url = f"https://news.google.com/rss/search?q={enc}+when:7d&hl=en-US&gl=US&ceid=US:en"
            feed = feedparser.parse(url)
            
            if feed.entries:
                print(f"OK ({len(feed.entries)}개)")
                for e in feed.entries:
                    all_data.append(f"{e.published} | {q} | {e.title}")
            else:
                print("데이터 없음")
            time.sleep(0.5)
        except:
            print("에러")

    if not all_data:
        print("🚨 수집된 데이터가 최종 0건입니다.")
        return

    # 3. 데이터 분할 업로드
    print(f"📦 총 {len(all_data)}개 데이터 업로드 시작...")
    chunk_size = 150
    for i in range(0, len(all_data), chunk_size):
        chunk = all_data[i:i + chunk_size]
        content = "DATE | CATEGORY | TITLE\n" + "-"*40 + "\n" + "\n".join(chunk)
        
        file_name = f"Backfill_News_Part_{ (i//chunk_size)+1 :02d}.txt"
        meta = {'name': file_name, 'parents': [FOLDER_ID]}
        media = MediaInMemoryUpload(content.encode('utf-8'), mimetype='text/plain')
        
        drive_service.files().create(body=meta, media_body=media).execute()
        print(f"📤 {file_name} 완료")

    print("--- ✨ 모든 작업 종료 ---")

# 이 부분이 반드시 있어야 코드가 실행됩니다!
if __name__ == "__main__":
    main()
