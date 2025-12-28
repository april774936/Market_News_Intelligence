import os, json, urllib.parse, feedparser, time
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from oauth2client.service_account import ServiceAccountCredentials

def main():
    print("--- 🚀 MEGA 뉴스 수집기 가동 시작 ---")
    
    try:
        scope = ["https://www.googleapis.com/auth/drive.file"]
        creds_json = json.loads(os.environ.get('GSPREAD_JSON'))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
        drive_service = build('drive', 'v3', credentials=creds)
        print("✅ 구글 드라이브 인증 성공")
    except Exception as e:
        print(f"🚨 인증 오류: {e}")
        return

    FOLDER_ID = "1-aITCmfSiRZ1eNLnqvt071PyyqA9DjbT"
    
    queries = ["Nasdaq", "S&P 500", "Nvidia", "FOMC", "Fed", "Inflation", "Trump", "Bitcoin", "Gold", "Oil"]
    all_data = []

    for q in queries:
        print(f"📡 {q} 수집 중...", end=" ")
        try:
            enc = urllib.parse.quote(q)
            url = f"https://news.google.com/rss/search?q={enc}+when:7d&hl=en-US&gl=US&ceid=US:en"
            feed = feedparser.parse(url)
            
            if feed.entries:
                print(f"성공 ({len(feed.entries)}개)")
                for e in feed.entries:
                    all_data.append(f"{e.published} | {q} | {e.title}")
            else:
                print("데이터 없음")
            time.sleep(0.3)
        except Exception as e:
            print(f"에러: {e}")

    if not all_data:
        print("🚨 수집된 데이터가 0건입니다.")
        return

    print(f"📦 총 {len(all_data)}개 업로드 시작...")
    chunk_size = 150
    for i in range(0, len(all_data), chunk_size):
        chunk = all_data[i:i + chunk_size]
        content = "DATE | CATEGORY | TITLE\n" + "="*40 + "\n" + "\n".join(chunk)
        file_name = f"MEGA_Archive_Part_{ (i//chunk_size)+1 :02d}.txt"
        meta = {'name': file_name, 'parents': [FOLDER_ID]}
        media = MediaInMemoryUpload(content.encode('utf-8'), mimetype='text/plain')
        drive_service.files().create(body=meta, media_body=media).execute()
        print(f"📤 {file_name} 업로드 완료")

    print("--- ✨ 모든 작업 종료 ---")

if __name__ == "__main__":
    main()
