import os, json, urllib.parse, feedparser, time
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from oauth2client.service_account import ServiceAccountCredentials

def main():
    print("--- 🚀 MEGA 뉴스 수집기 가동 시작 ---")
    
    # 1. 드라이브 인증
    try:
        scope = ["https://www.googleapis.com/auth/drive.file"]
        # GitHub Secrets에 저장된 JSON 키 로드
        creds_json = json.loads(os.environ.get('GSPREAD_JSON'))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
        drive_service = build('drive', 'v3', credentials=creds)
        print("✅ 구글 드라이브 인증 성공")
    except Exception as e:
        print(f"🚨 인증 오류 발생: {e}")
        return

    # 사용자님의 구글 드라이브 폴더 ID
    FOLDER_ID = "1-aITCmfSiRZ1eNLnqvt071PyyqA9DjbT"
    
    # 2. 핵심 키워드 리스트
    queries = ["Nasdaq", "S&P 500", "Nvidia", "FOMC", "Fed", "Inflation", "Trump", "Bitcoin", "Gold", "Oil"]
    all_data = []

    print(f"📡 총 {len(queries)}개 키워드 수집 시작...")

    for q in queries:
        try:
            enc = urllib.parse.quote(q)
            # 최근 7일치 뉴스 데이터 RSS 요청
            url = f"https://news.google.com/rss/search?q={enc}+when:7d&hl=en-US&gl=US&ceid=US:en"
            feed = feedparser.parse(url)
            
            if feed.entries:
                print(f"✅ {q}: {len(feed.entries)}개 수집 성공")
                for e in feed.entries:
                    all_data.append(f"{e.published} | {q} | {e.title}")
            else:
                print(f"⚠️ {q}: 데이터 없음")
            time.sleep(0.5) # 차단 방지용 딜레이
        except Exception as e:
            print(f"❌ {q} 수집 중 에러: {e}")

    if not all_data:
        print("🚨 수집된 데이터가 최종 0건입니다. 종료합니다.")
        return

    # 3. 데이터 분할 및 업로드 (용량 문제 해결 옵션 포함)
    print(f"📦 총 {len(all_data)}개 헤드라인 업로드 시작...")
    chunk_size = 150
    
    for i in range(0, len(all_data), chunk_size):
        chunk = all_data[i:i + chunk_size]
        content = "DATE | CATEGORY | TITLE\n" + "="*50 + "\n" + "\n".join(chunk)
        
        file_name = f"MEGA_Archive_Part_{ (i//chunk_size)+1 :02d}.txt"
        meta = {'name': file_name, 'parents': [FOLDER_ID]}
        media = MediaInMemoryUpload(content.encode('utf-8'), mimetype='text/plain')
        
        try:
            # supportsAllDrives=True 옵션으로 서비스 계정의 용량 제한 우회
            drive_service.files().create(
                body=meta, 
                media_body=media,
                supportsAllDrives=True 
            ).execute()
            print(f"📤 {file_name} 업로드 완료")
        except Exception as e:
            print(f"❌ {file_name} 업로드 실패: {e}")

    print("--- ✨ 모든 작업이 완벽하게 종료되었습니다 ---")

if __name__ == "__main__":
    main()
