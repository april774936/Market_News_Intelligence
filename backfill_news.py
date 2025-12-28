import os, json, urllib.parse, feedparser, time
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from oauth2client.service_account import ServiceAccountCredentials

def run_mega_backfill():
    print("🎬 프로젝트 시작: 인증 절차 진행 중...")
    scope = ["https://www.googleapis.com/auth/drive.file"]
    
    try:
        creds_json = json.loads(os.environ.get('GSPREAD_JSON'))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
        drive_service = build('drive', 'v3', credentials=creds)
        print("🔑 구글 드라이브 인증 성공")
    except Exception as e:
        print(f"🚨 인증 실패: {e}")
        return

    BACKFILL_FOLDER_ID = "1-aITCmfSiRZ1eNLnqvt071PyyqA9DjbT"

    # 검색 쿼리를 단순화하여 성공률을 높입니다.
    search_queries = [
        "Nasdaq", "S&P 500", "Federal Reserve", "Nvidia", "Tesla", 
        "Apple", "Inflation", "Interest Rate", "Bitcoin", "Gold"
    ]
    
    all_headlines = []

    print(f"🚀 총 {len(search_queries)}개 테마 수집 시작...")
    
    for q in search_queries:
        try:
            # 주소 구성을 더 단순하게 변경 (when:30d -> when:7d 로 안정성 확보)
            encoded_query = urllib.parse.quote(q)
            url = f"https://news.google.com/rss/search?q={encoded_query}+when:7d&hl=en-US&gl=US&ceid=US:en"
            
            print(f"📡 요청 중: {q}...", end=" ")
            feed = feedparser.parse(url)
            
            if not feed.entries:
                print("❌ 결과 없음")
                continue
                
            count = len(feed.entries)
            print(f"✅ {count}개 발견")
            
            for entry in feed.entries:
                # 데이터 정규화: [날짜] | [키워드] | [제목]
                clean_title = entry.title.replace('|', '-') # 구분자 중복 방지
                all_headlines.append(f"{entry.published} | {q} | {clean_title}")
            
            time.sleep(1) # 차단 방지
        except Exception as e:
            print(f"⚠️ {q} 에러: {e}")

    if not all_headlines:
        print("🚨 수집된 데이터가 최종적으로 0건입니다. RSS 접속 환경을 확인해야 합니다.")
        return

    # 중복 제거 및 정렬
    all_headlines = list(set(all_headlines))
    all_headlines.sort()
    print(f"🔥 총 {len(all_headlines)}개의 고유 헤드라인 확보!")

    # 데이터 저장
    chunk_size = 150 
    for i in range(0, len(all_headlines), chunk_size):
        chunk = all_headlines[i:i + chunk_size]
        file_content = "DATE | CATEGORY | HEADLINE\n" + "="*60 + "\n"
        file_content += "\n".join(chunk)
        
        file_name = f"MEGA_Archive_Part_{ (i//chunk_size)+1 :02d}.txt"
        file_metadata = {'name': file_name, 'parents': [BACKFILL_FOLDER_ID]}
        media = MediaInMemoryUpload(file_content.encode('utf-8'), mimetype='text/plain')
        
        try:
            drive_service.files().create(body=file_metadata, media_body=media).execute()
            print(f"📤 {file_name} 업로드 완료")
        except Exception as e:
            print(f"❌ {file_name} 업로드 실패: {e}")

if __name__ == "__main__":
    run_mega_backfill()
