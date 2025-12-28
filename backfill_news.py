import yfinance as yf
import os, json, urllib.parse
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from oauth2client.service_account import ServiceAccountCredentials

def run_backfill():
    # 1. 구글 드라이브 인증
    scope = ["https://www.googleapis.com/auth/drive.file"]
    creds_json = json.loads(os.environ.get('GSPREAD_JSON'))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    drive_service = build('drive', 'v3', credentials=creds)

    # 알려주신 과거 데이터용 폴더 ID
    BACKFILL_FOLDER_ID = "1-aITCmfSiRZ1eNLnqvt071PyyqA9DjbT"

    # 2. 수집 대상 (지수, 선물, 주요 섹터)
    tickers = ["^NDX", "^GSPC", "^DJI", "NVDA", "AAPL", "TSLA", "GC=F", "CL=F"]
    all_headlines = []

    print("🔍 과거 뉴스 헤드라인 수집 중...")
    for t in tickers:
        try:
            ticker_obj = yf.Ticker(t)
            news_list = ticker_obj.news
            for n in news_list:
                dt = datetime.fromtimestamp(n['providerPublishTime']).strftime('%Y-%m-%d %H:%M')
                title = n['title']
                # 데이터베이스 포맷: [날짜] | [종목] | [헤드라인]
                all_headlines.append(f"{dt} | {t} | {title}")
        except: continue

    # 3. 데이터 정렬 및 분할 저장 (NotebookLM 용량 및 가독성 고려)
    all_headlines.sort() # 날짜순 정렬
    
    chunk_size = 300 # 파일당 300줄씩 (약 15-20개 파일 생성 예상)
    for i in range(0, len(all_headlines), chunk_size):
        chunk = all_headlines[i:i + chunk_size]
        file_content = "\n".join(chunk)
        part_num = (i // chunk_size) + 1
        file_name = f"Historical_News_DB_Part_{part_num:02d}.txt"

        file_metadata = {'name': file_name, 'parents': [BACKFILL_FOLDER_ID]}
        media = MediaInMemoryUpload(file_content.encode('utf-8'), mimetype='text/plain')
        
        try:
            drive_service.files().create(body=file_metadata, media_body=media).execute()
            print(f"✅ {file_name} 업로드 완료")
        except Exception as e:
            print(f"🚨 {file_name} 실패: {e}")

if __name__ == "__main__":
    run_backfill()
