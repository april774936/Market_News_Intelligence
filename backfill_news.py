import yfinance as yf
import pandas as pd
import os, json
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from oauth2client.service_account import ServiceAccountCredentials

def news_backfill():
    # 1. 인증 및 드라이브 설정
    scope = ["https://www.googleapis.com/auth/drive.file"]
    creds_json = json.loads(os.environ.get('GSPREAD_JSON'))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    drive_service = build('drive', 'v3', credentials=creds)
    
    # [과거 데이터용 폴더 ID] - 새로 만드시는 것을 추천합니다
    BACKFILL_FOLDER_ID = "사용자님의_과거데이터_폴더_ID"

    # 2. 수집 대상 티커 (지수 및 주요 종목)
    tickers = ["^NDX", "^GSPC", "^DJI", "NVDA", "TSLA", "AAPL", "MSFT"]
    all_news = []

    print("🚀 과거 뉴스 헤드라인 수집 시작...")
    for t in tickers:
        ticker_obj = yf.Ticker(t)
        news = ticker_obj.news
        for n in news:
            dt = datetime.fromtimestamp(n['providerPublishTime']).strftime('%Y-%m-%d %H:%M')
            title = n['title']
            publisher = n.get('publisher', 'Unknown')
            # 데이터베이스 형태의 한 줄 텍스트 생성
            all_news.append(f"{dt} | {t} | {publisher} | {title}")

    # 3. 데이터 분할 및 저장 (NotebookLM 가독성 최적화)
    # 200줄마다 하나의 파일로 저장
    chunk_size = 200
    for i in range(0, len(all_news), chunk_size):
        chunk = all_news[i:i + chunk_size]
        content = "\n".join(chunk)
        file_name = f"Historical_News_Part_{i//chunk_size + 1}.txt"
        
        file_metadata = {'name': file_name, 'parents': [BACKFILL_FOLDER_ID]}
        media = MediaInMemoryUpload(content.encode('utf-8'), mimetype='text/plain')
        drive_service.files().create(body=file_metadata, media_body=media).execute()
        print(f"✅ {file_name} 업로드 완료")

if __name__ == "__main__":
    news_backfill()
