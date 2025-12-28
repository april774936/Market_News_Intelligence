import yfinance as yf
import os, json
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from oauth2client.service_account import ServiceAccountCredentials

def run_backfill():
    # 1. 인증 및 드라이브 설정
    scope = ["https://www.googleapis.com/auth/drive.file"]
    creds_json = json.loads(os.environ.get('GSPREAD_JSON'))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    drive_service = build('drive', 'v3', credentials=creds)

    BACKFILL_FOLDER_ID = "1-aITCmfSiRZ1eNLnqvt071PyyqA9DjbT"

    # 2. 수집 대상 확장 (지수는 뉴스가 적을 수 있어 관련 대형주 대거 추가)
    # 나스닥/S&P500을 대변하는 핵심 종목들
    tickers = [
        "QQQ", "SPY", "DIA", "NVDA", "AAPL", "TSLA", "MSFT", "GOOGL", "AMZN", "META",
        "TQQQ", "SOXL", "AMD", "ASML", "JPM", "GS", "BRK-B"
    ]
    
    all_headlines = []

    print(f"🔍 {len(tickers)}개 종목에서 과거 뉴스 추출 시작...")
    
    for t in tickers:
        try:
            ticker_obj = yf.Ticker(t)
            # yfinance는 최신 뉴스 10~20개를 제공합니다.
            news_list = ticker_obj.news
            if not news_list:
                print(f"⚠️ {t}: 뉴스가 없습니다.")
                continue
                
            print(f"✅ {t}: {len(news_list)}개 발견")
            for n in news_list:
                # 타임스탬프 변환
                dt = datetime.fromtimestamp(n['providerPublishTime']).strftime('%Y-%m-%d %H:%M')
                title = n['title']
                publisher = n.get('publisher', 'Unknown')
                # 데이터베이스 형식: [날짜] | [종목] | [출처] | [헤드라인]
                all_headlines.append(f"{dt} | {t} | {publisher} | {title}")
        except Exception as e:
            print(f"❌ {t} 에러: {e}")
            continue

    if not all_headlines:
        print("🚨 수집된 뉴스가 하나도 없습니다. 티커를 확인하거나 잠시 후 다시 시도하세요.")
        return

    # 3. 데이터 정렬 및 분할 저장
    all_headlines.sort() # 날짜순 정렬
    print(f"총 {len(all_headlines)}개의 헤드라인을 정리합니다.")
    
    # NotebookLM 학습을 위해 파일당 100줄씩 분할
    chunk_size = 100 
    for i in range(0, len(all_headlines), chunk_size):
        chunk = all_headlines[i:i + chunk_size]
        file_content = "\n".join(chunk)
        part_num = (i // chunk_size) + 1
        file_name = f"Historical_News_DB_Part_{part_num:02d}.txt"

        file_metadata = {'name': file_name, 'parents': [BACKFILL_FOLDER_ID]}
        media = MediaInMemoryUpload(file_content.encode('utf-8'), mimetype='text/plain')
        
        try:
            drive_service.files().create(body=file_metadata, media_body=media).execute()
            print(f"📤 {file_name} 업로드 완료")
        except Exception as e:
            print(f"🚨 {file_name} 업로드 실패: {e}")

if __name__ == "__main__":
    run_backfill()
