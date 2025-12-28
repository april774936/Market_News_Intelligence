import os, json, urllib.parse, feedparser, time
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from oauth2client.service_account import ServiceAccountCredentials

def run_mega_backfill():
    # 1. 인증 및 드라이브 설정
    scope = ["https://www.googleapis.com/auth/drive.file"]
    creds_json = json.loads(os.environ.get('GSPREAD_JSON'))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    drive_service = build('drive', 'v3', credentials=creds)

    BACKFILL_FOLDER_ID = "1-aITCmfSiRZ1eNLnqvt071PyyqA9DjbT"

    # 2. 방대한 수집을 위한 초정밀 키워드 리스트 (50개 테마)
    # 지수, 섹터, 원자재, 정치, 거시경제 지표 망라
    search_queries = [
        "Nasdaq 100", "S&P 500", "Dow Jones", "Russell 2000", "VIX Index",
        "NVDA stock", "Apple AAPL", "Tesla TSLA", "Microsoft MSFT", "Google Alphabet",
        "Amazon AMZN", "Meta Platforms", "AMD semiconductor", "Broadcom AVGO", "ASML news",
        "Federal Reserve Interest Rate", "FOMC Meeting", "Jerome Powell Speech", "US Treasury Yield",
        "Inflation CPI", "PCE Price Index", "Unemployment rate", "GDP Growth", "Consumer Spending",
        "Trump Trade Policy", "US China Trade War", "Government Budget Deficit", "Tax Reform",
        "Gold price", "Silver price", "Crude Oil WTI", "Natural Gas", "Copper futures",
        "Bitcoin BTC", "Ethereum ETH", "Crypto regulation", "Coinbase news",
        "Dollar Index DXY", "EUR USD forex", "USD JPY yen", "USD KRW won",
        "JPMorgan Chase", "Goldman Sachs", "Bank of America", "Private Equity", "Hedge Fund",
        "AI revolution", "Cloud Computing", "Electric Vehicles", "Energy transition"
    ]
    
    all_headlines = []

    print(f"🚀 총 {len(search_queries)}개 테마에 대해 초거대 수집을 시작합니다...")
    
    for q in search_queries:
        try:
            # 최대한 많은 과거치를 위해 'when:30d' (지난 30일간)로 확장
            encoded_query = urllib.parse.quote(q)
            url = f"https://news.google.com/rss/search?q={encoded_query}+when:30d&hl=en-US&gl=US&ceid=US:en"
            feed = feedparser.parse(url)
            
            if not feed.entries:
                continue
                
            print(f"✅ {q}: {len(feed.entries)}개 수집")
            for entry in feed.entries:
                # 데이터베이스 형식: [날짜] | [키워드] | [제목]
                all_headlines.append(f"{entry.published} | {q} | {entry.title}")
            
            # 구글 서버 차단 방지를 위한 최소한의 딜레이
            time.sleep(0.5)
        except Exception as e:
            print(f"❌ {q} 수집 중 에러: {e}")

    if not all_headlines:
        print("🚨 수집된 데이터가 없습니다.")
        return

    # 중복 제거 (여러 키워드에 겹치는 뉴스 제거)
    all_headlines = list(set(all_headlines))
    all_headlines.sort() # 시간순 정렬
    print(f"🔥 중복 제거 후 총 {len(all_headlines)}개의 헤드라인 확보!")

    # 3. 데이터 분할 저장 (NotebookLM 개별 파일 용량 최적화)
    # 파일당 200줄씩 저장하여 여러 개의 파일로 생성
    chunk_size = 200 
    for i in range(0, len(all_headlines), chunk_size):
        chunk = all_headlines[i:i + chunk_size]
        file_content = "DATE | CATEGORY | HEADLINE\n" + "="*50 + "\n"
        file_content += "\n".join(chunk)
        
        part_num = (i // chunk_size) + 1
        file_name = f"MEGA_Archive_Part_{part_num:02d}.txt"

        file_metadata = {'name': file_name, 'parents': [BACKFILL_FOLDER_ID]}
        media = MediaInMemoryUpload(file_content.encode('utf-8'), mimetype='text/plain')
        
        try:
            drive_service.files().create(body=file_metadata, media_body=media).execute()
            print(f"📤 {file_name} 업로드 완료 (라인 {i}~{i+len(chunk)})")
        except: continue

    print(f"✨ 모든 작업이 완료되었습니다. 구글 드라이브 폴더를 확인하세요!")

if __name__ == "__main__":
    run_mega_backfill()
