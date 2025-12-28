import os
import json
import feedparser
import urllib.parse  # URL 인코딩을 위한 라이브러리 추가
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from oauth2client.service_account import ServiceAccountCredentials

def collect_news():
    # 1. 인증 설정
    scope = ["https://www.googleapis.com/auth/drive.file"]
    try:
        creds_json = json.loads(os.environ.get('GSPREAD_JSON'))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
        drive_service = build('drive', 'v3', credentials=creds)
    except Exception as e:
        print(f"인증 오류: {e}")
        return

    # 2. 뉴스 키워드 설정
    queries = [
        "Nasdaq 100 analysis", 
        "Federal Reserve FOMC", 
        "US Inflation CPI", 
        "Bitcoin Ethereum trend", 
        "Global liquidity M2"
    ]
    
    news_body = f"MARKET INTELLIGENCE REPORT - {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
    news_body += "="*60 + "\n\n"

    for q in queries:
        # 핵심 해결 포인트: 키워드 내 공백을 URL용 문자로 변환
        encoded_query = urllib.parse.quote(q)
        print(f"키워드 수집 중: {q} (인코딩됨: {encoded_query})")
        
        # 인코딩된 쿼리를 URL에 삽입
        url = f"https://news.google.com/rss/search?q={encoded_query}+when:1d&hl=en-US&gl=US&ceid=US:en"
        feed = feedparser.parse(url)
        
        news_body += f"[[ TOPIC: {q} ]]\n"
        for entry in feed.entries[:10]:
            news_body += f"- {entry.title}\n"
            news_body += f"  Link: {entry.link}\n"
            news_body += f"  Date: {entry.published}\n\n"
        news_body += "-"*40 + "\n\n"

    # 3. 구글 드라이브 저장
    FOLDER_ID = "16Bzv2-cdMw2y_0Q_MMJlkSDaV99I_okH" 

    file_metadata = {
        'name': f"Market_News_{datetime.now().strftime('%Y%m%d_%H%M')}.txt",
        'parents': [FOLDER_ID]
    }
    
    media = MediaInMemoryUpload(news_body.encode('utf-8'), mimetype='text/plain')
    
    try:
        file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"✅ 뉴스 수집 성공! 생성된 파일 ID: {file.get('id')}")
    except Exception as e:
        print(f"🚨 드라이브 업로드 실패: {e}")

if __name__ == "__main__":
    collect_news()
