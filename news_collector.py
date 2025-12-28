import os
import json
import feedparser
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from oauth2client.service_account import ServiceAccountCredentials

def collect_news():
    # 1. 인증 설정 (Secrets에 등록된 GSPREAD_JSON 사용)
    scope = ["https://www.googleapis.com/auth/drive.file"]
    try:
        creds_json = json.loads(os.environ.get('GSPREAD_JSON'))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
        drive_service = build('drive', 'v3', credentials=creds)
    except Exception as e:
        print(f"인증 오류: {e}")
        return

    # 2. 뉴스 키워드 설정 (NotebookLM 분석용)
    # 더 구체적인 키워드를 원하시면 리스트를 수정하세요.
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
        print(f"키워드 수집 중: {q}")
        # 지난 24시간 이내의 영문 뉴스 검색
        url = f"https://news.google.com/rss/search?q={q}+when:1d&hl=en-US&gl=US&ceid=US:en"
        feed = feedparser.parse(url)
        
        news_body += f"[[ TOPIC: {q} ]]\n"
        # 상위 10개 뉴스 추출
        for entry in feed.entries[:10]:
            news_body += f"- {entry.title}\n"
            news_body += f"  Link: {entry.link}\n"
            news_body += f"  Date: {entry.published}\n\n"
        news_body += "-"*40 + "\n\n"

    # 3. 구글 드라이브 저장 (사용자님이 알려주신 폴더 ID 적용)
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
