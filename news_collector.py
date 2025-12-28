import os
import json
import feedparser
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from oauth2client.service_account import ServiceAccountCredentials

def collect_news():
    # 1. 인증 설정
    scope = ["https://www.googleapis.com/auth/drive.file"]
    creds_json = json.loads(os.environ.get('GSPREAD_JSON'))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    drive_service = build('drive', 'v3', credentials=creds)

    # 2. 뉴스 키워드 및 RSS 설정
    # 원하는 키워드로 커스텀 가능합니다.
    queries = ["Nasdaq", "Federal Reserve", "US Economy", "Bitcoin", "AI Tech"]
    news_body = f"DAILY MARKET INTELLIGENCE REPORT - {datetime.now().strftime('%Y-%m-%d')}\n"
    news_body += "="*50 + "\n\n"

    for q in queries:
        print(f"Collecting news for: {q}")
        url = f"https://news.google.com/rss/search?q={q}+when:1d&hl=en-US&gl=US&ceid=US:en"
        feed = feedparser.parse(url)
        
        news_body += f"[[ KEYWORD: {q} ]]\n"
        for entry in feed.entries[:7]:  # 키워드당 상위 7개 뉴스
            news_body += f"- Title: {entry.title}\n"
            news_body += f"  Link: {entry.link}\n"
            news_body += f"  Time: {entry.published}\n\n"
        news_body += "-"*30 + "\n\n"

    # 3. 구글 드라이브 저장 설정
    # !!! 중요: NotebookLM과 연결한 구글 드라이브 폴더의 ID를 여기에 넣으세요 !!!
    FOLDER_ID = "사용자님의_구글드라이브_폴더_ID" 

    file_metadata = {
        'name': f"Market_News_{datetime.now().strftime('%Y%m%d')}.txt",
        'parents': [FOLDER_ID]
    }
    
    media = MediaInMemoryUpload(news_body.encode('utf-8'), mimetype='text/plain')
    
    try:
        file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"✅ 성공! 파일이 드라이브에 생성되었습니다. ID: {file.get('id')}")
    except Exception as e:
        print(f"❌ 실패: {e}")

if __name__ == "__main__":
    collect_news()
