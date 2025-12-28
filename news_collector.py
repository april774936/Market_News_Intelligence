# ... (상단 인증 로직은 이전과 동일)

def collect_daily_news():
    # 비중(Weight)이 반영된 키워드 맵 (사용자 요청 반영)
    target_keywords = {
        "Fed FOMC Monetary Policy": 15,        # 핵심 비중
        "Trump Government Fiscal Policy": 12,
        "Nasdaq100 S&P500 Dow30 Futures": 10,
        "Semiconductor Business NVDA": 10,
        "US Economy Inflation CPI": 8,
        "Gold Silver Commodity Market": 6,
        "Bitcoin Crypto Market": 5
    }

    daily_lines = []
    today_str = datetime.now().strftime('%Y-%m-%d')

    for q, weight in target_keywords.items():
        encoded_query = urllib.parse.quote(q)
        url = f"https://news.google.com/rss/search?q={encoded_query}+when:1d&hl=en-US&gl=US&ceid=US:en"
        feed = feedparser.parse(url)
        
        for entry in feed.entries[:weight]:
            # 데이터베이스 포맷 유지
            daily_lines.append(f"{today_str} | {q} | {entry.title}")

    # 구글 드라이브 최신 뉴스 폴더에 저장 (기존 16B...okH 폴더 사용 권장)
    DAILY_FOLDER_ID = "16Bzv2-cdMw2y_0Q_MMJlkSDaV99I_okH"
    
    # ... (생성 로직은 이전과 동일하게 텍스트 파일로 저장)
