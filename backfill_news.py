name: MANUAL_BACKFILL

on:
  workflow_dispatch:

jobs:
  backfill_job:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'

      - name: Install dependencies
        run: |
          pip install feedparser google-api-python-client oauth2client

      - name: Run Backfill
        env:
          GSPREAD_JSON: ${{ secrets.GSPREAD_JSON }}
        run: python backfill_news.py
