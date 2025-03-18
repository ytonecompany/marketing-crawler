import os.path
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, timedelta
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import gspread
import re
from bs4 import BeautifulSoup
import requests
import pandas as pd
import argparse
from gspread.exceptions import APIError, SpreadsheetNotFound, WorksheetNotFound
import os

import logging

# 환경 확인 (서버인지 로컬인지)
IS_SERVER = os.path.exists('/home/hosting_users/ytonepd/www')

# 로깅 설정 수정
if IS_SERVER:
    # 서버 환경
    log_file = '/home/hosting_users/ytonepd/www/crawler.log'
    error_log_file = '/home/hosting_users/ytonepd/www/error_log.txt'
else:
    # 로컬 환경
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)  # 로그 디렉토리가 없으면 생성
    log_file = os.path.join(log_dir, 'crawler.log')
    error_log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'error_log.txt')

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1shWpyaGrQF00YKkmYGftL2IAEOgmZ8kjw2s-WKbdyGg'
RANGE_NAME = 'Google_Ads!A2:H'

def setup_google_sheets():
    # 필요한 모든 스코프 추가
    scope = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    try:
        # 서비스 계정 JSON 파일 경로 설정
        SERVICE_ACCOUNT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'naver-452205-a733573ea425.json')
        
        print(f"서비스 계정 파일 경로: {SERVICE_ACCOUNT_FILE}")
        print("인증 시도 중...")
        
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=scope
        )
        
        print(f"서비스 계정 이메일: crawling@naver-452205.iam.gserviceaccount.com")
        
        # gspread 클라이언트 생성
        gc = gspread.authorize(credentials)
        print("gspread 인증 성공")
        
        # 스프레드시트 열기
        SPREADSHEET_ID = '1shWpyaGrQF00YKkmYGftL2IAEOgmZ8kjw2s-WKbdyGg'
        try:
            spreadsheet = gc.open_by_key(SPREADSHEET_ID)
            print("스프레드시트 열기 성공")
            
            # Google_Ads 시트 열기
            sheet = spreadsheet.worksheet('Google_Ads')
            print("Google_Ads 시트 열기 성공")
            
            return sheet
            
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"스프레드시트를 찾을 수 없습니다. ID: {SPREADSHEET_ID}")
            raise
        except gspread.exceptions.WorksheetNotFound:
            print("Google_Ads 시트를 찾을 수 없습니다.")
            raise
        except gspread.exceptions.APIError as e:
            print(f"API 오류: {str(e)}")
            raise
            
    except FileNotFoundError:
        print(f"서비스 계정 키 파일을 찾을 수 없습니다: {SERVICE_ACCOUNT_FILE}")
        raise
    except Exception as e:
        print(f"Google Sheets 설정 중 오류 발생: {str(e)}")
        raise

def parse_korean_date(date_str):
    """한글 날짜를 datetime 객체로 변환"""
    try:
        # "2024년 2월 20일" 형식의 문자열에서 숫자만 추출
        numbers = re.findall(r'\d+', date_str)
        if len(numbers) == 3:
            return datetime(int(numbers[0]), int(numbers[1]), int(numbers[2]))
    except Exception as e:
        print(f"날짜 파싱 오류: {e}")
    return None

def is_within_6_months(date_str):
    """날짜가 현재로부터 6개월 이내인지 확인"""
    date = parse_korean_date(date_str)
    if not date:
        return False
    six_months_ago = datetime.now() - timedelta(days=180)
    return date >= six_months_ago

def get_existing_titles(sheet):
    """시트에서 기존 공지사항 제목 목록 가져오기"""
    existing_data = sheet.get_all_values()
    return [row[0] for row in existing_data[1:] if row]

def crawl_and_update_sheet():
    try:
        print("구글 광고 크롤링 시작:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
        # Google Sheets 연결
        sheet = setup_google_sheets()
        print("스프레드시트 연결 성공")
        
        # 크롬 옵션 설정
        chrome_options = Options()
        chrome_options.add_argument('--headless=new')  # 헤드리스 모드 활성화
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--ignore-ssl-errors')
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        # 셀레니움 설정
        driver = webdriver.Chrome(options=chrome_options)
        
        try:
            print("페이지 접속 시도...")
            driver.get("https://ads.google.com/intl/ko_kr/home/resources/announcements/")
            
            # 페이지가 완전히 로드될 때까지 충분히 기다림
            time.sleep(10)
            
            # JavaScript 실행이 완료될 때까지 대기
            WebDriverWait(driver, 20).until(
                lambda x: x.execute_script("return document.readyState") == "complete"
            )
            
            # 디버깅을 위해 페이지 소스 저장
            with open('page_source.html', 'w', encoding='utf-8') as f:
                f.write(driver.page_source)
            print("페이지 소스를 page_source.html에 저장했습니다.")
            
            # 스크린샷 저장
            screenshots_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'screenshots')
            os.makedirs(screenshots_dir, exist_ok=True)
            screenshot_path = os.path.join(screenshots_dir, f"google_ads_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            driver.save_screenshot(screenshot_path)
            print(f"스크린샷을 {screenshot_path}에 저장했습니다.")
            
            # 공지사항 목록 가져오기
            html_content = driver.page_source
            
            # 크롤링 함수 호출
            announcements = crawl_google_ads_announcements(html_content)
            
            if announcements:
                print(f"총 {len(announcements)}개의 공지사항을 찾았습니다.")
                
                # 기존 데이터 가져오기
                existing_data = sheet.get_all_values()
                existing_titles = [row[0] for row in existing_data[1:] if row]
                
                # 새로운 데이터만 필터링
                new_data = []
                for announcement in announcements:
                    if announcement['title'] not in existing_titles:
                        new_data.append([
                            announcement['title'],
                            announcement['category'],
                            announcement['date'],
                            announcement['link'],
                            announcement['content'],
                            '',  # 요약 (나중에 채워질 예정)
                            'N',  # 중요 공지 여부
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 크롤링 시간
                        ])
                
                if new_data:
                    # 스프레드시트에 새 데이터 추가
                    sheet.append_rows(new_data)
                    print(f"성공적으로 {len(new_data)}개의 새로운 항목을 추가했습니다.")
                else:
                    print("새로운 공지사항이 없습니다.")
            else:
                print("공지사항을 찾을 수 없습니다.")
                
        except Exception as e:
            print(f"페이지 접속 또는 크롤링 중 오류: {str(e)}")
            raise
            
    except Exception as e:
        print(f"크롤링 중 오류 발생: {str(e)}")
        raise
    finally:
        if 'driver' in locals():
            driver.quit()
        print("크롤링 종료:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def crawl_google_ads_announcements(html_content):
    # BeautifulSoup 객체 생성
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # 공지사항 목록 찾기
    announcements = soup.find_all('li', class_='announcement__post')
    
    # 결과를 저장할 리스트
    results = []
    
    for announcement in announcements:
        try:
            # 제목 추출
            title = announcement.find('h2', class_='announcement__post-title').text.strip()
            
            # 날짜 추출
            date_str = announcement.find('h3', class_='announcement__post-sub-head').text.strip()
            
            # 내용 추출
            content = announcement.find('div', class_='announcement__post-body-content').text.strip()
            
            # 링크 추출
            link = announcement.find('a', class_='announcement__post-body-read-more-link')['href']
            
            # 결과 딕셔너리에 저장
            results.append({
                'title': title,
                'date': date_str,
                'content': content,
                'link': f"https://support.google.com{link}"
            })
            
        except Exception as e:
            print(f"Error processing announcement: {e}")
            continue
    
    # DataFrame 생성
    df = pd.DataFrame(results)
    
    # 날짜 형식 변환
    df['date'] = pd.to_datetime(df['date'], format='%Y년 %m월 %d일')
    
    # 날짜순으로 정렬
    df = df.sort_values('date', ascending=False)
    
    return df

# 결과를 CSV 파일로 저장하는 함수
def save_to_csv(df, filename):
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"Data saved to {filename}")

# 메인 실행 부분
if __name__ == "__main__":
    try:
        logging.info("크롤링 시작")
        print(f"크롤링 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        crawl_and_update_sheet()  # 구글 광고 크롤링 실행
        print(f"크롤링 완료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info("크롤링 완료")
    except Exception as e:
        error_msg = f"크롤링 중 오류 발생: {str(e)}"
        print(error_msg)
        logging.error(error_msg)
        # 에러 로그 기록
        with open(error_log_file, 'a') as f:
            f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Error: {str(e)}\n")