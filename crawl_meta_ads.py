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
RANGE_NAME = 'Meta_Ads!A2:H'

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
            
            # Meta_Ads 시트 열기
            sheet = spreadsheet.worksheet('Meta_Ads')
            print("Meta_Ads 시트 열기 성공")
            
            return sheet
            
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"스프레드시트를 찾을 수 없습니다. ID: {SPREADSHEET_ID}")
            raise
        except gspread.exceptions.WorksheetNotFound:
            print("Meta_Ads 시트를 찾을 수 없습니다.")
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
        # "2024년 1월 24일" 형식의 문자열에서 숫자만 추출
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
    return [row[0] for row in existing_data[1:]] if len(existing_data) > 1 else []

def summarize_text(text):
    """OpenAI API를 사용하여 텍스트 요약"""
    try:
        if not text or text == "내용을 가져오지 못했습니다.":
            return ""
            
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "Meta Business 공지사항의 내용을 3줄로 요약해주세요. 핵심 내용만 간단히 작성해주세요."},
                {"role": "user", "content": text}
            ],
            temperature=0.7,
            max_tokens=200
        )
        
        summary = response.choices[0].message['content'].strip()
        return summary
        
    except Exception as e:
        print(f"요약 생성 중 오류 발생: {e}")
        return ""

def crawl_meta_ads():
    try:
        print("메타 광고 크롤링 시작:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
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
            driver.get("https://www.facebook.com/business/help/")
            
            # 페이지가 완전히 로드될 때까지 충분히 기다림
            time.sleep(15)
            
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
            screenshot_path = os.path.join(screenshots_dir, f"meta_ads_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            driver.save_screenshot(screenshot_path)
            print(f"스크린샷을 {screenshot_path}에 저장했습니다.")
            
            # 공지사항 목록이 있는 컨테이너 찾기
            try:
                # 검색 버튼 클릭
                search_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "input[placeholder='Search Help Center']"))
                )
                search_button.click()
                time.sleep(2)
                
                # 검색어 입력
                search_button.send_keys("announcements")
                time.sleep(2)
                
                # 검색 실행
                search_button.submit()
                time.sleep(5)
                
                # 검색 결과 가져오기
                search_results = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".searchResultItem"))
                )
                
                print(f"검색 결과 수: {len(search_results)}")
                
                # 기존 데이터 가져오기
                existing_data = sheet.get_all_values()
                existing_titles = [row[0] for row in existing_data[1:] if row]
                
                # 새로운 데이터 저장
                new_data = []
                
                for result in search_results[:10]:  # 상위 10개 결과만 처리
                    try:
                        title_element = result.find_element(By.CSS_SELECTOR, "h3")
                        title = title_element.text.strip()
                        
                        link_element = result.find_element(By.TAG_NAME, "a")
                        link = link_element.get_attribute("href")
                        
                        # 이미 존재하는 제목이면 건너뛰기
                        if title in existing_titles:
                            continue
                        
                        # 상세 페이지로 이동하여 내용 가져오기
                        print(f"상세 페이지 접속 시도: {link}")
                        
                        # 새 탭에서 상세 페이지 열기
                        driver.execute_script("window.open('');")
                        driver.switch_to.window(driver.window_handles[-1])
                        driver.get(link)
                        time.sleep(5)  # 상세 페이지 로딩 대기
                        
                        try:
                            # 상세 내용 가져오기
                            content_element = WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, ".article-content"))
                            )
                            content = content_element.text.strip()
                            
                            # 날짜 정보 가져오기 (없을 수 있음)
                            try:
                                date_element = driver.find_element(By.CSS_SELECTOR, ".article-date")
                                date = date_element.text.strip()
                            except:
                                date = datetime.now().strftime("%Y-%m-%d")  # 날짜 정보가 없으면 현재 날짜 사용
                            
                            print(f"내용 길이: {len(content)}")
                            
                        except Exception as e:
                            print(f"상세 내용 가져오기 실패: {str(e)}")
                            content = "내용을 가져올 수 없습니다."
                            date = datetime.now().strftime("%Y-%m-%d")
                        finally:
                            # 상세 페이지 닫고 목록 페이지로 돌아가기
                            driver.close()
                            driver.switch_to.window(driver.window_handles[0])
                        
                        # 데이터 추가
                        new_data.append([
                            title,
                            'Meta Ads',
                            date,
                            link,
                            content,
                            '',
                            'N',
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        ])
                        
                        print(f"'{title}' 처리 완료")
                        
                    except Exception as e:
                        print(f"항목 처리 중 오류 발생: {str(e)}")
                        if len(driver.window_handles) > 1:
                            driver.close()
                            driver.switch_to.window(driver.window_handles[0])
                        continue
                
                if new_data:
                    sheet.append_rows(new_data)
                    print(f"성공적으로 {len(new_data)}개의 새로운 항목을 추가했습니다.")
                else:
                    print("새로운 공지사항이 없습니다.")
                    
            except Exception as e:
                print(f"검색 또는 결과 처리 중 오류: {str(e)}")
                raise
                
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

# 메인 실행 부분
if __name__ == "__main__":
    try:
        logging.info("크롤링 시작")
        print(f"크롤링 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        crawl_meta_ads()  # 메타 광고 크롤링 실행
        print(f"크롤링 완료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info("크롤링 완료")
    except Exception as e:
        error_msg = f"크롤링 중 오류 발생: {str(e)}"
        print(error_msg)
        logging.error(error_msg)
        # 에러 로그 기록
        with open(error_log_file, 'a') as f:
            f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Error: {str(e)}\n") 