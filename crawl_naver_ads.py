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
RANGE_NAME = 'Naver_Ads!A2:H'

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
            
            # Naver_Ads 시트 열기
            sheet = spreadsheet.worksheet('Naver_Ads')
            print("Naver_Ads 시트 열기 성공")
            
            return sheet
            
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"스프레드시트를 찾을 수 없습니다. ID: {SPREADSHEET_ID}")
            raise
        except gspread.exceptions.WorksheetNotFound:
            print("Naver_Ads 시트를 찾을 수 없습니다.")
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
    # 기존 데이터 가져오기
    existing_data = sheet.get_all_values()
    # 첫 번째 행은 헤더이므로 제외하고, 첫 번째 열(제목)만 추출
    return [row[0] for row in existing_data[1:] if row]

def crawl_naver_ads():
    try:
        print("네이버 광고 크롤링 시작:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
        # Google Sheets 연결
        sheet = setup_google_sheets()
        print("스프레드시트 연결 성공")
        
        # 기존 데이터 확인
        existing_titles = get_existing_titles(sheet)
        print(f"기존 게시글 수: {len(existing_titles)}")
        
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
            driver.get("https://ads.naver.com/notice")
            
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
            screenshot_path = os.path.join(screenshots_dir, f"naver_ads_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            driver.save_screenshot(screenshot_path)
            print(f"스크린샷을 {screenshot_path}에 저장했습니다.")
            
            # 공지사항 목록이 있는 컨테이너 찾기
            try:
                notice_container = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".notice_area"))
                )
                print("공지사항 컨테이너를 찾았습니다.")
            except Exception as e:
                print(f"공지사항 컨테이너를 찾을 수 없습니다: {str(e)}")
                # 다른 선택자 시도
                try:
                    notice_container = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".post_area"))
                    )
                    print("대체 선택자로 공지사항 컨테이너를 찾았습니다.")
                except Exception as e2:
                    print(f"대체 선택자로도 공지사항 컨테이너를 찾을 수 없습니다: {str(e2)}")
                    # 페이지의 모든 요소 출력
                    print("페이지의 모든 요소 클래스 목록:")
                    all_elements = driver.find_elements(By.XPATH, "//*")
                    for i, element in enumerate(all_elements[:30]):  # 처음 30개만 출력
                        try:
                            class_name = element.get_attribute("class")
                            tag_name = element.tag_name
                            print(f"{i}: {tag_name} - 클래스: {class_name}")
                        except:
                            pass
                    raise Exception("공지사항 컨테이너를 찾을 수 없습니다.")
            
            # 공지사항 항목들 찾기 (페이지네이션과 버튼 박스 제외)
            try:
                items = notice_container.find_elements(By.CSS_SELECTOR, ".post_tbody > li")
                if not items:
                    items = notice_container.find_elements(By.CSS_SELECTOR, "li")
                
                # 페이지네이션과 버튼 박스 요소 제외
                filtered_items = []
                for item in items:
                    try:
                        # 페이지네이션 클래스가 있는지 확인
                        pagination_elements = item.find_elements(By.CSS_SELECTOR, ".pagination")
                        if pagination_elements:
                            print("페이지네이션 요소 제외")
                            continue
                        
                        # 버튼 박스 클래스가 있는지 확인
                        btn_box_elements = item.find_elements(By.CSS_SELECTOR, ".btn_box")
                        if btn_box_elements:
                            print("버튼 박스 요소 제외")
                            continue
                        
                        # 부모 요소에서도 페이지네이션/버튼 박스 확인
                        parent = item.find_element(By.XPATH, "..")
                        parent_class = parent.get_attribute("class")
                        if "pagination" in parent_class or "btn_box" in parent_class:
                            print("부모 요소가 페이지네이션/버튼 박스인 항목 제외")
                            continue
                        
                        filtered_items.append(item)
                    except Exception as e:
                        print(f"항목 필터링 중 오류: {str(e)}")
                        continue
                
                items = filtered_items
                print(f"필터링 후 항목 수: {len(items)}")
            except Exception as e:
                print(f"공지사항 항목을 찾을 수 없습니다: {str(e)}")
                items = []
            
            # 새로운 데이터 저장
            new_data = []
            for item in items:
                try:
                    # 요소가 보이도록 스크롤
                    driver.execute_script("arguments[0].scrollIntoView(true);", item)
                    time.sleep(1)
                    
                    # 링크 요소 찾기
                    link_element = item.find_element(By.TAG_NAME, "a")
                    href = link_element.get_attribute("href")
                    
                    if not href:
                        print("링크를 찾을 수 없음")
                        continue
                        
                    # 제목 추출
                    title_element = item.find_element(By.CSS_SELECTOR, "p.post_title")
                    title = title_element.text.strip()
                    
                    # 중요 공지 여부 확인
                    is_important = len(item.find_elements(By.CSS_SELECTOR, "span.em_label")) > 0
                    
                    # 구분과 날짜 추출
                    category = item.find_element(By.CSS_SELECTOR, "span.category").text.strip()
                    date = item.find_element(By.CSS_SELECTOR, "span.date").text.strip()
                    
                    if not all([title, category, date, href]):
                        print(f"필수 정보 누락: title={title}, category={category}, date={date}, href={href}")
                        continue
                    
                    if title in existing_titles:
                        continue
                    
                    # 상세 페이지로 이동하여 내용 가져오기
                    print(f"상세 페이지 접속 시도: {href}")
                    
                    # 새 탭에서 상세 페이지 열기
                    driver.execute_script("window.open('');")
                    driver.switch_to.window(driver.window_handles[-1])
                    driver.get(href)
                    time.sleep(5)  # 상세 페이지 로딩 대기
                    
                    try:
                        # iframe이 있는지 확인
                        iframes = driver.find_elements(By.TAG_NAME, "iframe")
                        if iframes:
                            # iframe으로 전환
                            driver.switch_to.frame(iframes[0])
                        
                        # 상세 내용 가져오기 (여러 선택자 시도)
                        content = ""
                        possible_selectors = [
                            ".detail_content",
                            ".content_area",
                            ".board_view_content",
                            "#content",
                            ".content",
                            ".notice_content",
                            "article"
                        ]
                        
                        for selector in possible_selectors:
                            try:
                                content_element = WebDriverWait(driver, 3).until(
                                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                                )
                                content = content_element.text.strip()
                                if content:
                                    print(f"내용을 찾았습니다. (선택자: {selector})")
                                    break
                            except:
                                continue
                        
                        if not content:
                            # 전체 페이지 텍스트 가져오기
                            content = driver.find_element(By.TAG_NAME, "body").text.strip()
                        
                        # iframe에서 다시 메인 컨텐츠로 전환
                        driver.switch_to.default_content()
                        
                        print(f"내용 길이: {len(content)}")
                        
                    except Exception as e:
                        print(f"상세 내용 가져오기 실패: {str(e)}")
                        content = "내용을 가져올 수 없습니다."
                    finally:
                        # 상세 페이지 닫고 목록 페이지로 돌아가기
                        driver.close()
                        driver.switch_to.window(driver.window_handles[0])
                    
                    # 데이터 추가
                    new_data.append([
                        title,
                        category,
                        date,
                        href,
                        content,
                        '',
                        'Y' if is_important else 'N',
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
        crawl_naver_ads()  # 네이버 광고 크롤링 실행
        print(f"크롤링 완료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info("크롤링 완료")
    except Exception as e:
        error_msg = f"크롤링 중 오류 발생: {str(e)}"
        print(error_msg)
        logging.error(error_msg)
        # 에러 로그 기록
        with open(error_log_file, 'a') as f:
            f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Error: {str(e)}\n") 
