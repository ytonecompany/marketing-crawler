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

def crawl_meta_ads(sheet):
    try:
        # Chrome 옵션 설정
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        
        # 사용자 에이전트 설정 - 일반 브라우저처럼 보이게 함
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36')
        
        # 브라우저 시작
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # 타임아웃 설정
        driver.set_page_load_timeout(60)
        wait = WebDriverWait(driver, 20)
        
        # Meta Business Help Center 페이지 접속
        url = 'https://www.facebook.com/business/help/updates'
        logging.info(f"페이지 접속 시도: {url}")
        driver.get(url)
        
        # 디버깅을 위해 페이지 소스 저장
        with open('page_source.html', 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
        logging.info("페이지 소스를 page_source.html에 저장했습니다.")
        
        # 스크린샷 저장
        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        screenshot_path = f"screenshots/meta_ads_{now}.png"
        driver.save_screenshot(screenshot_path)
        logging.info(f"스크린샷을 {os.path.abspath(screenshot_path)}에 저장했습니다.")
        
        # 페이지가 로드될 때까지 명시적 대기 추가
        try:
            # 쿠키 동의 버튼이 있으면 클릭
            try:
                cookie_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Accept') or contains(text(), 'Allow') or contains(text(), '수락')]")))
                cookie_button.click()
                logging.info("쿠키 동의 버튼을 클릭했습니다.")
                time.sleep(2)  # 쿠키 대화상자가 사라질 때까지 잠시 대기
            except:
                logging.info("쿠키 동의 버튼이 없거나 클릭할 수 없습니다.")
            
            # 공지사항 컨테이너 찾기 - 여러 선택자 시도
            article_elements = None
            
            # 첫 번째 방법: 직접적인 XPath 사용
            try:
                article_elements = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//div[contains(@class, 'article') or contains(@class, 'post') or contains(@class, 'update')]")))
                logging.info("XPath로 공지사항 요소를 찾았습니다.")
            except:
                logging.info("XPath로 공지사항 요소를 찾지 못했습니다. 다른 방법을 시도합니다.")
            
            # 두 번째 방법: 태그 이름으로 검색
            if not article_elements or len(article_elements) == 0:
                try:
                    article_elements = wait.until(EC.presence_of_all_elements_located((By.TAG_NAME, "article")))
                    logging.info("article 태그로 공지사항 요소를 찾았습니다.")
                except:
                    logging.info("article 태그로 공지사항 요소를 찾지 못했습니다. 다른 방법을 시도합니다.")
            
            # 세 번째 방법: 더 일반적인 div 요소 찾기
            if not article_elements or len(article_elements) == 0:
                try:
                    # 페이지의 주요 컨텐츠 영역을 찾기 위한 시도
                    main_content = wait.until(EC.presence_of_element_located((
                        By.XPATH, "//main | //div[contains(@class, 'content') or contains(@class, 'main')]")))
                    
                    # 메인 컨텐츠 내의 업데이트 항목을 나타낼 수 있는 요소들 찾기
                    article_elements = main_content.find_elements(By.XPATH, 
                        ".//div[contains(@class, 'card') or contains(@class, 'item') or contains(@class, 'post') or @role='article']")
                    logging.info("메인 컨텐츠 내에서 업데이트 항목을 찾았습니다.")
                except:
                    logging.info("메인 컨텐츠에서 업데이트 항목을 찾지 못했습니다.")
            
            # 마지막 시도: 페이지의 모든 링크 요소 검색
            if not article_elements or len(article_elements) == 0:
                try:
                    # 제목을 포함할 가능성이 있는 h2, h3 요소 찾기
                    heading_elements = driver.find_elements(By.XPATH, "//h2 | //h3 | //h4")
                    
                    if heading_elements and len(heading_elements) > 0:
                        # 부모 요소를 게시글로 간주
                        article_elements = []
                        for heading in heading_elements:
                            try:
                                # 부모 div를 찾아 게시글로 간주
                                parent = heading.find_element(By.XPATH, "./ancestor::div[3]")
                                article_elements.append(parent)
                            except:
                                pass
                        logging.info(f"제목 요소를 기반으로 {len(article_elements)}개의 게시글을 찾았습니다.")
                except:
                    logging.info("제목 요소를 기반으로 게시글을 찾지 못했습니다.")
            
            # 공지사항 요소를 찾지 못한 경우
            if not article_elements or len(article_elements) == 0:
                driver.save_screenshot(f"screenshots/meta_ads_error_{now}.png")
                logging.error("공지사항 요소를 찾지 못했습니다. 페이지 구조가 변경되었을 수 있습니다.")
                return
            
            logging.info(f"찾은 항목 수: {len(article_elements)}")
            
            # 기존 데이터 가져오기
            existing_data = sheet.get_all_records()
            existing_titles = [item.get('제목', '') for item in existing_data]
            
            # 새로운 공지사항 항목 처리
            new_items = []
            
            for article in article_elements[:15]:  # 최신 15개 항목만 처리
                try:
                    # 스크롤하여 현재 항목이 보이게 함
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", article)
                    time.sleep(0.5)  # 스크롤 후 잠시 대기
                    
                    # 제목 추출 시도
                    title_element = None
                    try:
                        title_element = article.find_element(By.XPATH, ".//h2 | .//h3 | .//h4 | .//strong | .//b | .//div[contains(@class, 'title')]")
                    except:
                        # 제목 요소를 찾지 못한 경우, 첫 번째 텍스트 노드를 제목으로 사용
                        paragraphs = article.find_elements(By.TAG_NAME, "p")
                        if paragraphs:
                            title_element = paragraphs[0]
                    
                    # 제목 텍스트 추출
                    title = "제목 없음"
                    if title_element:
                        title = title_element.text.strip()
                        # 제목이 너무 길면 잘라내기
                        if len(title) > 100:
                            title = title[:97] + "..."
                    
                    # 날짜 추출 시도
                    date_str = ""
                    try:
                        # 날짜 패턴이 있는 요소 찾기
                        date_patterns = [
                            ".//time",
                            ".//span[contains(text(), '/') or contains(text(), '-') or contains(text(), '년') or contains(text(), '월')]",
                            ".//div[contains(text(), '/') or contains(text(), '-') or contains(text(), '년') or contains(text(), '월')]"
                        ]
                        
                        for pattern in date_patterns:
                            try:
                                date_element = article.find_element(By.XPATH, pattern)
                                date_str = date_element.text.strip()
                                if date_str:
                                    break
                            except:
                                continue
                        
                        # 날짜 요소를 찾지 못한 경우 현재 날짜 사용
                        if not date_str:
                            date_str = datetime.now().strftime("%Y-%m-%d")
                        else:
                            # 다양한 날짜 형식 처리
                            date_str = standardize_date(date_str)
                    except:
                        # 날짜 추출 실패 시 현재 날짜 사용
                        date_str = datetime.now().strftime("%Y-%m-%d")
                    
                    # 링크 추출 시도
                    link = ""
                    try:
                        link_element = article.find_element(By.XPATH, ".//a")
                        link = link_element.get_attribute("href")
                    except:
                        link = "https://www.facebook.com/business/help/updates"
                    
                    # 내용 추출 시도
                    content = ""
                    try:
                        # 제목을 제외한 모든 단락 추출
                        paragraphs = article.find_elements(By.TAG_NAME, "p")
                        if len(paragraphs) > 1:  # 첫 번째 단락이 제목일 수 있으므로 건너뜀
                            content = "\n".join([p.text.strip() for p in paragraphs[1:]])
                        else:
                            # 단락이 없는 경우 div 요소 내용 사용
                            content_divs = article.find_elements(By.XPATH, ".//div[not(contains(@class, 'title'))]")
                            content = "\n".join([div.text.strip() for div in content_divs if div.text.strip()])
                    except:
                        content = "내용을 추출할 수 없습니다."
                    
                    # 이미 존재하는 제목인지 확인
                    if title not in existing_titles:
                        new_items.append({
                            '제목': title,
                            '구분': '업데이트',
                            '작성일': date_str,  # 표준화된 날짜 형식(YYYY-MM-DD)
                            '링크': link,
                            '내용': content
                        })
                        existing_titles.append(title)
                except Exception as e:
                    logging.error(f"항목 처리 중 오류: {str(e)}")
            
            # 새로운 항목이 있으면 시트에 추가
            if new_items:
                # 기존 데이터가 없으면 헤더 추가
                if not existing_data:
                    sheet.append_row(['제목', '구분', '작성일', '링크', '내용', '요약'])
                
                # 새 항목 역순으로 추가 (최신 항목이 위에 오도록)
                for item in reversed(new_items):
                    sheet.append_row([
                        item['제목'],
                        item['구분'],
                        item['작성일'],
                        item['링크'],
                        item['내용'],
                        ''  # 요약은 비워둠 (나중에 별도 스크립트로 처리)
                    ])
                
                logging.info(f"{len(new_items)}개의 새로운 공지사항을 추가했습니다.")
            else:
                logging.info("새로운 공지사항이 없습니다.")
        except Exception as e:
            logging.error(f"검색 또는 결과 처리 중 오류: {str(e)}")
            driver.save_screenshot(f"screenshots/meta_ads_error_{now}.png")
    except Exception as e:
        logging.error(f"페이지 접속 또는 크롤링 중 오류: {str(e)}")
    finally:
        driver.quit()

def standardize_date(date_str):
    """다양한 날짜 형식을 YYYY-MM-DD 형식으로 표준화"""
    try:
        # 연/월/일 패턴 찾기
        patterns = [
            # 영어 날짜 형식
            r'(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})',  # MM/DD/YYYY or DD/MM/YYYY
            r'([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{2,4})',  # Month DD, YYYY
            r'(\d{1,2})\s+([A-Za-z]+)\s+(\d{2,4})',  # DD Month YYYY
            
            # 한국어 날짜 형식
            r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일',  # YYYY년 MM월 DD일
            r'(\d{2,4})[.-](\d{1,2})[.-](\d{1,2})',   # YYYY.MM.DD or YYYY-MM-DD
        ]
        
        # 영어 월 이름을 숫자로 변환하기 위한 사전
        month_dict = {
            'january': '01', 'jan': '01',
            'february': '02', 'feb': '02',
            'march': '03', 'mar': '03',
            'april': '04', 'apr': '04',
            'may': '05',
            'june': '06', 'jun': '06',
            'july': '07', 'jul': '07',
            'august': '08', 'aug': '08',
            'september': '09', 'sep': '09',
            'october': '10', 'oct': '10',
            'november': '11', 'nov': '11',
            'december': '12', 'dec': '12'
        }
        
        for pattern in patterns:
            match = re.search(pattern, date_str, re.IGNORECASE)
            if match:
                groups = match.groups()
                if len(groups) == 3:
                    # 패턴에 따라 다르게 처리
                    if 'year' in pattern or '년' in pattern:
                        year, month, day = groups
                    elif '[A-Za-z]+' in pattern and '\\d{1,2}' in pattern and '\\d{2,4}' in pattern:
                        # Month DD, YYYY 패턴
                        if re.match(r'[A-Za-z]+', groups[0]):
                            month, day, year = groups
                            month = month_dict.get(month.lower(), '01')
                        # DD Month YYYY 패턴
                        else:
                            day, month, year = groups
                            month = month_dict.get(month.lower(), '01')
                    else:
                        # MM/DD/YYYY 또는 DD/MM/YYYY 패턴 (미국식 또는 영국식)
                        # 여기서는 MM/DD/YYYY로 가정
                        month, day, year = groups
                    
                    # 2자리 연도를 4자리로 변환
                    if len(year) == 2:
                        year = f"20{year}" if int(year) < 30 else f"19{year}"
                    
                    # 월과 일이 한 자리인 경우 앞에 0 추가
                    month = month.zfill(2) if month.isdigit() else month
                    day = day.zfill(2) if day.isdigit() else day
                    
                    return f"{year}-{month}-{day}"
        
        # 패턴을 찾지 못한 경우 원래 값 반환 또는 현재 날짜 사용
        today = datetime.now().strftime("%Y-%m-%d")
        logging.warning(f"날짜 형식 표준화 실패: '{date_str}', 현재 날짜({today})를 사용합니다.")
        return today
        
    except Exception as e:
        # 날짜 변환 오류 시 현재 날짜 반환
        logging.error(f"날짜 변환 중 오류: {str(e)}")
        return datetime.now().strftime("%Y-%m-%d")

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
