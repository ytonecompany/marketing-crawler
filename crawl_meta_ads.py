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

# 제외할 URL 목록 추가
EXCLUDED_URLS = [
    'https://www.facebook.com/business/updates-signup',
    'https://www.facebook.com/business/m/updates-signup?ref=fbb_ens'
]

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
        
        # 한국어로 설정
        chrome_options.add_argument('--lang=ko-KR')
        chrome_options.add_experimental_option('prefs', {'intl.accept_languages': 'ko-KR,ko'})
        
        # 브라우저 시작
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # 타임아웃 설정
        driver.set_page_load_timeout(60)
        wait = WebDriverWait(driver, 20)
        
        # 메타 비즈니스 뉴스 페이지
        url = 'https://www.facebook.com/business/news'
        print(f"페이지 접속 시도: {url}")
        logging.info(f"페이지 접속 시도: {url}")
        driver.get(url)
        
        # 언어 전환이 제대로 됐는지 확인
        try:
            # 언어 선택 메뉴가 있다면 클릭하고 한국어 선택
            language_selector = driver.find_element(By.XPATH, "//div[contains(@class, 'language-selector') or contains(@class, 'locale-selector')]")
            driver.execute_script("arguments[0].click();", language_selector)
            time.sleep(1)
            korean_option = driver.find_element(By.XPATH, "//a[contains(text(), '한국어') or @data-locale='ko_KR']")
            driver.execute_script("arguments[0].click();", korean_option)
            time.sleep(2)
            print("언어를 한국어로 변경했습니다.")
            logging.info("언어를 한국어로 변경했습니다.")
        except:
            print("언어 선택기를 찾을 수 없거나 이미 한국어로 설정되어 있습니다.")
            logging.info("언어 선택기를 찾을 수 없거나 이미 한국어로 설정되어 있습니다.")
        
        # 디버깅을 위해 페이지 소스 저장
        with open('page_source.html', 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
        print("페이지 소스를 page_source.html에 저장했습니다.")
        logging.info("페이지 소스를 page_source.html에 저장했습니다.")
        
        # 스크린샷 저장
        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        screenshot_path = f"screenshots/meta_ads_{now}.png"
        driver.save_screenshot(screenshot_path)
        print(f"스크린샷을 {os.path.abspath(screenshot_path)}에 저장했습니다.")
        logging.info(f"스크린샷을 {os.path.abspath(screenshot_path)}에 저장했습니다.")
        
        # 페이지가 로드될 때까지 명시적 대기 추가
        try:
            # 쿠키 동의 버튼이 있으면 클릭
            try:
                cookie_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Accept') or contains(text(), 'Allow') or contains(text(), '수락') or contains(text(), '동의') or contains(text(), '확인')]")))
                cookie_button.click()
                print("쿠키 동의 버튼을 클릭했습니다.")
                logging.info("쿠키 동의 버튼을 클릭했습니다.")
                time.sleep(2)  # 쿠키 대화상자가 사라질 때까지 잠시 대기
            except:
                print("쿠키 동의 버튼이 없거나 클릭할 수 없습니다.")
                logging.info("쿠키 동의 버튼이 없거나 클릭할 수 없습니다.")
            
            # 새로운 페이지 구조에 맞는 게시글 요소 찾기
            article_elements = None
            
            # 첫 번째 방법: 비즈니스 뉴스 게시물 찾기
            try:
                # 게시물 컨테이너를 찾습니다 - 클래스 이름을 더 구체적으로 지정
                article_elements = wait.until(EC.presence_of_all_elements_located((
                    By.XPATH, "//div[contains(@class, '_7rmt') and contains(@class, '_4sea')]"
                )))
                print(f"XPath로 {len(article_elements)}개의 뉴스 게시물을 찾았습니다.")
                logging.info(f"XPath로 {len(article_elements)}개의 뉴스 게시물을 찾았습니다.")
            except:
                print("특정 클래스로 게시물을 찾지 못했습니다. 다른 방법을 시도합니다.")
                logging.info("특정 클래스로 게시물을 찾지 못했습니다. 다른 방법을 시도합니다.")
            
            # 두 번째 방법: 일반적인 게시물 컨테이너 찾기
            if not article_elements or len(article_elements) == 0:
                try:
                    # 더 일반적인 구조의 게시물 컨테이너를 찾습니다
                    main_content = wait.until(EC.presence_of_element_located((
                        By.XPATH, "//main | //div[@role='main'] | //div[contains(@class, 'content')] | //div[contains(@class, '콘텐츠')]"
                    )))
                    
                    # 메인 컨텐츠 내에서 각 뉴스 아이템이나 카드를 찾습니다
                    article_elements = main_content.find_elements(By.XPATH, 
                        ".//div[contains(@class, 'item') or contains(@class, 'card') or contains(@class, 'post') or contains(@class, '게시물') or contains(@class, '뉴스')]")
                    
                    if not article_elements or len(article_elements) == 0:
                        # 헤딩 요소를 기준으로 상위 컨테이너를 찾습니다
                        headings = main_content.find_elements(By.XPATH, ".//h1 | .//h2 | .//h3 | .//strong[contains(@class, 'title')]")
                        article_elements = []
                        for heading in headings:
                            try:
                                # 헤딩을 포함하는 부모 컨테이너를 찾습니다
                                container = heading.find_element(By.XPATH, "./ancestor::div[position() <= 3]")
                                article_elements.append(container)
                            except:
                                pass
                    
                    print(f"메인 컨텐츠에서 {len(article_elements)}개의 게시물을 찾았습니다.")
                    logging.info(f"메인 컨텐츠에서 {len(article_elements)}개의 게시물을 찾았습니다.")
                except:
                    print("메인 컨텐츠에서 게시물을 찾지 못했습니다.")
                    logging.info("메인 컨텐츠에서 게시물을 찾지 못했습니다.")
            
            # 공지사항 요소를 찾지 못한 경우
            if not article_elements or len(article_elements) == 0:
                # 최종 시도: 페이지의 모든 링크와 타이틀 찾기
                try:
                    links = driver.find_elements(By.XPATH, "//a[.//h2 or .//h3 or .//span[@class='title'] or .//div[contains(text(), '더 알아보기')] or .//div[contains(text(), '읽어보기')]]")
                    if links and len(links) > 0:
                        article_elements = links
                        print(f"링크 요소로 {len(article_elements)}개의 게시물을 찾았습니다.")
                        logging.info(f"링크 요소로 {len(article_elements)}개의 게시물을 찾았습니다.")
                except:
                    pass
            
            # 모든 시도 후에도 게시물을 찾지 못한 경우
            if not article_elements or len(article_elements) == 0:
                driver.save_screenshot(f"screenshots/meta_ads_error_{now}.png")
                error_msg = "게시물 요소를 찾지 못했습니다. 페이지 구조가 변경되었을 수 있습니다."
                print(error_msg)
                logging.error(error_msg)
                
                # 페이지 소스 추가 저장
                with open(f"page_source_error_{now}.html", 'w', encoding='utf-8') as f:
                    f.write(driver.page_source)
                return
            
            print(f"찾은 항목 수: {len(article_elements)}")
            logging.info(f"찾은 항목 수: {len(article_elements)}")
            
            # 기존 데이터 가져오기
            existing_data = sheet.get_all_records()
            existing_titles = [item.get('제목', '') for item in existing_data]
            
            # 기존 데이터의 URL을 기준으로 항목 검색 가능하도록 맵 생성
            existing_url_map = {}
            row_index_map = {}
            for i, item in enumerate(existing_data):
                if '링크' in item and item['링크']:
                    existing_url_map[item['링크']] = item
                    row_index_map[item['링크']] = i + 2  # +2는 헤더행(1)과 0-인덱스를 1-인덱스로 변환하기 위함
            
            # 새로운 공지사항 항목 처리
            new_items = []
            updated_items = []
            
            for article in article_elements[:20]:  # 최신 20개 항목만 처리
                try:
                    # 스크롤하여 현재 항목이 보이게 함
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", article)
                    time.sleep(0.5)  # 스크롤 후 잠시 대기
                    
                    # 디버깅을 위해 현재 항목의 HTML 출력
                    article_html = article.get_attribute('outerHTML')
                    print(f"현재 처리 중인 항목 HTML: {article_html[:200]}...")  # 너무 길면 자름
                    
                    # 제목 추출 시도
                    title_element = None
                    try:
                        title_element = article.find_element(By.XPATH, ".//h2 | .//h3 | .//div[contains(@class, 'title')] | .//span[contains(@class, 'title')] | .//div[contains(@class, '제목')] | .//strong")
                    except:
                        try:
                            # 다른 가능한 제목 요소 찾기
                            title_element = article.find_element(By.XPATH, ".//strong | .//b | .//a | .//div[contains(@class, 'heading')]")
                        except:
                            # 요소가 텍스트를 직접 포함하는 경우
                            if article.text.strip():
                                title_element = article
                    
                    # 제목 텍스트 추출
                    title = "제목 없음"
                    if title_element:
                        title = title_element.text.strip()
                        # 제목이 너무 길면 잘라내기
                        if len(title) > 100:
                            title = title[:97] + "..."
                    
                    # 제목이 유효한지 확인
                    if not title or title == "제목 없음" or "page doesn't exist" in title.lower():
                        print(f"유효하지 않은 제목 건너뜀: {title}")
                        continue
                    
                    # 링크 추출 시도
                    link = ""
                    try:
                        # 요소 자체가 <a> 태그인 경우
                        if article.tag_name == 'a':
                            link = article.get_attribute("href")
                        else:
                            # 내부에 <a> 태그가 있는 경우
                            link_element = article.find_element(By.XPATH, ".//a")
                            link = link_element.get_attribute("href")
                    except:
                        # 링크를 찾지 못한 경우 기본 URL 사용
                        link = url
                    
                    # 제외할 URL 목록에 있는 링크는 건너뜀
                    if any(excluded_url in link for excluded_url in EXCLUDED_URLS):
                        print(f"제외 목록에 있는 URL 건너뜀: {link}")
                        continue
                    
                    # 날짜 추출 시도
                    date_str = ""
                    try:
                        # 특정 URL에 대한 날짜 패턴 확인 - new-creator-marketing-tools URL 특별 처리
                        if "new-creator-marketing-tools" in link:
                            # 직접 세팅
                            date_str = "2025-03-25"
                            print(f"new-creator-marketing-tools 페이지에 대해 직접 날짜 설정: {date_str}")
                        else:
                            # 날짜 요소를 찾기 위한 XPath - Meta 웹사이트 구조에 맞게 최적화
                            date_xpath = ".//div[contains(@class, '_7rmo')]"
                            try:
                                date_element = article.find_element(By.XPATH, date_xpath)
                                date_text = date_element.text.strip()
                                if date_text and ('년' in date_text or '월' in date_text):
                                    date_str = date_text
                                    print(f"날짜 요소 발견: {date_str}")
                            except:
                                # 추가 날짜 패턴 시도
                                try:
                                    # 다양한 날짜 패턴 확인
                                    date_patterns = [
                                        ".//time",
                                        ".//span[contains(text(), '년') and contains(text(), '월')]",
                                        ".//div[contains(text(), '년') and contains(text(), '월')]",
                                        ".//span[contains(@class, 'date')]",
                                        ".//div[contains(@class, 'date')]"
                                    ]
                                    
                                    for pattern in date_patterns:
                                        try:
                                            date_element = article.find_element(By.XPATH, pattern)
                                            date_str = date_element.text.strip()
                                            if date_str and ('년' in date_str or '월' in date_str or '-' in date_str):
                                                print(f"날짜 요소 발견: {date_str}")
                                                break
                                        except:
                                            continue
                                except:
                                    pass
                            
                            # 날짜를 찾지 못한 경우, 제목 요소 주변에서 형제 요소로 날짜 찾기 시도
                            if not date_str and title_element:
                                try:
                                    # 제목 요소의 부모에서 날짜 관련 요소 찾기
                                    parent = title_element.find_element(By.XPATH, "./..")
                                    sibling_date = parent.find_element(By.XPATH, "./following-sibling::div[contains(text(), '년') or contains(text(), '월') or contains(@class, 'date')]")
                                    if sibling_date:
                                        date_str = sibling_date.text.strip()
                                        print(f"제목 주변에서 날짜 발견: {date_str}")
                                except:
                                    pass
                            
                            # 날짜를 찾지 못한 경우 현재 날짜 사용
                            if not date_str:
                                date_str = datetime.now().strftime("%Y-%m-%d")
                                print(f"날짜를 찾지 못해 현재 날짜 사용: {date_str}")
                            else:
                                # 표준 형식으로 변환
                                date_str = standardize_date(date_str)
                    except Exception as date_error:
                        # 날짜 추출 실패 시 현재 날짜 사용
                        print(f"날짜 추출 중 오류: {str(date_error)}")
                        date_str = datetime.now().strftime("%Y-%m-%d")
                    
                    # 내용 추출 시도
                    content = ""
                    try:
                        # 제목을 제외한 텍스트 추출
                        if title_element and title_element != article:
                            # 제목 요소를 제외한 나머지 텍스트
                            all_text = article.text.strip()
                            content = all_text.replace(title, "").strip()
                        else:
                            # 단락 요소 찾기
                            paragraphs = article.find_elements(By.XPATH, ".//p")
                            if paragraphs:
                                content = "\n".join([p.text.strip() for p in paragraphs])
                            else:
                                # 텍스트가 있는 div 요소 찾기
                                text_divs = article.find_elements(By.XPATH, ".//div[string-length(normalize-space(text())) > 5]")
                                if text_divs:
                                    content = "\n".join([div.text.strip() for div in text_divs if div.text.strip() != title])
                                else:
                                    # 설명 요소 찾기 (한국어 사이트에서 자주 사용)
                                    desc_elements = article.find_elements(By.XPATH, ".//div[contains(@class, 'desc') or contains(@class, 'description') or contains(@class, '설명') or contains(@class, '내용')]")
                                    if desc_elements:
                                        content = "\n".join([desc.text.strip() for desc in desc_elements])
                    except:
                        content = "내용을 추출할 수 없습니다."
                    
                    # 내용이 없으면 제목으로 대체
                    if not content:
                        content = f"제목: {title}"
                    
                    # 새 항목 추가 또는 기존 항목 업데이트
                    if title == "제목 없음":
                        print(f"유효하지 않은 제목 건너뜀: {title}")
                        continue
                        
                    # 제목이 너무 짧은 경우 건너뜀 (예: "뉴스", "카테고리" 등)
                    if len(title) < 5:
                        print(f"제목이 너무 짧아 건너뜀: {title}")
                        continue
                        
                    # 날짜 형식이 없는 경우 건너뜀
                    if date_str == datetime.now().strftime("%Y-%m-%d") and not '년' in article.text and not '월' in article.text:
                        print(f"날짜 형식이 없어 건너뜀: {title}")
                        continue

                    # 새 항목 생성
                    item_data = {
                        '제목': title,
                        '구분': 'Meta Ads',
                        '작성일': date_str,  # 표준화된 날짜 형식(YYYY-MM-DD)
                        '링크': link,
                        '내용': content,
                        '출처': 'official site'  # 출처 정보 추가
                    }
                    
                    # 기존 항목이 있는지 확인
                    if link in existing_url_map:
                        # 링크가 동일한 기존 항목 있음 - 날짜 업데이트 확인
                        existing_item = existing_url_map[link]
                        existing_date = existing_item.get('작성일', '')
                        
                        # "new-creator-marketing-tools" 링크를 포함하는 항목은 날짜를 항상 업데이트
                        if "new-creator-marketing-tools" in link:
                            print(f"마케팅 도구 글 날짜 업데이트: {title}, {existing_date} -> {date_str}")
                            
                            # 시트에서 해당 항목 행 업데이트
                            row_index = row_index_map[link]
                            sheet.update_cell(row_index, 3, date_str)  # 작성일 열 업데이트
                            updated_items.append(item_data)
                        # 기존 날짜가 현재 날짜인 경우만 업데이트 (일반 항목)
                        elif existing_date == datetime.now().strftime("%Y-%m-%d"):
                            print(f"기존 항목 날짜 업데이트: {title}, {existing_date} -> {date_str}")
                            
                            # 시트에서 해당 항목 행 업데이트
                            row_index = row_index_map[link]
                            sheet.update_cell(row_index, 3, date_str)  # 작성일 열 업데이트
                            updated_items.append(item_data)
                    elif title not in existing_titles:
                        # 새 항목 추가
                        print(f"새 항목 발견: {title}, 날짜: {date_str}")
                        new_items.append(item_data)
                        existing_titles.append(title)
                except Exception as e:
                    print(f"항목 처리 중 오류: {str(e)}")
                    logging.error(f"항목 처리 중 오류: {str(e)}")
            
            # 새로운 항목이 있으면 시트에 추가
            if new_items:
                # 기존 데이터가 없으면 헤더 추가
                if not existing_data:
                    headers = ['제목', '구분', '작성일', '링크', '출처', '내용', '요약', '최종수정일']
                    sheet.append_row(headers)
                
                # 새 항목 역순으로 추가 (최신 항목이 위에 오도록)
                for item in reversed(new_items):
                    current_date = datetime.now().strftime("%Y-%m-%d")
                    row = [
                        item['제목'],
                        item['구분'],
                        item['작성일'],
                        item['링크'],
                        item['출처'],
                        item['내용'],
                        '',  # 요약은 비워둠
                        current_date  # 최종수정일
                    ]
                    sheet.append_row(row)
                
                print(f"{len(new_items)}개의 새로운 항목을 추가했습니다.")
                logging.info(f"{len(new_items)}개의 새로운 항목을 추가했습니다.")
            else:
                print("새로운 항목이 없습니다.")
                logging.info("새로운 항목이 없습니다.")
                
            if updated_items:
                print(f"{len(updated_items)}개의 항목의 날짜를 업데이트했습니다.")
                logging.info(f"{len(updated_items)}개의 항목의 날짜를 업데이트했습니다.")
                
        except Exception as e:
            print(f"검색 또는 결과 처리 중 오류: {str(e)}")
            logging.error(f"검색 또는 결과 처리 중 오류: {str(e)}")
            driver.save_screenshot(f"screenshots/meta_ads_error_{now}.png")
            
    except Exception as e:
        print(f"페이지 접속 또는 크롤링 중 오류: {str(e)}")
        logging.error(f"페이지 접속 또는 크롤링 중 오류: {str(e)}")
    finally:
        driver.quit()

def standardize_date(date_str):
    """다양한 날짜 형식을 YYYY-MM-DD 형식으로 표준화"""
    try:
        # 디버깅을 위한 원본 날짜 출력
        print(f"표준화 처리 전 날짜 문자열: '{date_str}'")
        
        # [월] 또는 [FB] 형식 패턴 추출 (예: [월] - [2025년 4월 1일])
        fb_pattern = r'\[월\]\s*-\s*\[(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일\]'
        fb_match = re.search(fb_pattern, date_str)
        if fb_match:
            year, month, day = fb_match.groups()
            month = month.zfill(2)
            day = day.zfill(2)
            return f"{year}-{month}-{day}"
        
        # [March 25, 2025] 형식 패턴 추가 - 대괄호가 있는 경우
        eng_month_pattern = r'\[([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})\]'
        eng_month_match = re.search(eng_month_pattern, date_str)
        if eng_month_match:
            month_name, day, year = eng_month_match.groups()
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
            month = month_dict.get(month_name.lower(), '01')
            day = day.zfill(2)
            print(f"[March 25, 2025] 패턴 인식 - 변환 결과: {year}-{month}-{day}")
            return f"{year}-{month}-{day}"
            
        # March 25, 2025 형식 패턴 추가 - 대괄호가 없는 경우
        eng_month_pattern2 = r'([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})'
        eng_month_match2 = re.search(eng_month_pattern2, date_str)
        if eng_month_match2:
            month_name, day, year = eng_month_match2.groups()
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
            month = month_dict.get(month_name.lower(), '01')
            day = day.zfill(2)
            print(f"March 25, 2025 패턴 인식 - 변환 결과: {year}-{month}-{day}")
            return f"{year}-{month}-{day}"
            
        # 연/월/일 패턴 찾기
        patterns = [
            # 영어 날짜 형식
            r'(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})',  # MM/DD/YYYY or DD/MM/YYYY
            r'([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{2,4})',  # Month DD, YYYY
            r'(\d{1,2})\s+([A-Za-z]+)\s+(\d{2,4})',  # DD Month YYYY
            
            # 한국어 날짜 형식
            r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일',  # YYYY년 MM월 DD일
            r'(\d{2,4})[.-](\d{1,2})[.-](\d{1,2})',   # YYYY.MM.DD or YYYY-MM-DD
            r'(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})',   # YYYY. MM. DD (한국어 사이트 날짜 형식)
            r'(\d{4})\s*\.\s*(\d{1,2})\s*\.\s*(\d{1,2})',  # YYYY . MM . DD (공백 포함)
            r'(\d{2})\.(\d{2})\.(\d{2})',  # YY.MM.DD
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
            'december': '12', 'dec': '12',
            # 한국어 월 매핑 추가
            '1월': '01', '2월': '02', '3월': '03', '4월': '04', 
            '5월': '05', '6월': '06', '7월': '07', '8월': '08', 
            '9월': '09', '10월': '10', '11월': '11', '12월': '12'
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

# 개별 URL에 대한 날짜 처리 기능 추가
def update_specific_item_dates(sheet):
    """특정 URL 항목의 날짜를 수동으로 업데이트"""
    try:
        print("특정 항목의 날짜 업데이트 시작...")
        logging.info("특정 항목의 날짜 업데이트 시작...")
        
        # 스프레드시트에서 모든 데이터 가져오기
        all_data = sheet.get_all_records()
        
        # 특정 URL과 원하는 날짜 매핑
        specific_url_dates = {
            'https://www.facebook.com/business/news/new-creator-marketing-tools-to-grow-your-business': '2025-03-25'
        }
        
        # 각 행 검사
        count = 0
        for i, row in enumerate(all_data):
            row_index = i + 2  # 헤더(1) + 0-인덱스 보정
            
            if '링크' in row and row['링크'] in specific_url_dates:
                url = row['링크']
                correct_date = specific_url_dates[url]
                current_date = row.get('작성일', '')
                
                # 날짜가 다른 경우에만 업데이트
                if current_date != correct_date:
                    print(f"수동 날짜 업데이트: 행 {row_index}, URL: {url}")
                    print(f"  - 현재 날짜: {current_date} -> 새 날짜: {correct_date}")
                    
                    # 날짜 열(3번째 열) 업데이트
                    sheet.update_cell(row_index, 3, correct_date)
                    count += 1
        
        print(f"날짜 수동 업데이트 완료: {count}개 항목 처리됨")
        logging.info(f"날짜 수동 업데이트 완료: {count}개 항목 처리됨")
        
    except Exception as e:
        print(f"날짜 수동 업데이트 중 오류: {str(e)}")
        logging.error(f"날짜 수동 업데이트 중 오류: {str(e)}")

# 메인 실행 부분
if __name__ == "__main__":
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info(f"크롤링 시작: {current_time}")
    
    try:
        # 로그 출력 추가
        print(f"메타 광고 크롤링 시작: {current_time}")
        
        # 스프레드시트 연결 시도
        try:
            sheet = setup_google_sheets()
            print("스프레드시트 연결 성공")
            
            # 특정 항목 날짜 수동 업데이트 실행
            update_specific_item_dates(sheet)
            
            # 크롤링 함수 호출 - sheet 인자 전달
            crawl_meta_ads(sheet)
        except Exception as sheet_error:
            print(f"스프레드시트 연결 실패: {str(sheet_error)}")
            logging.error(f"스프레드시트 연결 실패: {str(sheet_error)}")
            
            # 백업 접근 방법 시도
            try:
                print("대체 인증 방법으로 시도합니다...")
                
                # 서비스 계정 파일 경로 설정
                service_account_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'naver-452205-a733573ea425.json')
                
                # 파일 존재 여부 확인
                print(f"서비스 계정 파일 경로: {service_account_file}")
                print(f"파일 존재 여부: {os.path.exists(service_account_file)}")
                
                # 필요한 모든 스코프 추가
                scope = [
                    'https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive',
                    'https://www.googleapis.com/auth/spreadsheets'
                ]
                
                # 서비스 계정 인증 정보 생성
                credentials = service_account.Credentials.from_service_account_file(
                    service_account_file, scopes=scope)
                
                # gspread 클라이언트 생성
                gc = gspread.authorize(credentials)
                print(f"서비스 계정 이메일: crawling@naver-452205.iam.gserviceaccount.com")
                
                # 스프레드시트 열기
                spreadsheet = gc.open_by_key('1shWpyaGrQF00YKkmYGftL2IAEOgmZ8kjw2s-WKbdyGg')
                sheet = spreadsheet.worksheet('Meta_Ads')
                print("대체 인증 방법으로 시트 연결 성공")
                
                # 크롤링 함수 호출
                crawl_meta_ads(sheet)
                
            except Exception as backup_error:
                print(f"대체 인증 방법도 실패: {str(backup_error)}")
                logging.error(f"대체 인증 방법도 실패: {str(backup_error)}")
                raise
    except Exception as e:
        print(f"크롤링 중 오류 발생: {str(e)}")
        logging.error(f"크롤링 중 오류 발생: {str(e)}")
    
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"크롤링 종료: {current_time}")
    logging.info(f"크롤링 종료: {current_time}") 
