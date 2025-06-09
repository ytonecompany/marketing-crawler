#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
from googleapiclient.http import MediaFileUpload
import ftplib
from ftplib import FTP
import socket  # 소켓 타임아웃 설정을 위해 추가
import logging
import sys
import json
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains

# 환경 확인 (서버인지 로컬인지)
IS_SERVER = os.path.exists('/home/hosting_users/ytonepd/www')  # 서버 환경 자동 감지

# 로깅 설정 수정
if IS_SERVER:
    # 서버 환경
    log_dir = '/home/hosting_users/ytonepd/www'
    log_file = os.path.join(log_dir, 'crawler.log')
    error_log_file = os.path.join(log_dir, 'error_log.txt')
else:
    # 로컬 환경
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)  # 로컬 로그 디렉토리 생성
    log_file = os.path.join(log_dir, 'crawler.log')
    error_log_file = os.path.join(log_dir, 'error_log.txt')

# 로그 디렉토리가 없으면 생성
os.makedirs(os.path.dirname(log_file), exist_ok=True)

logging.basicConfig(
    filename=log_file,
    level=logging.DEBUG,  # INFO에서 DEBUG로 변경
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1shWpyaGrQF00YKkmYGftL2IAEOgmZ8kjw2s-WKbdyGg'
RANGE_NAME = 'Boss_pdf!A2:H'

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
        try:
            spreadsheet = gc.open_by_key(SPREADSHEET_ID)
            print("스프레드시트 열기 성공")
            
            # Boss_pdf 시트 열기
            sheet = spreadsheet.worksheet('Boss_pdf')
            print("Boss_pdf 시트 열기 성공")
            
            return sheet
            
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"스프레드시트를 찾을 수 없습니다. ID: {SPREADSHEET_ID}")
            raise
        except gspread.exceptions.WorksheetNotFound:
            print("Boss_pdf 시트를 찾을 수 없습니다.")
            # 시트가 없으면 생성 시도
            try:
                sheet = spreadsheet.add_worksheet(title='Boss_pdf', rows=1000, cols=10)
                # 헤더 설정
                sheet.append_row(['제목', '작성일', '링크', 'PDF 링크', '내용', '중요여부', '파일명', '마지막 업데이트'])
                print("Boss_pdf 시트 생성 성공")
                return sheet
            except Exception as e:
                print(f"시트 생성 중 오류: {str(e)}")
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

def get_element_with_multiple_selectors(driver, selectors_list):
    for selector in selectors_list:
        try:
            element = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
            )
            return element
        except:
            continue
    raise Exception("모든 선택자가 실패했습니다.")

# 로깅 함수 정의
def log_message(message):
    """로그 메시지를 콘솔에 출력하는 함수"""
    print(message)
    # 필요한 경우 파일에도 로깅할 수 있음
    # with open('crawling_log.txt', 'a', encoding='utf-8') as f:
    #     f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")

def get_post_content(driver, post_link):
    """게시글 상세 페이지에서 내용 추출"""
    try:
        # 현재 URL 저장
        current_url = driver.current_url
        
        # 게시글 페이지로 이동
        driver.get(post_link)
        time.sleep(2)  # 페이지 로딩 대기
        
        content = ""
        try:
            # 게시글 본문 내용 추출 시도
            content_selectors = [
                ".ABA-view-body",
                ".ABA-article-contents",
                "#bo_content",
                ".read_body",
                ".article-body"
            ]
            
            for selector in content_selectors:
                try:
                    content_element = driver.find_element(By.CSS_SELECTOR, selector)
                    content = content_element.text.strip()
                    if content:
                        log_message(f"내용 추출 성공 ({selector})")
                        break
                except:
                    continue
            
            if not content:
                log_message("지정된 선택자로 내용을 찾을 수 없습니다.")
        except Exception as e:
            log_message(f"내용 추출 중 오류: {str(e)}")
        
        # 원래 페이지로 돌아가기
        driver.get(current_url)
        time.sleep(1)
        
        return content
        
    except Exception as e:
        log_message(f"게시글 상세 정보 추출 중 오류: {str(e)}")
        
        # 원래 페이지로 돌아가기
        try:
            driver.get(current_url)
            time.sleep(1)
        except:
            pass
            
        return ""

def login_to_iboss(driver):
    try:
        # 로그인 페이지로 이동
        login_url = "https://www.i-boss.co.kr/ab-login"
        driver.get(login_url)
        time.sleep(3)
        
        try:
            # 이메일 입력
            email_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='user_id']"))
            )
            email_input.clear()
            email_input.send_keys("business@y-tone.co.kr")
            log_message("이메일 입력 완료")
            
            # 비밀번호 입력
            password_input = driver.find_element(By.CSS_SELECTOR, "input[name='user_passwd']")
            password_input.clear()
            password_input.send_keys("ytonecompany1!")
            log_message("비밀번호 입력 완료")
            
            # 로그인 버튼 활성화 및 클릭
            login_button = driver.find_element(By.CSS_SELECTOR, "input[name='submit_OK']")
            # disabled 속성 제거
            driver.execute_script("arguments[0].removeAttribute('disabled')", login_button)
            # 폼 제출
            driver.execute_script("arguments[0].form.submit();", login_button)
            log_message("로그인 폼 제출")
            
            # 로그인 완료 대기
            time.sleep(5)
            
            # 로그인 성공 확인
            try:
                # 로그인 후 표시되는 요소 확인 (여러 선택자 시도)
                success_selectors = [
                    ".user-menu",
                    ".logout",
                    ".mypage",
                    ".member_info",
                    "#member_info",
                    "a[href*='logout']"
                ]
                
                for selector in success_selectors:
                    try:
                        element = WebDriverWait(driver, 3).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        )
                        log_message(f"로그인 성공 - {selector} 요소 발견")
                        return True
                    except:
                        continue
                
                log_message("로그인 실패 - 사용자 메뉴를 찾을 수 없음")
                # 현재 페이지의 HTML 구조 로깅
                try:
                    log_message("현재 페이지 HTML 구조:")
                    log_message(f"페이지 제목: {driver.title}")
                    log_message(f"현재 URL: {driver.current_url}")
                except:
                    pass
                return False
                
            except Exception as e:
                log_message(f"로그인 상태 확인 중 오류: {str(e)}")
                return False
                
        except Exception as e:
            log_message(f"로그인 폼 작성 중 오류: {str(e)}")
            # 현재 페이지의 HTML 구조 로깅
            try:
                log_message("현재 페이지 HTML 구조:")
                log_message(f"페이지 제목: {driver.title}")
                log_message(f"현재 URL: {driver.current_url}")
                # form 태그 찾기
                forms = driver.find_elements(By.TAG_NAME, "form")
                log_message(f"페이지 내 form 태그 수: {len(forms)}")
                if forms:
                    log_message("첫 번째 form의 HTML:")
                    log_message(forms[0].get_attribute('outerHTML'))
            except:
                pass
            return False
            
    except Exception as e:
        log_message(f"로그인 시도 중 오류: {str(e)}")
        return False

def convert_to_direct_download_link(web_view_link):
    """Google Drive 뷰어 링크를 직접 다운로드 링크로 변환"""
    try:
        # 파일 ID 추출
        file_id = web_view_link.split('/')[-2]
        # 직접 다운로드 링크 생성 - 향상된 버전
        direct_link = f"https://drive.google.com/uc?id={file_id}&export=download&confirm=t&uuid={int(time.time())}"
        return direct_link
    except:
        return web_view_link

def upload_to_ftp(file_content, remote_filename):
    """FTP를 통해 파일을 카페24 서버에 업로드"""
    try:
        # FTP 연결 설정
        ftp = FTP()
        # 먼저 연결 가능한지 테스트
        log_message("FTP 서버 연결 테스트 중...")
        
        # 카페24 FTP 서버 정보
        host = 'ytonepd.cafe24.com'  # 카페24 FTP 호스트
        username = 'ytonepd'
        password = 'ytonecompany!@'
        
        # FTP 연결
        log_message(f"FTP 연결 시도... 호스트: {host}")
        ftp.connect(host, 21, timeout=30)  # 타임아웃 30초로 감소
        
        # 디버그 모드 활성화
        ftp.set_debuglevel(2)
        
        # 로그인 시도
        log_message("FTP 로그인 시도...")
        ftp.login(username, password)
        log_message("FTP 로그인 성공")
        
        # UTF-8 인코딩 설정
        ftp.encoding = 'utf-8'
        
        # 패시브 모드 설정
        ftp.set_pasv(True)
        
        # 현재 디렉토리 확인
        current_dir = ftp.pwd()
        log_message(f"현재 FTP 디렉토리: {current_dir}")
        
        # 디렉토리 목록 확인
        log_message("디렉토리 목록 확인:")
        ftp.dir()
        
        # pdf_storage 디렉토리로 이동
        try:
            log_message("pdf_storage 디렉토리로 이동 시도...")
            ftp.cwd('/www/pdf_storage')
            log_message("디렉토리 이동 성공")
        except Exception as e:
            log_message(f"디렉토리 이동 실패: {str(e)}")
            log_message("pdf_storage 디렉토리 생성 시도...")
            try:
                ftp.mkd('/www/pdf_storage')
                ftp.cwd('/www/pdf_storage')
                log_message("디렉토리 생성 및 이동 성공")
            except Exception as e:
                log_message(f"디렉토리 생성 실패: {str(e)}")
                # 루트 디렉토리에 업로드 시도
                ftp.cwd('/')
                log_message("루트 디렉토리로 이동")
        
        # 파일 업로드 (바이너리 데이터 직접 전송)
        from io import BytesIO
        bio = BytesIO(file_content)
        
        # 전송 시도 횟수 설정
        max_retries = 3
        retry_count = 0
        chunk_size = 8192  # 8KB 청크 사이즈
        
        while retry_count < max_retries:
            try:
                # 파일 크기 로깅
                file_size = len(file_content)
                log_message(f"업로드 시도: {remote_filename} (크기: {file_size/1024/1024:.2f}MB)")
                
                # 청크 단위로 파일 전송
                ftp.storbinary(f'STOR {remote_filename}', bio, blocksize=chunk_size)
                log_message(f"FTP 업로드 완료: {remote_filename}")
                break
            except Exception as e:
                retry_count += 1
                if retry_count == max_retries:
                    raise e
                log_message(f"FTP 업로드 재시도 {retry_count}/{max_retries} - 오류: {str(e)}")
                time.sleep(5)  # 재시도 전 대기 시간 증가
                bio.seek(0)  # 파일 포인터 초기화
                
                # 연결 재설정 시도
                try:
                    ftp.quit()
                except:
                    pass
                ftp = FTP()
                ftp.connect(host, 21, timeout=30)
                ftp.login(username, password)
                ftp.set_pasv(True)
                ftp.cwd('/www/pdf_storage')
        
        # FTP 연결 종료
        ftp.quit()
        log_message("FTP 연결 종료")
        
        return True
    except Exception as e:
        log_message(f"FTP 업로드 중 오류: {str(e)}")
        # 스택 트레이스 출력
        import traceback
        log_message(f"상세 오류: {traceback.format_exc()}")
        return False

def save_to_server(driver, download_url, file_name):
    """PDF 파일을 구글 드라이브에 업로드"""
    try:
        # 파일명에서 특수문자 제거 및 한글 처리
        safe_filename = "".join([c for c in file_name if c.isalnum() or c in (' ', '-', '_', '.', ')')]).rstrip()
        
        # 원본 파일명 사용 (타임스탬프 제거)
        final_filename = safe_filename
        
        # 현재 세션의 쿠키 가져오기
        cookies = driver.get_cookies()
        session = requests.Session()
        for cookie in cookies:
            session.cookies.set(cookie['name'], cookie['value'])
        
        # PDF 파일 다운로드
        response = session.get(download_url, stream=True)
        if response.status_code == 200:
            try:
                # 응답 내용을 메모리에 저장
                content = response.content
                log_message(f"PDF 파일 다운로드 완료: {len(content)} bytes")
                
                # 구글 드라이브 서비스 인증
                SERVICE_ACCOUNT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'naver-452205-a733573ea425.json')
                credentials = service_account.Credentials.from_service_account_file(
                    SERVICE_ACCOUNT_FILE,
                    scopes=['https://www.googleapis.com/auth/drive']
                )
                
                # 드라이브 서비스 생성
                drive_service = build('drive', 'v3', credentials=credentials)
                
                # 임시 파일 생성 및 저장
                temp_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), final_filename)
                with open(temp_file_path, 'wb') as f:
                    f.write(content)
                
                try:
                    # 파일 메타데이터 설정
                    file_metadata = {
                        'name': final_filename,
                        'parents': ['1dnb8Rz-WTW-bCFvoU99q0DUcCb71HSTq']  # 구글 드라이브 폴더 ID
                    }
                    
                    # 파일 업로드
                    media = MediaFileUpload(temp_file_path, mimetype='application/pdf', resumable=True)
                    file = drive_service.files().create(
                        body=file_metadata,
                        media_body=media,
                        fields='id, webViewLink'
                    ).execute()
                    
                    # 파일 권한 설정 (공개)
                    permission = {
                        'type': 'anyone',
                        'role': 'reader'
                    }
                    drive_service.permissions().create(
                        fileId=file.get('id'),
                        body=permission
                    ).execute()
                    
                    # 웹뷰 링크를 직접 다운로드 링크로 변환
                    download_link = convert_to_direct_download_link(file.get('webViewLink'))
                    
                    log_message(f"구글 드라이브 업로드 완료: {download_link}")
                    
                    # 임시 파일 삭제
                    os.remove(temp_file_path)
                    
                    return download_link
                    
                except Exception as e:
                    log_message(f"구글 드라이브 업로드 중 오류: {str(e)}")
                    if os.path.exists(temp_file_path):
                        os.remove(temp_file_path)
                    return None
                    
            except Exception as e:
                log_message(f"파일 처리 중 오류: {str(e)}")
                return None
                    
        else:
            log_message(f"PDF 다운로드 실패: {response.status_code}")
            return None
            
    except Exception as e:
        log_message(f"파일 저장 중 오류: {str(e)}")
        return None

def get_pdf_download_links(driver, content_element):
    try:
        # PDF 다운로드 링크와 파일명을 함께 저장할 리스트
        pdf_info_list = []
        
        # 첨부파일 영역에서 PDF 링크 찾기
        attachment_areas = driver.find_elements(By.CSS_SELECTOR, ".attached_file_in_conts")
        for area in attachment_areas:
            try:
                # PDF 아이콘 확인
                pdf_icon = area.find_element(By.CSS_SELECTOR, "img[src*='pdf.gif']")
                
                # 파일명과 다운로드 링크 추출
                file_name_element = area.find_element(By.CSS_SELECTOR, ".file_name a span")
                file_name = file_name_element.text.strip()
                
                download_link = area.find_element(By.CSS_SELECTOR, ".file_name a")
                href = download_link.get_attribute("href")
                
                if href and file_name:
                    log_message(f"PDF 파일 발견: {file_name}")
                    log_message(f"다운로드 URL: {href}")
                    
                    # PDF 파일을 서버에 저장
                    server_url = save_to_server(driver, href, file_name)
                    
                    if server_url:
                        pdf_info_list.append({
                            "file_name": file_name,
                            "download_url": server_url
                        })
                        log_message(f"PDF 파일 처리 완료: {file_name}")
                        log_message(f"서버 URL: {server_url}")
            except Exception as e:
                log_message(f"첨부파일 처리 중 오류: {str(e)}")
                continue
        
        return pdf_info_list
    except Exception as e:
        log_message(f"PDF 다운로드 링크 추출 중 오류: {str(e)}")
        return []

def crawl_boss_pdf():
    try:
        # 로깅 및 초기 설정
        log_message("보스 자료 크롤링 시작: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
        # Selenium 드라이버 설정
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')  # 헤드리스 모드
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        driver = webdriver.Chrome(options=options)
        
        # 로그인 시도
        if not login_to_iboss(driver):
            log_message("로그인 실패로 크롤링을 중단합니다.")
            return []
        
        # 페이지 접속
        url = "https://www.i-boss.co.kr/ab-3207"  # 아이보스 사이트 URL
        driver.get(url)
        log_message(f"페이지 접속 시도... URL: {url}")
        
        # 페이지 로딩 대기
        time.sleep(5)
        
        # 결과 저장을 위한 배열
        results = []
        
        # 게시물 목록 찾기
        posts = driver.find_elements(By.CSS_SELECTOR, ".cell.fixed_")
        total_posts = len(posts)
        log_message(f"총 {total_posts}개의 게시물을 찾았습니다.")
        
        # 각 게시물의 정보를 저장할 배열
        post_info_list = []
        
        # 먼저 모든 게시물의 기본 정보를 수집
        for post in posts:
            try:
                title_element = post.find_element(By.CSS_SELECTOR, "div.title strong")
                title = title_element.text.strip()
                
                link_element = post.find_element(By.CSS_SELECTOR, "a")
                link = link_element.get_attribute("href")
                
                date_element = post.find_element(By.CSS_SELECTOR, "span.date")
                date = date_element.text.strip()
                
                category_element = post.find_element(By.CSS_SELECTOR, "span.category a")
                category = category_element.text.strip()
                
                # PDF 아이콘 확인
                has_pdf = False
                try:
                    pdf_icons = post.find_elements(By.CSS_SELECTOR, "img[src*='pdf']")
                    has_pdf = len(pdf_icons) > 0
                except:
                    pass
                
                if has_pdf:
                    post_info_list.append({
                        "title": title,
                        "link": link,
                        "date": date,
                        "category": category
                    })
                    log_message(f"PDF 게시물 정보 수집: {title[:50]}...")
                
            except Exception as e:
                log_message(f"게시물 정보 수집 중 오류: {str(e)}")
                continue
        
        log_message(f"총 {len(post_info_list)}개의 PDF 게시물 정보를 수집했습니다.")
        
        # 수집된 정보를 바탕으로 상세 페이지 방문
        for post_info in post_info_list:
            try:
                # 상세 페이지 방문
                driver.get(post_info["link"])
                time.sleep(2)  # 페이지 로딩 대기
                
                # 내용 추출
                content = ""
                content_element = None
                try:
                    content_selectors = [
                        ".ABA-view-body",
                        ".ABA-article-contents",
                        "#bo_content",
                        ".read_body",
                        ".article-body"
                    ]
                    
                    for selector in content_selectors:
                        try:
                            content_element = driver.find_element(By.CSS_SELECTOR, selector)
                            content = content_element.text.strip()
                            if content:
                                break
                        except:
                            continue
                except Exception as e:
                    log_message(f"내용 추출 중 오류: {str(e)}")
                
                # PDF 다운로드 링크 추출
                pdf_links = []
                if content_element:
                    pdf_links = get_pdf_download_links(driver, content_element)
                
                # 결과 저장
                results.append({
                    "title": post_info["title"],
                    "link": post_info["link"],
                    "date": post_info["date"],
                    "category": post_info["category"],
                    "file_type": "PDF",
                    "download_count": "정보 없음",
                    "has_pdf_icon": True,
                    "content": content,
                    "pdf_links": pdf_links
                })
                
                log_message(f"PDF 게시물 #{len(results)} 추출 성공: {post_info['title'][:50]}...")
                if pdf_links:
                    log_message(f"발견된 PDF 링크 수: {len(pdf_links)}")
                
            except Exception as e:
                log_message(f"상세 페이지 처리 중 오류: {str(e)}")
                continue
        
        log_message(f"총 {len(results)}개의 PDF 게시물을 추출했습니다.")
        return results
        
    except Exception as e:
        error_msg = f"크롤링 중 오류: {str(e)}"
        log_message(error_msg)
        return []
    finally:
        try:
            if 'driver' in locals():
                driver.quit()
                log_message("드라이버 종료")
        except:
            pass

# 메인 코드 (필요한 경우)
if __name__ == "__main__":
    print("크롤링 시작:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    try:
        # 드라이버 초기화
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')  # 헤드리스 모드
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        driver = webdriver.Chrome(options=options)
        
        # 크롤링 실행
        results = crawl_boss_pdf()
        
        # 결과 처리 - 구글 스프레드시트에 저장
        if results:
            print(f"크롤링 완료: {len(results)}개의 PDF 게시물 정보를 추출했습니다.")
            
            try:
                # Google Sheets 설정
                scope = [
                    'https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive'
                ]
                
                # 서비스 계정 JSON 파일 경로 설정
                SERVICE_ACCOUNT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'naver-452205-a733573ea425.json')
                print(f"서비스 계정 파일 경로: {SERVICE_ACCOUNT_FILE}")
                print(f"파일 존재 여부: {os.path.exists(SERVICE_ACCOUNT_FILE)}")
                
                credentials = service_account.Credentials.from_service_account_file(
                    SERVICE_ACCOUNT_FILE,
                    scopes=scope
                )
                
                print(f"서비스 계정 이메일: {credentials.service_account_email}")
                
                # gspread 클라이언트 생성
                gc = gspread.authorize(credentials)
                print("gspread 인증 성공")
                
                # 스프레드시트 열기 - 정확한 ID 확인
                current_spreadsheet_id = SPREADSHEET_ID
                print(f"사용 중인 스프레드시트 ID: {current_spreadsheet_id}")
                
                try:
                    # 스프레드시트 열기 전 접근 가능한 모든 스프레드시트 목록 확인
                    all_spreadsheets = gc.openall()
                    print(f"접근 가능한 스프레드시트 목록:")
                    for sheet in all_spreadsheets:
                        print(f"- {sheet.title} (ID: {sheet.id})")
                except Exception as e:
                    print(f"스프레드시트 목록 조회 실패: {str(e)}")
                
                try:
                    # 직접 sheets API 사용
                    service = build('sheets', 'v4', credentials=credentials)
                    print("Sheets API 서비스 생성 성공")
                    
                    # 스프레드시트 정보 요청
                    spreadsheet_info = service.spreadsheets().get(spreadsheetId=current_spreadsheet_id).execute()
                    print(f"스프레드시트 정보 가져오기 성공: {spreadsheet_info['properties']['title']}")
                    
                    # 기존 시트 정보 확인
                    sheets = spreadsheet_info.get('sheets', [])
                    sheet_titles = [sheet['properties']['title'] for sheet in sheets]
                    print(f"스프레드시트 내 시트 목록: {sheet_titles}")
                    
                    # 두 개의 시트에 데이터 추가하기 위한 함수 정의
                    def process_sheet_data(worksheet_title):
                        print(f"\n===== {worksheet_title} 시트 처리 시작 =====")
                        
                        # 시트 정보 가져오기
                        sheet_info = next((sheet for sheet in sheets if sheet['properties']['title'] == worksheet_title), None)
                        if not sheet_info:
                            print(f"{worksheet_title} 시트를 찾을 수 없습니다.")
                            return 0
                        
                        # sheet_id 가져오기
                        sheet_id = sheet_info['properties']['sheetId']
                        print(f"{worksheet_title} 시트 ID: {sheet_id}")
                        
                        # 현재 시트의 데이터 가져오기
                        result = service.spreadsheets().values().get(
                            spreadsheetId=current_spreadsheet_id,
                            range=f"{worksheet_title}!A:H"
                        ).execute()
                        
                        current_values = result.get('values', [])
                        if not current_values:
                            print(f"{worksheet_title} - 시트가 비어있습니다. 헤더 추가...")
                            headers = ['제목', '작성일', '링크', 'PDF 링크', '내용', '중요여부', '파일명', '마지막 업데이트']
                            service.spreadsheets().values().update(
                                spreadsheetId=current_spreadsheet_id,
                                range=f"{worksheet_title}!A1:H1",
                                valueInputOption="RAW",
                                body={"values": [headers]}
                            ).execute()
                            current_values = [headers]
                        
                        # 기존 제목 목록 가져오기 (중복 체크용)
                        existing_titles = [row[0] for row in current_values[1:] if row]
                        
                        # 새로운 데이터를 저장할 리스트
                        new_rows = []
                        new_data_count = 0
                        
                        for idx, result in enumerate(results):
                            try:
                                title = result.get('title', '')
                                print(f"{worksheet_title} - 처리 중인 결과 #{idx+1}: {title[:30]}...")
                                
                                # 중복 체크
                                if title in existing_titles:
                                    print(f"{worksheet_title} - 중복 항목 건너뜀: {title[:30]}...")
                                    continue
                                
                                # PDF 정보 처리
                                pdf_links = []
                                pdf_names = []
                                for pdf_info in result.get('pdf_links', []):
                                    pdf_links.append(pdf_info['download_url'])
                                    pdf_names.append(pdf_info['file_name'])
                                
                                # 현재 날짜 및 시간 추가
                                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                
                                # 새 행 데이터 설정
                                row_data = [
                                    title,                                  # A열: 제목
                                    result.get('date', ''),                 # B열: 작성일
                                    result.get('link', ''),                 # C열: 링크
                                    ', '.join(pdf_links) if pdf_links else '', # D열: PDF 링크
                                    result.get('content', ''),              # E열: 내용
                                    '',                                     # F열: 중요여부
                                    ', '.join(pdf_names) if pdf_names else '', # G열: 파일명
                                    now                                     # H열: 마지막 업데이트
                                ]
                                
                                new_rows.append(row_data)
                                new_data_count += 1
                                print(f"{worksheet_title} - 항목 #{idx+1} 데이터 생성 완료")
                                
                            except Exception as e:
                                print(f"{worksheet_title} - 행 데이터 생성 중 오류: {str(e)}")
                        
                        if new_rows:
                            print(f"{worksheet_title} - {new_data_count}개의 새 항목을 추가합니다...")
                            
                            # 2번째 행에 새로운 행들을 삽입
                            for row_data in reversed(new_rows):  # 역순으로 처리하여 순서 유지
                                # 2번째 행에 빈 행 삽입
                                request = {
                                    'insertDimension': {
                                        'range': {
                                            'sheetId': sheet_id,
                                            'dimension': 'ROWS',
                                            'startIndex': 1,  # 2번째 행 (0-based index)
                                            'endIndex': 2
                                        }
                                    }
                                }
                                
                                service.spreadsheets().batchUpdate(
                                    spreadsheetId=current_spreadsheet_id,
                                    body={'requests': [request]}
                                ).execute()
                                
                                # 삽입된 행에 데이터 입력
                                update_range = f"{worksheet_title}!A2:H2"
                                service.spreadsheets().values().update(
                                    spreadsheetId=current_spreadsheet_id,
                                    range=update_range,
                                    valueInputOption="RAW",
                                    body={"values": [row_data]}
                                ).execute()
                            
                            print(f"{worksheet_title} - 업데이트 완료: {new_data_count}개의 새 항목이 2번째 행에 추가됨")
                        else:
                            print(f"{worksheet_title} - 추가할 새 항목이 없습니다")
                        
                        # 최종 확인
                        final_result = service.spreadsheets().values().get(
                            spreadsheetId=current_spreadsheet_id,
                            range=f"{worksheet_title}!A:H"
                        ).execute()
                        
                        final_values = final_result.get('values', [])
                        print(f"{worksheet_title} - 업데이트 후 시트에 {len(final_values)} 행의 데이터가 있습니다")
                        print(f"===== {worksheet_title} 시트 처리 완료 =====\n")
                        
                        return new_data_count
                    
                    # 첫 번째 시트 (Boss_pdf) 처리
                    pdf_count = process_sheet_data('Boss_pdf')
                    print(f"Boss_pdf 시트에 {pdf_count}개의 항목이 추가되었습니다.")
                    
                    # 두 번째 시트 (Boss_pdf2) 처리
                    pdf2_count = process_sheet_data('Boss_pdf2')
                    print(f"Boss_pdf2 시트에 {pdf2_count}개의 항목이 추가되었습니다.")
                    
                    print("\n==== 모든 시트 처리 완료 ====")
                    print(f"총 추가된 항목 수: Boss_pdf({pdf_count}), Boss_pdf2({pdf2_count})")
                    
                except Exception as e:
                    print(f"스프레드시트 작업 중 오류: {str(e)}")
                    import traceback
                    print(f"상세 오류: {traceback.format_exc()}")
                    
                    # 백업 방법 시도: 일반 gspread 사용
                    print("백업 방법으로 gspread 라이브러리 사용 시도...")
                    
                    def backup_process_sheet(sheet_name):
                        print(f"\n===== 백업: {sheet_name} 시트 처리 시작 =====")
                        spreadsheet = gc.open_by_key(current_spreadsheet_id)
                        print(f"스프레드시트 '{spreadsheet.title}' 열기 성공")
                        
                        try:
                            sheet = spreadsheet.worksheet(sheet_name)
                            print(f"{sheet_name} 시트 열기 성공")
                        except gspread.exceptions.WorksheetNotFound:
                            sheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=10)
                            sheet.append_row(['제목', '작성일', '링크', 'PDF 링크', '내용', '중요여부', '파일명', '마지막 업데이트'])
                            print(f"{sheet_name} 시트 생성 및 헤더 추가 완료")
                        
                        # 단일 행씩 추가 시도
                        new_data_count = 0
                        all_values = sheet.get_all_values()
                        existing_titles = [row[0] for row in all_values[1:] if row]
                        
                        for idx, result in enumerate(results):
                            title = result.get('title', '')
                            if title in existing_titles:
                                print(f"{sheet_name} - 백업: 중복 항목 건너뜀: {title[:30]}...")
                                continue
                            
                            # PDF 정보 처리
                            pdf_links = []
                            pdf_names = []
                            for pdf_info in result.get('pdf_links', []):
                                pdf_links.append(pdf_info['download_url'])
                                pdf_names.append(pdf_info['file_name'])
                            
                            # 현재 날짜 및 시간
                            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            
                            # 데이터 설정
                            row_data = [
                                title,                                   # A열: 제목
                                result.get('date', ''),                  # B열: 작성일
                                result.get('link', ''),                  # C열: 링크
                                ', '.join(pdf_links) if pdf_links else '',  # D열: PDF 링크
                                result.get('content', ''),               # E열: 내용
                                '',                                      # F열: 중요여부
                                ', '.join(pdf_names) if pdf_names else '',  # G열: 파일명
                                now                                      # H열: 마지막 업데이트
                            ]
                            
                            try:
                                # append_row를 사용하여 전체 행을 한 번에 추가 시도
                                print(f"{sheet_name} - 백업: 행 추가 시도... {title[:30]}")
                                sheet.append_row(row_data)
                                new_data_count += 1
                                print(f"{sheet_name} - 백업: 행 추가 완료 ({new_data_count})")
                                
                                # API 제한 방지
                                time.sleep(1)
                                
                            except Exception as e:
                                print(f"{sheet_name} - 백업: 행 추가 중 오류: {str(e)}")
                                
                                try:
                                    # 각 항목별로 update_cell 사용 시도
                                    row_num = len(sheet.get_all_values()) + 1
                                    print(f"{sheet_name} - 백업: 셀 업데이트 시도... 행 {row_num}")
                                    
                                    for col_idx, value in enumerate(row_data, start=1):
                                        sheet.update_cell(row_num, col_idx, value)
                                        print(f"{sheet_name} - 백업: 셀 {row_num}행 {col_idx}열 업데이트 완료")
                                        time.sleep(0.5)  # API 제한 방지
                                    
                                    new_data_count += 1
                                    print(f"{sheet_name} - 백업: 행 {row_num} 추가 완료")
                                except Exception as e2:
                                    print(f"{sheet_name} - 백업: 셀 업데이트 중 오류: {str(e2)}")
                            
                        print(f"===== 백업: {sheet_name} 시트 처리 완료 ({new_data_count}개 항목 추가) =====\n")
                        return new_data_count
                    
                    # 두 시트에 대해 백업 처리 수행
                    backup1_count = backup_process_sheet('Boss_pdf')
                    backup2_count = backup_process_sheet('Boss_pdf2')
                    print(f"\n==== 백업 방법 처리 완료 ====")
                    print(f"총 추가된 항목 수: Boss_pdf({backup1_count}), Boss_pdf2({backup2_count})")
                
            except Exception as e:
                print(f"스프레드시트 업데이트 중 오류: {str(e)}")
                import traceback
                print(f"상세 오류: {traceback.format_exc()}")
        else:
            print("크롤링 결과가 없습니다.")
            
    except Exception as e:
        print(f"크롤링 중 오류 발생: {str(e)}")
        import traceback
        print(f"상세 오류: {traceback.format_exc()}")
    finally:
        # 드라이버 종료
        if 'driver' in locals():
            driver.quit()
            print("드라이버 종료")

# 추가 조치
# 1. **크롤링 코드 단순화**
#    문제를 좁히기 위해 PDF 다운로드 및 업로드를 생략하고 기본 데이터만 스프레드시트에 추가하는 간소화된 버전의 코드를 시도해보세요.
# 2. **스프레드시트 권한 확인**
#    서비스 계정이 스프레드시트에 쓰기 권한이 있는지 확인하세요. 스프레드시트 공유 설정에서 서비스 계정 이메일(`crawling@naver-452205.iam.gserviceaccount.com`)에 편집 권한이 있어야 합니다.
# 3. **서비스 계정 키 재생성**
#   Google Cloud Console에서 서비스 계정 키를 재생성하고 새 키를 사용해보세요.
# 4. **크롤링 코드 단순화**
#   문제를 좁히기 위해 PDF 다운로드 및 업로드를 생략하고 기본 데이터만 스프레드시트에 추가하는 간소화된 버전의 코드를 시도해보세요.
# 5. **스프레드시트 수동 업데이트 확인**
#   스프레드시트가 제대로 열리는지 직접 확인하세요:
#   ```python
#   import gspread
#   from google.oauth2 import service_account
#   
#   SERVICE_ACCOUNT_FILE = '경로/naver-452205-a733573ea425.json'
#   scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
#   credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scope)
#   gc = gspread.authorize(credentials)
#   
#   spreadsheet = gc.open_by_key('1shWpyaGrQF00YKkmYGftL2IAEOgmZ8kjw2s-WKbdyGg')
#   sheet = spreadsheet.worksheet('Boss_pdf_V2')
#   
#   # 테스트 데이터 추가
#   sheet.append_row(['테스트 제목', '테스트 날짜', 'https://example.com', '', '테스트 내용', '테스트파일.pdf'])
#   print("데이터 추가 성공")
#   ```
# 6. **추가 조치**
#   현재 스프레드시트가 비어 있는 것으로 보아 크롤링 과정 초기에 문제가 발생하는 것 같습니다. 로그를 더 자세히 확인하고 위의 단계를 통해 문제를 해결해보세요. 
