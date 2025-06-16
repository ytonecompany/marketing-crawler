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
        
        # 현재 URL 로깅
        current_url = driver.current_url
        log_message(f"현재 페이지 URL: {current_url}")
        
        try:
            # 이메일 입력 필드 대기 및 입력
            email_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='user_id']"))
            )
            email_input.clear()
            time.sleep(1)
            email_input.send_keys("business@y-tone.co.kr")
            log_message("이메일 입력 완료")
            
            # 비밀번호 입력
            password_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='user_passwd']"))
            )
            password_input.clear()
            time.sleep(1)
            password_input.send_keys("ytonecompany1!")
            log_message("비밀번호 입력 완료")
            
            # 로그인 버튼 찾기 및 클릭
            login_button = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='submit_OK']"))
            )
            
            # JavaScript로 클릭 이벤트 실행
            driver.execute_script("arguments[0].click();", login_button)
            log_message("로그인 버튼 클릭")
            
            # 로그인 후 페이지 로딩 대기
            time.sleep(5)
            
            # 로그인 성공 여부 확인
            success_indicators = [
                "a[href*='logout']",  # 로그아웃 링크
                ".user-menu",         # 사용자 메뉴
                ".mypage",            # 마이페이지
                "#member_info"        # 회원 정보
            ]
            
            for indicator in success_indicators:
                try:
                    element = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, indicator))
                    )
                    log_message(f"로그인 성공 확인: {indicator} 요소 발견")
                    
                    # 로그인 후 URL 확인
                    after_login_url = driver.current_url
                    log_message(f"로그인 후 URL: {after_login_url}")
                    
                    return True
                except:
                    continue
            
            # 로그인 실패 시 페이지 소스 확인
            log_message("로그인 상태 확인 실패. 현재 페이지 정보:")
            log_message(f"현재 URL: {driver.current_url}")
            log_message(f"페이지 제목: {driver.title}")
            
            # 에러 메시지 확인
            try:
                error_messages = driver.find_elements(By.CSS_SELECTOR, ".error_message, .alert, #msg_error")
                for error in error_messages:
                    log_message(f"발견된 에러 메시지: {error.text}")
            except:
                pass
            
            return False
            
        except Exception as e:
            log_message(f"로그인 과정 중 오류: {str(e)}")
            return False
            
    except Exception as e:
        log_message(f"로그인 시도 중 예외 발생: {str(e)}")
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

def check_and_clean_drive_space(drive_service):
    """드라이브 공간을 확인하고 필요한 경우 오래된 파일을 정리"""
    try:
        # pdf_storage 폴더 찾기
        folder_results = drive_service.files().list(
            q="name='pdf_storage' and mimeType='application/vnd.google-apps.folder'",
            fields="files(id, name)"
        ).execute()
        
        folders = folder_results.get('files', [])
        if not folders:
            log_message("pdf_storage 폴더를 찾을 수 없습니다.")
            # 폴더 생성
            folder_metadata = {
                'name': 'pdf_storage',
                'mimeType': 'application/vnd.google-apps.folder'
            }
            folder = drive_service.files().create(
                body=folder_metadata,
                fields='id'
            ).execute()
            folder_id = folder.get('id')
            log_message("pdf_storage 폴더가 생성되었습니다.")
        else:
            folder_id = folders[0]['id']
            log_message("pdf_storage 폴더를 찾았습니다.")
        
        # 폴더 내 파일 목록 조회
        results = drive_service.files().list(
            q=f"'{folder_id}' in parents and mimeType='application/pdf'",
            fields="files(id, name, createdTime, size, md5Checksum)",
            orderBy="createdTime"
        ).execute()
        
        files = results.get('files', [])
        log_message(f"pdf_storage 폴더 내 총 {len(files)}개 PDF 파일 발견")
        
        # 6개월 이상된 파일 처리
        six_months_ago = (datetime.now() - timedelta(days=180)).isoformat() + 'Z'  # ISO 형식으로 변환
        old_files_to_delete = []
        old_files_size = 0
        
        for file in files:
            created_time = file.get('createdTime', '')
            if created_time and created_time < six_months_ago:
                old_files_to_delete.append(file)
                old_files_size += int(file.get('size', 0))
        
        if old_files_to_delete:
            log_message(f"\n6개월 이상된 파일 정리 시작: {len(old_files_to_delete)}개 파일")
            for file in old_files_to_delete:
                try:
                    drive_service.files().delete(fileId=file['id']).execute()
                    created_date = datetime.fromisoformat(file['createdTime'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
                    log_message(f"오래된 파일 삭제: {file['name']} (생성일: {created_date})")
                except Exception as e:
                    log_message(f"파일 삭제 실패 {file['name']}: {str(e)}")
            
            log_message(f"6개월 이상된 파일 정리 완료: {len(old_files_to_delete)}개 파일 삭제 ({old_files_size/(1024*1024):.2f}MB 확보)")
        
        # 중복 파일 처리
        # 남은 파일들 다시 조회
        results = drive_service.files().list(
            q=f"'{folder_id}' in parents and mimeType='application/pdf'",
            fields="files(id, name, createdTime, size, md5Checksum)",
            orderBy="createdTime"
        ).execute()
        
        files = results.get('files', [])
        files_by_name = {}
        files_by_checksum = {}
        duplicates_to_delete = set()
        
        # 파일들을 이름과 체크섬으로 그룹화
        for file in files:
            name = file['name']
            checksum = file.get('md5Checksum')
            
            # 이름으로 그룹화
            if name not in files_by_name:
                files_by_name[name] = []
            files_by_name[name].append(file)
            
            # 체크섬으로 그룹화 (체크섬이 있는 경우)
            if checksum:
                if checksum not in files_by_checksum:
                    files_by_checksum[checksum] = []
                files_by_checksum[checksum].append(file)
        
        # 중복 파일 찾기
        deleted_size = 0
        duplicate_count = 0
        
        # 1. 동일 이름 파일 처리
        for name, file_list in files_by_name.items():
            if len(file_list) > 1:
                # 생성일 기준으로 정렬
                file_list.sort(key=lambda x: x.get('createdTime', ''), reverse=True)
                # 가장 최신 파일을 제외한 나머지를 삭제 목록에 추가
                for file in file_list[1:]:
                    duplicates_to_delete.add(file['id'])
                    duplicate_count += 1
                    deleted_size += int(file.get('size', 0))
                    log_message(f"중복 파일 발견 (이름): {name} - {file.get('createdTime', '')}")
        
        # 2. 동일 체크섬 파일 처리
        for checksum, file_list in files_by_checksum.items():
            if len(file_list) > 1:
                # 생성일 기준으로 정렬
                file_list.sort(key=lambda x: x.get('createdTime', ''), reverse=True)
                # 가장 최신 파일을 제외한 나머지를 삭제 목록에 추가
                for file in file_list[1:]:
                    if file['id'] not in duplicates_to_delete:  # 아직 삭제 목록에 없는 경우만
                        duplicates_to_delete.add(file['id'])
                        duplicate_count += 1
                        deleted_size += int(file.get('size', 0))
                        log_message(f"중복 파일 발견 (내용): {file['name']} - {file.get('createdTime', '')}")
        
        # 중복 파일 삭제
        if duplicates_to_delete:
            log_message(f"\n중복 파일 정리 시작: {len(duplicates_to_delete)}개 파일")
            for file_id in duplicates_to_delete:
                try:
                    drive_service.files().delete(fileId=file_id).execute()
                except Exception as e:
                    log_message(f"파일 삭제 실패 (ID: {file_id}): {str(e)}")
            
            log_message(f"중복 파일 정리 완료: {len(duplicates_to_delete)}개 파일 삭제 ({deleted_size/(1024*1024):.2f}MB 확보)")
        
        # 드라이브 사용량 확인
        about = drive_service.about().get(fields="storageQuota").execute()
        quota = about.get('storageQuota', {})
        used = int(quota.get('usage', 0))
        total = int(quota.get('limit', 0))
        available = total - used
        
        log_message(f"\n드라이브 사용량: {used/(1024*1024*1024):.2f}GB / {total/(1024*1024*1024):.2f}GB")
        log_message(f"남은 공간: {available/(1024*1024*1024):.2f}GB")
        
        # 저장 공간이 90% 이상 사용된 경우 오래된 파일부터 삭제
        if used > total * 0.9 and files:
            log_message("\n저장 공간이 90% 이상 사용됨. pdf_storage 폴더 내 오래된 파일 정리 시작")
            
            # 중복 파일 삭제 후 남은 파일들 다시 조회
            results = drive_service.files().list(
                q=f"'{folder_id}' in parents and mimeType='application/pdf'",
                fields="files(id, name, createdTime, size)",
                orderBy="createdTime"
            ).execute()
            
            remaining_files = results.get('files', [])
            remaining_files.sort(key=lambda x: x.get('createdTime', ''))  # 생성일 기준 정렬
            
            # 30% 정도의 파일을 삭제
            files_to_delete = remaining_files[:int(len(remaining_files) * 0.3)]
            old_deleted_size = 0
            for file in files_to_delete:
                try:
                    drive_service.files().delete(fileId=file['id']).execute()
                    file_size = int(file.get('size', 0))
                    old_deleted_size += file_size
                    log_message(f"오래된 파일 삭제됨: {file['name']} ({file_size/(1024*1024):.2f}MB)")
                except Exception as e:
                    log_message(f"파일 삭제 실패 {file['name']}: {str(e)}")
            
            log_message(f"오래된 파일 정리 완료: {len(files_to_delete)}개 파일 삭제 (총 {old_deleted_size/(1024*1024):.2f}MB 추가 확보)")
        
        # 최종 정리 결과
        total_deleted_size = old_files_size + deleted_size + (old_deleted_size if 'old_deleted_size' in locals() else 0)
        if total_deleted_size > 0:
            log_message(f"\n총 정리 결과: {total_deleted_size/(1024*1024):.2f}MB 공간 확보")
        
        return folder_id
    except Exception as e:
        log_message(f"드라이브 공간 확인 중 오류: {str(e)}")
        return None

def save_to_server(driver, download_url, file_name):
    """PDF 파일을 구글 드라이브에 업로드"""
    try:
        log_message(f"\n=== PDF 파일 저장 시작 ===")
        log_message(f"다운로드 URL: {download_url}")
        log_message(f"파일명: {file_name}")
        
        # 파일명에서 특수문자 제거 및 한글 처리
        safe_filename = "".join([c for c in file_name if c.isalnum() or c in (' ', '-', '_', '.', ')')]).rstrip()
        log_message(f"정제된 파일명: {safe_filename}")
        
        # 현재 세션의 쿠키 가져오기
        cookies = driver.get_cookies()
        session = requests.Session()
        for cookie in cookies:
            session.cookies.set(cookie['name'], cookie['value'])
        
        # 요청 헤더 설정
        headers = {
            'User-Agent': driver.execute_script("return navigator.userAgent;"),
            'Referer': driver.current_url
        }
        log_message("세션 및 헤더 설정 완료")
        
        # PDF 파일 다운로드 시도
        try:
            log_message("파일 다운로드 시도 중...")
            response = session.get(download_url, headers=headers, stream=True, timeout=30)
            log_message(f"서버 응답 상태 코드: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    # 응답 내용을 메모리에 저장
                    content = response.content
                    content_length = len(content)
                    log_message(f"파일 다운로드 완료: {content_length} bytes")
                    
                    if content_length < 1000:  # 파일이 너무 작은 경우
                        log_message(f"경고: 다운로드된 파일이 너무 작습니다 ({content_length} bytes)")
                        return None
                    
                    # 구글 드라이브 서비스 인증
                    SERVICE_ACCOUNT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'naver-452205-a733573ea425.json')
                    credentials = service_account.Credentials.from_service_account_file(
                        SERVICE_ACCOUNT_FILE,
                        scopes=['https://www.googleapis.com/auth/drive']
                    )
                    
                    # 드라이브 서비스 생성
                    drive_service = build('drive', 'v3', credentials=credentials)
                    log_message("구글 드라이브 서비스 인증 완료")
                    
                    # 드라이브 공간 확인 및 정리
                    folder_id = check_and_clean_drive_space(drive_service)
                    if not folder_id:
                        log_message("드라이브 공간 확인 실패")
                        return None
                    
                    # 기존 파일 검색
                    try:
                        results = drive_service.files().list(
                            q=f"name='{safe_filename}' and '{folder_id}' in parents",
                            spaces='drive'
                        ).execute()
                        
                        if results.get('files', []):
                            # 기존 파일이 있으면 해당 파일의 링크 반환
                            file_id = results['files'][0]['id']
                            return convert_to_direct_download_link(f"https://drive.google.com/file/d/{file_id}/view")
                    except Exception as e:
                        log_message(f"기존 파일 검색 중 오류: {str(e)}")
                    
                    # 임시 파일 생성 및 저장
                    temp_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), safe_filename)
                    with open(temp_file_path, 'wb') as f:
                        f.write(content)
                    log_message(f"임시 파일 저장 완료: {temp_file_path}")
                    
                    try:
                        # 파일 메타데이터 설정
                        file_metadata = {
                            'name': safe_filename,
                            'parents': [folder_id]  # pdf_storage 폴더 ID
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
                        log_message("임시 파일 삭제 완료")
                        
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
                log_message(f"PDF 다운로드 실패: 상태 코드 {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            log_message(f"파일 다운로드 요청 중 오류: {str(e)}")
            return None
            
    except Exception as e:
        log_message(f"파일 저장 중 예외 발생: {str(e)}")
        return None
    finally:
        log_message("=== PDF 파일 저장 종료 ===\n")

def get_pdf_download_links(driver, content_element):
    try:
        # PDF 다운로드 링크와 파일명을 함께 저장할 리스트
        pdf_info_list = []
        
        # 로그인 상태 재확인
        if not check_login_status(driver):
            log_message("PDF 다운로드 시도 전 로그인 상태 확인 실패")
            if not login_to_iboss(driver):
                log_message("재로그인 시도 실패")
                return []
        
        # 현재 페이지 URL 로깅
        current_url = driver.current_url
        log_message(f"현재 페이지 URL: {current_url}")
        
        try:
            # 페이지 소스 출력 (디버깅용)
            page_source = driver.page_source
            log_message("페이지 소스 확인:")
            log_message(page_source[:500])  # 처음 500자만 로깅
            
            # 모든 링크 요소 찾기
            all_links = driver.find_elements(By.TAG_NAME, "a")
            log_message(f"페이지 내 전체 링크 수: {len(all_links)}")
            
            # PDF 다운로드 링크 찾기 (여러 선택자 시도)
            download_selectors = [
                "a[id='content_download']",
                "a[href*='download_file.php']",
                ".attached_file_in_conts a",
                "a[href*='.pdf']",
                ".ABA-view-body a[href*='.pdf']",
                ".ABA-article-contents a[href*='.pdf']",
                "#bo_content a[href*='.pdf']",
                ".read_body a[href*='.pdf']",
                ".article-body a[href*='.pdf']",
                "a[href*='download']",
                "a[href*='attachment']",
                "a[onclick*='download']",
                "a[href*='file_download']",
                ".file_link a",
                ".download_link",
                ".pdf_link",
                "a[href*='files']",
                "a[href*='data']"
            ]
            
            for selector in download_selectors:
                log_message(f"선택자 {selector} 시도 중...")
                download_links = driver.find_elements(By.CSS_SELECTOR, selector)
                
                if download_links:
                    log_message(f"선택자 {selector}로 {len(download_links)}개의 링크 발견")
                    
                    for download_link in download_links:
                        try:
                            href = download_link.get_attribute("href")
                            # onclick 속성도 확인
                            onclick = download_link.get_attribute("onclick")
                            
                            # onclick에서 URL 추출 시도
                            if onclick and not href:
                                import re
                                url_match = re.search(r"window\.open\('([^']+)'", onclick)
                                if url_match:
                                    href = url_match.group(1)
                            
                            # 링크 텍스트나 span 내부 텍스트에서 파일명 추출 시도
                            file_name = ""
                            try:
                                # span 태그 내부 텍스트 확인
                                span = download_link.find_element(By.TAG_NAME, "span")
                                file_name = span.text.strip()
                            except:
                                # 직접 링크 텍스트 사용
                                file_name = download_link.text.strip()
                            
                            # href에서 파일명 추출 시도
                            if not file_name and href:
                                file_name = href.split('/')[-1]
                                if '?' in file_name:
                                    file_name = file_name.split('?')[0]
                            
                            # href나 파일명이 비어있지 않고, PDF 관련 키워드가 포함된 경우에만 처리
                            if href and (file_name or '.pdf' in href.lower()):
                                if not file_name:
                                    file_name = f"download_{int(time.time())}.pdf"
                                
                                log_message(f"PDF 파일 발견:")
                                log_message(f"- 파일명: {file_name}")
                                log_message(f"- 다운로드 URL: {href}")
                                
                                # PDF 파일을 서버에 저장
                                server_url = save_to_server(driver, href, file_name)
                                
                                if server_url:
                                    pdf_info_list.append({
                                        "file_name": file_name,
                                        "download_url": server_url
                                    })
                                    log_message(f"PDF 파일 처리 완료: {file_name}")
                                    log_message(f"서버 URL: {server_url}")
                                else:
                                    log_message(f"PDF 파일 저장 실패: {file_name}")
                        except Exception as e:
                            log_message(f"개별 PDF 링크 처리 중 오류: {str(e)}")
                            continue
                else:
                    log_message(f"선택자 {selector}로 링크를 찾을 수 없음")
            
            # 추가: iframe 내부 확인
            try:
                iframes = driver.find_elements(By.TAG_NAME, "iframe")
                for iframe in iframes:
                    try:
                        driver.switch_to.frame(iframe)
                        iframe_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='.pdf']")
                        for link in iframe_links:
                            href = link.get_attribute("href")
                            if href and '.pdf' in href.lower():
                                file_name = f"iframe_pdf_{int(time.time())}.pdf"
                                server_url = save_to_server(driver, href, file_name)
                                if server_url:
                                    pdf_info_list.append({
                                        "file_name": file_name,
                                        "download_url": server_url
                                    })
                        driver.switch_to.default_content()
                    except:
                        driver.switch_to.default_content()
                        continue
            except:
                pass
            
            if not pdf_info_list:
                log_message("어떤 선택자로도 PDF 링크를 찾을 수 없음")
                
        except Exception as e:
            log_message(f"PDF 링크 검색 중 오류: {str(e)}")
            import traceback
            log_message(f"상세 오류: {traceback.format_exc()}")
        
        return pdf_info_list
    except Exception as e:
        log_message(f"PDF 다운로드 링크 추출 중 오류: {str(e)}")
        return []

def check_login_status(driver):
    """로그인 상태를 확인하는 함수"""
    try:
        # 로그인 상태 확인을 위한 요소들
        success_indicators = [
            "a[href*='logout']",
            ".user-menu",
            ".mypage",
            "#member_info"
        ]
        
        for indicator in success_indicators:
            try:
                element = WebDriverWait(driver, 2).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, indicator))
                )
                return True
            except:
                continue
        
        return False
    except:
        return False

def crawl_boss_pdf():
    try:
        # 로깅 및 초기 설정
        log_message("보스 자료 크롤링 시작: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
        # Selenium 드라이버 설정
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')  # 헤드리스 모드
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        
        # User-Agent 설정
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36')
        
        if os.path.exists('/usr/bin/chromium-browser'):  # GitHub Actions 환경
            options.binary_location = '/usr/bin/chromium-browser'
            chrome_driver_path = '/usr/bin/chromedriver'
            service = Service(chrome_driver_path)
            driver = webdriver.Chrome(service=service, options=options)
        else:  # 로컬 환경
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

def process_missing_pdfs(sheet_name):
    """PDF 링크가 없는 항목들을 처리"""
    try:
        # Google Sheets 설정
        sheet = setup_google_sheets()
        if not sheet:
            return
            
        # 모든 데이터 가져오기
        all_data = sheet.get_all_values()
        if len(all_data) <= 1:  # 헤더만 있는 경우
            return
            
        # Selenium 드라이버 설정
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36')
        
        if os.path.exists('/usr/bin/chromium-browser'):  # GitHub Actions 환경
            options.binary_location = '/usr/bin/chromium-browser'
            chrome_driver_path = '/usr/bin/chromedriver'
            service = Service(chrome_driver_path)
            driver = webdriver.Chrome(service=service, options=options)
        else:  # 로컬 환경
            driver = webdriver.Chrome(options=options)
        
        try:
            # 로그인
            if not login_to_iboss(driver):
                log_message("로그인 실패")
                return
                
            # 각 행 처리
            for idx, row in enumerate(all_data[1:], start=2):  # 2부터 시작 (헤더 제외)
                try:
                    if len(row) >= 4 and not row[3].strip():  # D열(PDF 링크)이 비어있는 경우
                        title = row[0]
                        post_link = row[2]
                        log_message(f"\n처리 중: {title}")
                        
                        # 게시물 페이지 방문
                        driver.get(post_link)
                        time.sleep(3)
                        
                        # PDF 다운로드 링크 찾기
                        content_element = driver.find_element(By.CSS_SELECTOR, ".ABA-view-body")
                        pdf_links = get_pdf_download_links(driver, content_element)
                        
                        if pdf_links:
                            # PDF 링크와 파일명을 콤마로 구분하여 저장
                            pdf_urls = [link['download_url'] for link in pdf_links if link['download_url']]
                            pdf_names = [link['file_name'] for link in pdf_links if link['file_name']]
                            
                            if pdf_urls:
                                # D열(PDF 링크)와 G열(파일명) 업데이트
                                sheet.update_cell(idx, 4, ', '.join(pdf_urls))  # D열
                                sheet.update_cell(idx, 7, ', '.join(pdf_names))  # G열
                                log_message(f"업데이트 완료: {title}")
                                time.sleep(1)  # API 제한 방지
                except Exception as e:
                    log_message(f"행 처리 중 오류: {str(e)}")
                    continue
                    
        finally:
            driver.quit()
            
    except Exception as e:
        log_message(f"PDF 처리 중 오류: {str(e)}")

# 메인 코드에 process_missing_pdfs 호출 추가
if __name__ == "__main__":
    print("크롤링 시작:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    try:
        # PDF 링크가 없는 항목 처리
        print("\nPDF 링크가 없는 항목 처리 시작")
        process_missing_pdfs('Boss_pdf')
        print("모든 처리 완료")
        
    except Exception as e:
        print(f"실행 중 오류 발생: {str(e)}")
        import traceback
        print(f"상세 오류: {traceback.format_exc()}")
    finally:
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
