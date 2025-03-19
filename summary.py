from openai import OpenAI
import gspread
from google.oauth2 import service_account
import time
from datetime import datetime
import logging
import os

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

# 환경 변수에서 API 키 읽기
try:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("API 키가 비어 있습니다. 환경 변수를 확인하세요.")
except KeyError:
    raise ValueError("OPENAI_API_KEY 환경 변수가 설정되지 않았습니다.")

# Google Sheets 설정
SPREADSHEET_ID = '1shWpyaGrQF00YKkmYGftL2IAEOgmZ8kjw2s-WKbdyGg'
SHEET_NAMES = ['Naver_Ads', 'Google_Ads', 'Meta_Ads']

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
        
        # gspread 클라이언트 생성
        gc = gspread.authorize(credentials)
        print("gspread 인증 성공")
        
        # 스프레드시트 열기
        try:
            spreadsheet = gc.open_by_key(SPREADSHEET_ID)
            print("스프레드시트 열기 성공")
            return spreadsheet
            
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"스프레드시트를 찾을 수 없습니다. ID: {SPREADSHEET_ID}")
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

def summarize_text(text, max_length=200):
    """OpenAI API를 사용하여 텍스트 요약"""
    if not text or len(text) < 100:
        return text
    
    try:
        client = OpenAI(api_key=api_key)
        
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "너는 광고 플랫폼 공지사항을 요약하는 전문가야. 다음 형식으로 요약해줘: [주요 변경사항], [적용 일정], [영향 및 조치사항]. 문장이 자연스럽게 끝나도록 해줘."},
                {"role": "user", "content": f"다음 공지사항을 요약해줘:\n\n{text}"}
            ],
            max_tokens=150,
            temperature=0.3
        )
        
        summary = response.choices[0].message.content.strip()
        return summary
    
    except Exception as e:
        print(f"요약 중 오류 발생: {str(e)}")
        return "요약 생성 중 오류가 발생했습니다."

def process_sheet(sheet):
    """시트의 내용을 가져와서 요약이 필요한 항목 처리"""
    print(f"{sheet.title} 시트 처리 중...")
    
    # 모든 데이터 가져오기
    data = sheet.get_all_values()
    
    if len(data) <= 1:  # 헤더만 있는 경우
        print(f"{sheet.title} 시트에 데이터가 없습니다.")
        return 0
    
    # 헤더 제외한 데이터
    rows = data[1:]
    
    # 요약이 필요한 행 찾기
    rows_to_update = []
    for i, row in enumerate(rows, start=2):  # 시트 인덱스는 1부터 시작, 헤더가 1행이므로 2부터
        if len(row) >= 6 and row[4] and not row[5]:  # 내용은 있고 요약은 없는 경우
            rows_to_update.append((i, row))
    
    print(f"요약이 필요한 항목 수: {len(rows_to_update)}")
    
    # 요약 처리
    updated_count = 0
    for row_idx, row in rows_to_update:
        try:
            content = row[4]
            print(f"'{row[0]}' 요약 중...")
            
            summary = summarize_text(content)
            
            # 요약 결과 업데이트
            sheet.update_cell(row_idx, 6, summary)  # 6번째 열이 요약 열
            
            updated_count += 1
            print(f"'{row[0]}' 요약 완료")
            
            # API 호출 제한 방지를 위한 대기
            time.sleep(1)
            
        except Exception as e:
            print(f"행 {row_idx} 처리 중 오류 발생: {str(e)}")
    
    return updated_count

def run_summary():
    """모든 시트에 대해 요약 처리 실행"""
    try:
        print("요약 프로그램 시작:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
        # Google Sheets 연결
        spreadsheet = setup_google_sheets()
        
        total_updated = 0
        
        # 각 시트 처리
        for sheet_name in SHEET_NAMES:
            try:
                sheet = spreadsheet.worksheet(sheet_name)
                updated = process_sheet(sheet)
                total_updated += updated
                print(f"{sheet_name} 시트 처리 완료: {updated}개 항목 요약됨")
            except Exception as e:
                print(f"{sheet_name} 시트 처리 중 오류 발생: {str(e)}")
        
        print(f"총 {total_updated}개 항목 요약 완료")
        
    except Exception as e:
        print(f"요약 프로그램 실행 중 오류 발생: {str(e)}")
        raise
    finally:
        print("요약 프로그램 종료:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

if __name__ == "__main__":
    try:
        logging.info("요약 프로그램 시작")
        run_summary()
        logging.info("요약 프로그램 완료")
    except Exception as e:
        error_msg = f"요약 프로그램 중 오류 발생: {str(e)}"
        print(error_msg)
        logging.error(error_msg)
        # 에러 로그 기록
        with open(error_log_file, 'a') as f:
            f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Error: {str(e)}\n")
