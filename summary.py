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
SHEET_NAMES = ['Naver_Ads', 'Google_Ads', 'Meta_Ads', 'Boss_pdf', 'Boss_pdf2']
# 30년차 조언이 필요한 시트 목록
ADVICE_SHEET_NAMES = ['Naver_Ads', 'Google_Ads', 'Meta_Ads']

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

def summarize_text(text, max_length=200, sheet_name=None):
    """OpenAI API를 사용하여 텍스트 요약"""
    if not text or len(text) < 100:
        return text
    
    try:
        client = OpenAI(api_key=api_key)
        
        # Boss_pdf와 Boss_pdf2는 핵심 내용을 넘버링 형식으로 요약
        if sheet_name in ['Boss_pdf', 'Boss_pdf2']:
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "너는 마케팅 자료와 광고 플랫폼 공지사항을 핵심 요약하는 전문가야. 다음 형식으로 정확히 요약해줘:\n\n1. [첫 번째 핵심 포인트]\n2. [두 번째 핵심 포인트]\n3. [세 번째 핵심 포인트]\n\n각 포인트는 간결하고 명확하게 작성하고, 전체 요약은 200자 이내로 제한해줘. 각 항목을 줄바꿈으로 구분해줘. 다른 설명이나 서식은 추가하지 마.\n\n중요: 내용이 부족하거나 없는 경우에는 해당 번호를 표시하지 마세요. 실질적인 내용이 있는 항목만 번호를 부여하세요. 예를 들어 2개 포인트만 있으면 1과 2만 사용하고, 1개만 있으면 1만 사용하세요."},
                    {"role": "user", "content": f"다음 마케팅 자료의 핵심 내용을 넘버링 포인트로 요약해줘 (최대 3개):\n\n{text}"}
                ],
                max_tokens=200,
                temperature=0.3,
                presence_penalty=0.2,
                frequency_penalty=0.2
            )
        else:
            # 기존 로직 - 다른 시트들은 이전 요약 형식 유지
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "너는 마케팅 자료와 광고 플랫폼 공지사항을 요약하는 전문가야. 다음 형식으로 요약해줘: [주요 내용], [중요 정보], [활용 방안]. 모든 문장이 완전하게 끝나도록 하고, 중간에 잘리지 않게 해줘. 특히 마지막 문장이 자연스럽게 완결되어야 해."},
                    {"role": "user", "content": f"다음 내용을 간결하게 요약해줘:\n\n{text}"}
                ],
                max_tokens=200,
                temperature=0.3,
                presence_penalty=0.2,
                frequency_penalty=0.2
            )
        
        summary = response.choices[0].message.content.strip()
        
        # 문장이 중간에 끊기지 않았는지 확인하고, 필요하면 마지막 문장을 제거
        if summary and not summary.endswith(('.', '!', '?', '다.', '요.', '임.', '됨.')):
            last_period_index = max(
                summary.rfind('.'), 
                summary.rfind('다.'), 
                summary.rfind('요.'),
                summary.rfind('임.'),
                summary.rfind('됨.'),
                summary.rfind('!'),
                summary.rfind('?')
            )
            if last_period_index > 0:
                summary = summary[:last_period_index+1]
        
        return summary
    
    except Exception as e:
        print(f"요약 중 오류 발생: {str(e)}")
        return "요약 생성 중 오류가 발생했습니다."

def generate_expert_advice(content, summary=None):
    """OpenAI API를 사용하여 30년차 디지털 마케터의 조언 생성"""
    if not content:
        return ""
    
    try:
        client = OpenAI(api_key=api_key)
        
        # 원본 내용과 요약(있는 경우)을 합쳐서 조언 생성 요청
        input_text = f"원본 내용:\n{content}\n"
        if summary:
            input_text += f"\n요약:\n{summary}\n"
        
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": """당신은 30년 경력의 디지털 마케팅 전문가로서, 광고 플랫폼의 공지사항을 보고 현장 마케터들에게 실용적인 조언을 제공합니다.

다음 공지사항에 대해 30년 경력의 전문가로서 조언해주세요:
1. 이 변화/업데이트가 실제 마케팅 성과에 미칠 영향
2. 이 변화를 활용할 수 있는 구체적인 전략적 조언
3. 가능한 위험 요소와 대응 방안
4. 장기적 관점에서의 인사이트

조언은 평이한 말투보다는 경험에서 우러나오는 통찰력 있는 표현으로 작성해주세요.
전체 조언은 300자 이내로 제한하고, 실무자가 바로 활용할 수 있는 구체적인 내용으로 작성해주세요.
모든 문장이 완전하게 끝나도록 하고, 중간에 잘리지 않게 해주세요."""},
                {"role": "user", "content": input_text}
            ],
            max_tokens=300,
            temperature=0.7
        )
        
        advice = response.choices[0].message.content.strip()
        
        # 문장이 중간에 끊기지 않았는지 확인
        if advice and not advice.endswith(('.', '!', '?', '다.', '요.', '임.', '됨.')):
            last_period_index = max(
                advice.rfind('.'), 
                advice.rfind('다.'), 
                advice.rfind('요.'),
                advice.rfind('임.'),
                advice.rfind('됨.'),
                advice.rfind('!'),
                advice.rfind('?')
            )
            if last_period_index > 0:
                advice = advice[:last_period_index+1]
        
        return advice
    
    except Exception as e:
        print(f"조언 생성 중 오류 발생: {str(e)}")
        return "조언 생성 중 오류가 발생했습니다."

def setup_advice_column(sheet):
    """'30년차' 열 설정"""
    try:
        # 헤더 가져오기
        headers = sheet.row_values(1)
        
        # G열에 '30년차' 헤더가 없는 경우 추가
        if len(headers) < 7 or headers[6] != '30년차':
            print(f"{sheet.title} 시트에 '30년차' 열 추가 중...")
            sheet.update_cell(1, 7, '30년차')
            print(f"{sheet.title} 시트에 '30년차' 열 추가 완료")
        else:
            print(f"{sheet.title} 시트에는 이미 '30년차' 열이 있습니다.")
    
    except Exception as e:
        print(f"{sheet.title} 시트의 '30년차' 열 설정 중 오류 발생: {str(e)}")

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
    
    # 요약 및 조언이 필요한 행 찾기
    rows_to_update = []
    
    # Boss_pdf와 Boss_pdf2는 E열에 내용, F열에 요약(중요여부)
    if sheet.title in ['Boss_pdf', 'Boss_pdf2']:
        for i, row in enumerate(rows, start=2):  # 시트 인덱스는 1부터 시작, 헤더가 1행이므로 2부터
            if len(row) >= 5 and row[4] and (len(row) < 6 or not row[5]):  # 내용은 있고 요약은 없는 경우
                rows_to_update.append((i, row))
    else:
        # 기존 로직 - 다른 시트들은 이전 컬럼 구조 사용
        for i, row in enumerate(rows, start=2):
            if len(row) >= 6 and row[4] and not row[5]:  # 내용은 있고 요약은 없는 경우
                rows_to_update.append((i, row))
    
    print(f"요약이 필요한 항목 수: {len(rows_to_update)}")
    
    # 요약 처리
    updated_count = 0
    for row_idx, row in rows_to_update:
        try:
            content = row[4]  # E열이 내용 열
            print(f"'{row[0]}' 요약 중...")
            
            # 시트 이름을 함수에 전달하여 적절한 요약 형식 선택
            summary = summarize_text(content, sheet_name=sheet.title)
            
            # 요약 결과 업데이트
            sheet.update_cell(row_idx, 6, summary)  # 6번째 열(F열)이 요약 열
            
            # 30년차 디지털 마케터 조언 생성 (해당 시트만)
            if sheet.title in ADVICE_SHEET_NAMES:
                print(f"'{row[0]}' 조언 생성 중...")
                advice = generate_expert_advice(content, summary)
                sheet.update_cell(row_idx, 7, advice)  # 7번째 열(G열)이 30년차 조언 열
                print(f"'{row[0]}' 조언 생성 완료")
            
            updated_count += 1
            print(f"'{row[0]}' 요약 완료")
            
            # API 호출 제한 방지를 위한 대기
            time.sleep(1)
            
        except Exception as e:
            print(f"행 {row_idx} 처리 중 오류 발생: {str(e)}")
    
    return updated_count

def generate_missing_advice(sheet):
    """요약은 있지만 조언이 없는 항목에 대해 조언 생성"""
    if sheet.title not in ADVICE_SHEET_NAMES:
        return 0
    
    print(f"{sheet.title} 시트의 누락된 조언 처리 중...")
    
    # 모든 데이터 가져오기
    data = sheet.get_all_values()
    
    if len(data) <= 1:  # 헤더만 있는 경우
        return 0
    
    # 헤더 제외한 데이터
    rows = data[1:]
    
    # 조언이 필요한 행 찾기 (요약은 있고 조언은 없는 경우)
    rows_to_update = []
    for i, row in enumerate(rows, start=2):
        if len(row) >= 6 and row[5] and (len(row) < 7 or not row[6]):
            rows_to_update.append((i, row))
    
    print(f"조언이 필요한 항목 수: {len(rows_to_update)}")
    
    # 조언 처리
    updated_count = 0
    for row_idx, row in rows_to_update:
        try:
            content = row[4]  # E열이 내용 열
            summary = row[5]  # F열이 요약 열
            print(f"'{row[0]}' 조언 생성 중...")
            
            # 30년차 디지털 마케터 조언 생성
            advice = generate_expert_advice(content, summary)
            sheet.update_cell(row_idx, 7, advice)  # 7번째 열(G열)이 30년차 조언 열
            
            updated_count += 1
            print(f"'{row[0]}' 조언 생성 완료")
            
            # API 호출 제한 방지를 위한 대기
            time.sleep(1)
            
        except Exception as e:
            print(f"행 {row_idx} 조언 생성 중 오류 발생: {str(e)}")
    
    return updated_count

def run_summary():
    """모든 시트에 대해 요약 처리 실행"""
    try:
        print("요약 프로그램 시작:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
        # Google Sheets 연결
        spreadsheet = setup_google_sheets()
        
        total_updated = 0
        total_advice = 0
        
        # 각 시트 처리
        for sheet_name in SHEET_NAMES:
            try:
                sheet = spreadsheet.worksheet(sheet_name)
                
                # 30년차 조언이 필요한 시트에 '30년차' 열 설정
                if sheet_name in ADVICE_SHEET_NAMES:
                    setup_advice_column(sheet)
                
                # 요약 및 조언 처리
                updated = process_sheet(sheet)
                total_updated += updated
                
                # 누락된 조언 처리 (요약은 있지만 조언이 없는 항목)
                if sheet_name in ADVICE_SHEET_NAMES:
                    advice_updated = generate_missing_advice(sheet)
                    total_advice += advice_updated
                    print(f"{sheet_name} 시트 조언 처리 완료: {advice_updated}개 항목에 조언 추가됨")
                
                print(f"{sheet_name} 시트 처리 완료: {updated}개 항목 요약됨")
            except Exception as e:
                print(f"{sheet_name} 시트 처리 중 오류 발생: {str(e)}")
        
        print(f"총 {total_updated}개 항목 요약 완료, {total_advice}개 항목에 조언 추가 완료")
        
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
