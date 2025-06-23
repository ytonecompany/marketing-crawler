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
# 추가 의견이 필요한 시트 목록 - 빈 배열에서 필요한 시트로 변경
ADDITIONAL_ADVICE_SHEET_NAMES = ['Naver_Ads', 'Google_Ads', 'Meta_Ads']

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
    
    max_retries = 3
    retry_delay = 2  # 초 단위
    
    for retry in range(max_retries):
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
            error_msg = f"조언 생성 중 오류 발생 (시도 {retry+1}/{max_retries}): {str(e)}"
            print(error_msg)
            logging.error(error_msg)
            
            if retry < max_retries - 1:
                # 마지막 시도가 아니면 대기 후 재시도
                print(f"{retry_delay}초 후 재시도...")
                time.sleep(retry_delay)
                # 다음 재시도에서 대기 시간 증가 (지수 백오프)
                retry_delay *= 2
            else:
                # 모든 재시도 실패 시
                print("최대 재시도 횟수 초과, 조언 생성 실패")
                return "조언 생성 중 오류가 발생했습니다."
    
    # 이 코드는 실행되지 않지만, 구문 오류 방지를 위해 추가
    return "조언 생성 중 오류가 발생했습니다."

def generate_importance_and_actions(content, summary=None):
    """OpenAI API를 사용하여 변경 개요의 중요성과 실무 적용 제언 생성"""
    if not content:
        return "", ""
    
    max_retries = 3
    retry_delay = 2  # 초 단위
    
    for retry in range(max_retries):
        try:
            client = OpenAI(api_key=api_key)
            
            # 원본 내용과 요약(있는 경우)을 합쳐서 조언 생성 요청
            input_text = f"원본 내용:\n{content}\n"
            if summary:
                input_text += f"\n요약:\n{summary}\n"
            
            # 변경 개요의 중요성 생성
            importance_response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": """당신은 디지털 마케팅과 광고 플랫폼에 대한 전문가입니다. 
광고 플랫폼의 변경사항이나 공지사항을 보고 해당 변경이 왜 중요한지 명확하게 설명해주세요.

다음 내용에 대해 '본 공지(변경사항)이 왜 중요한가?'라는 질문에 답변해주세요:
- 이 변경이 마케터/광고주에게 미치는 영향을 구체적으로 설명
- 이 변경의 잠재적인 긍정적/부정적 측면을 실무적 관점에서 설명
- 무시할 경우 발생할 수 있는 구체적인 결과나 리스크

답변 작성 시 주의사항:
1. 절대로 날짜나 시간 표현을 포함하지 마세요
2. 대신 변경사항의 본질적 중요성과 실무적 영향에 집중하세요
3. 구체적이고 실용적인 관점에서 설명하세요
4. "~해야 합니다", "~이 중요합니다"와 같은 일반적인 표현 대신 구체적인 영향과 결과를 설명하세요

답변은 명확하고 간결하게 작성하되, 실무자가 이해하기 쉽도록 작성해주세요.
전체 내용은 200자 이내로 제한하고, 모든 문장이 완전하게 끝나도록 해주세요."""},
                    {"role": "user", "content": input_text}
                ],
                max_tokens=200,
                temperature=0.5
            )
            
            # 실무 적용 제언 생성
            actions_response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": """당신은 디지털 마케팅과 광고 플랫폼에 대한 전문가입니다.
광고 플랫폼의 변경사항이나 공지사항을 보고 마케터가 지금 당장 취해야 할 행동을 제안해주세요.

다음 내용에 대해 '마케터가 지금 해야 할 일'을 구체적으로 제안해주세요:
- 실무에 바로 적용할 수 있는 명확한 행동 단계
- 우선순위와 함께 제시된 구체적인 액션 아이템
- 실행 가능한 체크리스트 형태의 조언

답변은 행동 지향적으로 작성하고, 실무자가 바로 실행할 수 있도록 구체적으로 작성해주세요.
전체 내용은 200자 이내로 제한하고, 모든 문장이 완전하게 끝나도록 해주세요.

중요: 답변에 날짜(예: 5월 22일, 2024년 등)를 포함하지 마세요. 시간적 표현 대신 즉시 실행 가능한 행동에 집중해주세요."""},
                    {"role": "user", "content": input_text}
                ],
                max_tokens=200,
                temperature=0.5
            )
            
            importance = importance_response.choices[0].message.content.strip()
            actions = actions_response.choices[0].message.content.strip()
            
            # 문장이 중간에 끊기지 않았는지 확인
            for text in [importance, actions]:
                if text and not text.endswith(('.', '!', '?', '다.', '요.', '임.', '됨.')):
                    last_period_index = max(
                        text.rfind('.'), 
                        text.rfind('다.'), 
                        text.rfind('요.'),
                        text.rfind('임.'),
                        text.rfind('됨.'),
                        text.rfind('!'),
                        text.rfind('?')
                    )
                    if last_period_index > 0:
                        if text == importance:
                            importance = text[:last_period_index+1]
                        else:
                            actions = text[:last_period_index+1]
            
            # 성공적으로 생성된 경우
            return importance, actions
            
        except Exception as e:
            error_msg = f"추가 의견 생성 중 오류 발생 (시도 {retry+1}/{max_retries}): {str(e)}"
            print(error_msg)
            logging.error(error_msg)
            
            if retry < max_retries - 1:
                # 마지막 시도가 아니면 대기 후 재시도
                print(f"{retry_delay}초 후 재시도...")
                time.sleep(retry_delay)
                # 다음 재시도에서 대기 시간 증가 (지수 백오프)
                retry_delay *= 2
            else:
                # 모든 재시도 실패 시
                print("최대 재시도 횟수 초과, 의견 생성 실패")
                return "의견 생성 중 오류가 발생했습니다.", "의견 생성 중 오류가 발생했습니다."
    
    # 이 코드는 실행되지 않지만, 구문 오류 방지를 위해 추가
    return "의견 생성 중 오류가 발생했습니다.", "의견 생성 중 오류가 발생했습니다."

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

def setup_additional_columns(sheet):
    """G열과 H열 사이에 신규 열 2개 추가"""
    try:
        print(f"{sheet.title} 시트에 신규 열 설정 중...")
        logging.info(f"{sheet.title} 시트에 신규 열 설정 시작")
        
        # 헤더 가져오기
        headers = sheet.row_values(1)
        print(f"현재 {sheet.title} 시트 헤더: {headers}")
        
        # 새로운 열 이름
        new_columns = ["변경 개요의 중요성", "실무 적용 제언"]
        
        if len(headers) < 1:
            # 헤더가 없는 경우 기본 헤더 추가
            print(f"{sheet.title} 시트에 헤더가 없습니다. 기본 헤더 추가...")
            default_headers = ['제목', '구분', '작성일', '링크', '출처', '내용', '요약', '30년차', '변경 개요의 중요성', '실무 적용 제언']
            sheet.update('A1:J1', [default_headers])
            logging.info(f"{sheet.title} 시트에 기본 헤더 추가 완료")
            return
        
        # 필요한 열이 없는지 확인
        missing_columns = []
        
        # "30년차" 열이 없는지 확인
        if len(headers) < 7 or headers[6] != '30년차':
            missing_columns.append('30년차')
            
        # "변경 개요의 중요성" 열이 없는지 확인
        if len(headers) < 8 or headers[7] != "변경 개요의 중요성":
            missing_columns.append('변경 개요의 중요성')
            
        # "실무 적용 제언" 열이 없는지 확인
        if len(headers) < 9 or headers[8] != "실무 적용 제언":
            missing_columns.append('실무 적용 제언')
        
        if not missing_columns:
            print(f"{sheet.title} 시트에는 이미 필요한 모든 열이 있습니다.")
            return
        
        print(f"{sheet.title} 시트에 누락된 열: {missing_columns}")
        
        # 모든 데이터 가져오기
        all_data = sheet.get_all_values()
        
        # '30년차' 열 설정 (없는 경우)
        # 7번 열이 '30년차'가 아니면 '30년차' 열 추가
        if '30년차' in missing_columns:
            print(f"{sheet.title} 시트에 '30년차' 열 추가 중...")
            # 변경된 모든 행
            updated_rows = []
            
            # 첫 번째 행(헤더) 처리
            if len(all_data) > 0:
                header_row = all_data[0]
                while len(header_row) < 7:  # 최소 7열 (G열) 확보
                    header_row.append("")
                # 7번째 열(G열)을 '30년차'로 설정
                header_row[6] = '30년차'  
                updated_rows.append(header_row)
                
                # 나머지 데이터 행 처리
                for i in range(1, len(all_data)):
                    data_row = all_data[i]
                    while len(data_row) < 7:  # 최소 7열 확보
                        data_row.append("")
                    updated_rows.append(data_row)
                
                # 스프레드시트 업데이트
                sheet.update(f'A1:{chr(65+len(header_row)-1)}{len(updated_rows)}', updated_rows)
                print(f"{sheet.title} 시트에 '30년차' 열 추가 완료")
                
                # 업데이트된 헤더 다시 가져오기
                headers = sheet.row_values(1)
                all_data = updated_rows
        
        # "변경 개요의 중요성"과 "실무 적용 제언" 열 확인 및 추가
        if "변경 개요의 중요성" in missing_columns or "실무 적용 제언" in missing_columns:
            print(f"{sheet.title} 시트에 추가 의견 열 추가 중...")
            
            # 모든 행 가져오기 (이미 업데이트된 경우 이전에 가져온 데이터 사용)
            all_rows = all_data
            updated_rows = []
            
            for i, row in enumerate(all_rows):
                new_row = row.copy()
                
                # 필요한 열이 확보되었는지 확인
                while len(new_row) < 9:  # 최소 9열 (I열) 확보
                    new_row.append("")
                
                # 첫 번째 행(헤더) 처리
                if i == 0:
                    # H열(8번째)에 "변경 개요의 중요성" 설정
                    new_row[7] = "변경 개요의 중요성"
                    # I열(9번째)에 "실무 적용 제언" 설정
                    new_row[8] = "실무 적용 제언"
                
                updated_rows.append(new_row)
            
            # 스프레드시트 업데이트
            max_col = chr(65 + max(len(row) for row in updated_rows) - 1)
            sheet.update(f'A1:{max_col}{len(updated_rows)}', updated_rows)
            print(f"{sheet.title} 시트에 추가 의견 열 추가 완료")
            
        # 최종 헤더 확인
        final_headers = sheet.row_values(1)
        print(f"최종 {sheet.title} 시트 헤더: {final_headers}")
        
        # 모든 열이 올바르게 설정되었는지 확인
        if (len(final_headers) >= 9 and 
            final_headers[6] == '30년차' and 
            final_headers[7] == '변경 개요의 중요성' and 
            final_headers[8] == '실무 적용 제언'):
            print(f"{sheet.title} 시트 열 설정 완료: 모든 필요한 열이 정상적으로 설정되었습니다.")
            logging.info(f"{sheet.title} 시트 열 설정 완료: 모든 필요한 열이 정상적으로 설정되었습니다.")
        else:
            error_msg = f"{sheet.title} 시트 열 설정 완료 후 검증 실패: {final_headers}"
            print(error_msg)
            logging.warning(error_msg)
            
    except Exception as e:
        error_msg = f"{sheet.title} 시트의 신규 열 추가 중 오류 발생: {str(e)}"
        print(error_msg)
        logging.error(error_msg)

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

def generate_missing_additional_advice(sheet):
    """추가 의견이 필요한 항목에 대해 변경 개요의 중요성과 실무 적용 제언 생성"""
    if sheet.title not in ADDITIONAL_ADVICE_SHEET_NAMES:
        return 0
    
    print(f"{sheet.title} 시트의 누락된 추가 의견 처리 중...")
    
    # 모든 데이터 가져오기
    data = sheet.get_all_values()
    
    if len(data) <= 1:  # 헤더만 있는 경우
        return 0
    
    # 헤더 제외한 데이터
    rows = data[1:]
    
    # 추가 의견이 필요한 행 찾기 (내용과 요약은 있지만 추가 의견이 없는 경우)
    rows_to_update = []
    for i, row in enumerate(rows, start=2):
        if len(row) >= 6 and row[4] and row[5]:  # 내용과 요약이 있고
            # H열 (변경 개요의 중요성)만 확인 - I열은 비활성화
            need_importance = len(row) < 8 or not row[7]  # H열 (변경 개요의 중요성)이 없거나 비어있음
            need_actions = False  # I열 (실무 적용 제언) 비활성화
            
            if need_importance:  # 중요성만 확인
                rows_to_update.append((i, row, need_importance, need_actions))
    
    print(f"추가 의견이 필요한 항목 수: {len(rows_to_update)}")
    
    # 추가 의견 처리
    updated_count = 0
    for row_idx, row, need_importance, need_actions in rows_to_update:
        try:
            content = row[4]  # E열이 내용 열
            summary = row[5]  # F열이 요약 열
            print(f"'{row[0]}' 추가 의견 생성 중... (중요성: {need_importance}, 제언: {need_actions})")
            
            if need_importance:
                # 중요성만 생성 (I열 실무 적용 제언은 비활성화)
                importance, _ = generate_importance_and_actions(content, summary)
                sheet.update_cell(row_idx, 8, importance)  # 8번째 열(H열)이 변경 개요의 중요성 열
                print(f"'{row[0]}' 중요성만 생성 완료 (I열 제언은 비활성화됨)")
            # elif need_actions 부분은 주석 처리 - I열 업데이트 비활성화
            # elif need_actions:
            #     # 제언만 필요한 경우
            #     _, actions = generate_importance_and_actions(content, summary)
            #     sheet.update_cell(row_idx, 9, actions)     # 9번째 열(I열)이 실무 적용 제언 열
            
            updated_count += 1
            print(f"'{row[0]}' 추가 의견 생성 완료")
            
            # API 호출 제한 방지를 위한 대기
            time.sleep(1)
            
        except Exception as e:
            error_msg = f"행 {row_idx} 추가 의견 생성 중 오류 발생: {str(e)}"
            print(error_msg)
            logging.error(error_msg)
            
            # 오류 발생 시에도 개별 항목 재시도
            try:
                if need_importance:
                    print(f"'{row[0]}' 중요성 항목 개별 재시도 중...")
                    client = OpenAI(api_key=api_key)
                    input_text = f"원본 내용:\n{content}\n"
                    if summary:
                        input_text += f"\n요약:\n{summary}\n"
                    
                    importance_response = client.chat.completions.create(
                        model="gpt-3.5-turbo",
                        messages=[
                            {"role": "system", "content": """당신은 디지털 마케팅과 광고 플랫폼에 대한 전문가입니다. 
광고 플랫폼의 변경사항이나 공지사항을 보고 해당 변경이 왜 중요한지 명확하게 설명해주세요.

다음 내용에 대해 '본 공지(변경사항)이 왜 중요한가?'라는 질문에 답변해주세요:
- 이 변경이 마케터/광고주에게 미치는 영향을 구체적으로 설명
- 이 변경의 잠재적인 긍정적/부정적 측면을 실무적 관점에서 설명
- 무시할 경우 발생할 수 있는 구체적인 결과나 리스크

답변은 명확하고 간결하게 작성하되, 실무자가 이해하기 쉽도록 작성해주세요.
전체 내용은 200자 이내로 제한하고, 모든 문장이 완전하게 끝나도록 해주세요."""},
                            {"role": "user", "content": input_text}
                        ],
                        max_tokens=200,
                        temperature=0.5
                    )
                    importance = importance_response.choices[0].message.content.strip()
                    sheet.update_cell(row_idx, 8, importance)
                    print(f"'{row[0]}' 중요성 생성 완료 (재시도)")
                    time.sleep(1)
                
                # I열 실무 적용 제언 생성 비활성화
                # if need_actions:
                #     print(f"'{row[0]}' 제언 항목 개별 재시도 중...")
                #     client = OpenAI(api_key=api_key)
                #     input_text = f"원본 내용:\n{content}\n"
                #     if summary:
                #         input_text += f"\n요약:\n{summary}\n"
                #     
                #     actions_response = client.chat.completions.create(
                #         model="gpt-3.5-turbo",
                #         messages=[
                #             {"role": "system", "content": """당신은 디지털 마케팅과 광고 플랫폼에 대한 전문가입니다.
                # 광고 플랫폼의 변경사항이나 공지사항을 보고 마케터가 지금 당장 취해야 할 행동을 제안해주세요.
                # 
                # 다음 내용에 대해 '마케터가 지금 해야 할 일'을 구체적으로 제안해주세요:
                # - 실무에 바로 적용할 수 있는 명확한 행동 단계
                # - 우선순위와 함께 제시된 구체적인 액션 아이템
                # - 실행 가능한 체크리스트 형태의 조언
                # 
                # 답변은 행동 지향적으로 작성하고, 실무자가 바로 실행할 수 있도록 구체적으로 작성해주세요.
                # 전체 내용은 200자 이내로 제한하고, 모든 문장이 완전하게 끝나도록 해주세요.
                # 
                # 중요: 답변에 날짜(예: 5월 22일, 2024년 등)를 포함하지 마세요. 시간적 표현 대신 즉시 실행 가능한 행동에 집중해주세요."""},
                #             {"role": "user", "content": input_text}
                #         ],
                #         max_tokens=200,
                #         temperature=0.5
                #     )
                #     actions = actions_response.choices[0].message.content.strip()
                #     sheet.update_cell(row_idx, 9, actions)
                #     print(f"'{row[0]}' 제언 생성 완료 (재시도)")
                #     time.sleep(1)
            
            except Exception as retry_error:
                retry_error_msg = f"행 {row_idx} 추가 의견 개별 재시도 중 오류 발생: {str(retry_error)}"
                print(retry_error_msg)
                logging.error(retry_error_msg)
    
    return updated_count

def translate_to_korean(text):
    """OpenAI API를 사용하여 텍스트를 한글로 번역"""
    if not text:
        return ""
    
    max_retries = 3
    retry_delay = 2  # 초 단위
    
    for retry in range(max_retries):
        try:
            client = OpenAI(api_key=api_key)
            
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "당신은 전문 번역가입니다. 주어진 텍스트를 한국어로 번역해주세요. 번역은 자연스러운 한국어로 하되, 전문 용어는 정확하게 유지해주세요."},
                    {"role": "user", "content": f"다음 텍스트를 한국어로 번역해주세요:\n\n{text}"}
                ],
                max_tokens=1000,
                temperature=0.3
            )
            
            translation = response.choices[0].message.content.strip()
            return translation
            
        except Exception as e:
            error_msg = f"번역 중 오류 발생 (시도 {retry+1}/{max_retries}): {str(e)}"
            print(error_msg)
            logging.error(error_msg)
            
            if retry < max_retries - 1:
                print(f"{retry_delay}초 후 재시도...")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                print("최대 재시도 횟수 초과, 번역 실패")
                return "번역 중 오류가 발생했습니다."
    
    return "번역 중 오류가 발생했습니다."

def process_translations(sheet):
    """시트의 E열 내용을 번역하여 I열에 저장"""
    if sheet.title not in SHEET_NAMES:
        return 0
    
    print(f"{sheet.title} 시트의 번역 처리 중...")
    
    # 모든 데이터 가져오기
    data = sheet.get_all_values()
    
    if len(data) <= 1:  # 헤더만 있는 경우
        return 0
    
    # 헤더 제외한 데이터
    rows = data[1:]
    
    # 번역이 필요한 행 찾기 (E열에 내용이 있고 I열이 비어있거나 "번역 중 오류가 발생했습니다." 인 경우)
    rows_to_update = []
    for i, row in enumerate(rows, start=2):
        # E열은 있고
        if len(row) >= 5 and row[4]:
            # I열이 없거나 비어있거나 에러 메시지인 경우에만 번역
            if (len(row) < 9 or 
                not row[8] or 
                row[8].strip() == "번역 중 오류가 발생했습니다."):
                rows_to_update.append((i, row))
    
    print(f"번역이 필요한 항목 수: {len(rows_to_update)}")
    
    # 번역 처리
    updated_count = 0
    for row_idx, row in rows_to_update:
        try:
            content = row[4]  # E열이 내용 열
            print(f"'{row[0]}' 번역 중...")
            
            # 번역 수행
            translation = translate_to_korean(content)
            
            # 번역 결과가 에러가 아닌 경우에만 업데이트
            if translation != "번역 중 오류가 발생했습니다.":
                sheet.update_cell(row_idx, 9, translation)  # 9번째 열(I열)이 번역 열
                updated_count += 1
                print(f"'{row[0]}' 번역 완료")
            else:
                print(f"'{row[0]}' 번역 실패")
            
            # API 호출 제한 방지를 위한 대기
            time.sleep(1)
            
        except Exception as e:
            error_msg = f"행 {row_idx} 번역 중 오류 발생: {str(e)}"
            print(error_msg)
            logging.error(error_msg)
    
    return updated_count

def run_summary():
    """모든 시트에 대해 요약 처리 실행"""
    try:
        start_time = datetime.now()
        print("요약 프로그램 시작:", start_time.strftime("%Y-%m-%d %H:%M:%S"))
        logging.info("요약 프로그램 시작: %s", start_time.strftime("%Y-%m-%d %H:%M:%S"))
        
        # Google Sheets 연결
        spreadsheet = setup_google_sheets()
        
        total_updated = 0
        total_advice = 0
        total_additional_advice = 0
        total_translations = 0
        
        # 시트별 처리 통계
        sheet_stats = {}
        
        # 각 시트 처리
        for sheet_name in SHEET_NAMES:
            try:
                print(f"\n==== {sheet_name} 시트 처리 시작 ====")
                logging.info(f"{sheet_name} 시트 처리 시작")
                
                sheet = spreadsheet.worksheet(sheet_name)
                
                # 시트별 통계 초기화
                sheet_stats[sheet_name] = {
                    'updated': 0,
                    'advice_updated': 0,
                    'additional_advice_updated': 0,
                    'translations_updated': 0,
                    'errors': 0
                }
                
                # 30년차 조언이 필요한 시트에 '30년차' 열 설정
                if sheet_name in ADVICE_SHEET_NAMES:
                    setup_advice_column(sheet)
                
                # 추가 의견이 필요한 시트에 신규 열 설정
                if sheet_name in ADDITIONAL_ADVICE_SHEET_NAMES:
                    setup_additional_columns(sheet)
                
                # 요약 및 조언 처리
                updated = process_sheet(sheet)
                total_updated += updated
                sheet_stats[sheet_name]['updated'] = updated
                
                # 누락된 조언 처리 (요약은 있지만 조언이 없는 항목)
                if sheet_name in ADVICE_SHEET_NAMES:
                    advice_updated = generate_missing_advice(sheet)
                    total_advice += advice_updated
                    sheet_stats[sheet_name]['advice_updated'] = advice_updated
                    print(f"{sheet_name} 시트 조언 처리 완료: {advice_updated}개 항목에 조언 추가됨")
                    logging.info(f"{sheet_name} 시트 조언 처리 완료: {advice_updated}개 항목에 조언 추가됨")
                
                # 누락된 추가 의견 처리
                if sheet_name in ADDITIONAL_ADVICE_SHEET_NAMES:
                    additional_advice_updated = generate_missing_additional_advice(sheet)
                    total_additional_advice += additional_advice_updated
                    sheet_stats[sheet_name]['additional_advice_updated'] = additional_advice_updated
                    print(f"{sheet_name} 시트 추가 의견 처리 완료: {additional_advice_updated}개 항목에 추가 의견 생성됨")
                    logging.info(f"{sheet_name} 시트 추가 의견 처리 완료: {additional_advice_updated}개 항목에 추가 의견 생성됨")
                
                # 번역 처리
                translations_updated = process_translations(sheet)
                total_translations += translations_updated
                sheet_stats[sheet_name]['translations_updated'] = translations_updated
                print(f"{sheet_name} 시트 번역 처리 완료: {translations_updated}개 항목 번역됨")
                logging.info(f"{sheet_name} 시트 번역 처리 완료: {translations_updated}개 항목 번역됨")
                
                # 누락된 열 확인 (중요성만 - I열 제언은 비활성화)
                if sheet_name in ADDITIONAL_ADVICE_SHEET_NAMES:
                    all_data = sheet.get_all_values()
                    if len(all_data) > 1:  # 헤더 제외
                        rows = all_data[1:]
                        missing_importance = 0
                        missing_translations = 0
                        
                        for row in rows:
                            if len(row) >= 5 and row[4]:  # 내용이 있을 때
                                if len(row) < 8 or not row[7]:  # 중요성 누락
                                    missing_importance += 1
                                if len(row) < 9 or not row[8]:  # 번역 누락
                                    missing_translations += 1
                        
                        if missing_importance > 0 or missing_translations > 0:
                            print(f"{sheet_name} 시트 완료 후 확인: 중요성 누락 {missing_importance}개, 번역 누락 {missing_translations}개")
                            logging.warning(f"{sheet_name} 시트 완료 후 확인: 중요성 누락 {missing_importance}개, 번역 누락 {missing_translations}개")
                
                print(f"==== {sheet_name} 시트 처리 완료: {updated}개 항목 요약됨 ====\n")
                logging.info(f"{sheet_name} 시트 처리 완료: {updated}개 항목 요약됨")
                
            except Exception as e:
                error_msg = f"{sheet_name} 시트 처리 중 오류 발생: {str(e)}"
                print(error_msg)
                logging.error(error_msg)
                if sheet_name in sheet_stats:
                    sheet_stats[sheet_name]['errors'] += 1
        
        # 종합 결과 출력
        print("\n========== 종합 처리 결과 ==========")
        for sheet_name, stats in sheet_stats.items():
            print(f"{sheet_name} 시트:")
            print(f"  - 요약 처리: {stats['updated']}개")
            if sheet_name in ADVICE_SHEET_NAMES:
                print(f"  - 조언 생성: {stats['advice_updated']}개")
            if sheet_name in ADDITIONAL_ADVICE_SHEET_NAMES:
                print(f"  - 추가 의견 생성: {stats['additional_advice_updated']}개")
            print(f"  - 번역 처리: {stats['translations_updated']}개")
            print(f"  - 오류 발생: {stats['errors']}개")
            print("")
        
        print(f"총 {total_updated}개 항목 요약 완료, {total_advice}개 항목에 조언 추가 완료, {total_additional_advice}개 항목에 추가 의견 생성 완료, {total_translations}개 항목 번역 완료")
        logging.info(f"총 {total_updated}개 항목 요약 완료, {total_advice}개 항목에 조언 추가 완료, {total_additional_advice}개 항목에 추가 의견 생성 완료, {total_translations}개 항목 번역 완료")
        
        end_time = datetime.now()
        duration = end_time - start_time
        print(f"총 소요 시간: {duration}")
        logging.info(f"총 소요 시간: {duration}")
        
    except Exception as e:
        error_msg = f"요약 프로그램 실행 중 오류 발생: {str(e)}"
        print(error_msg)
        logging.error(error_msg)
        raise
    finally:
        print("요약 프로그램 종료:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        logging.info("요약 프로그램 종료: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

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
