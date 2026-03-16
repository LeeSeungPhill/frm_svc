# 코드 심층 분석 및 전략 검토 보고서

> 분석일: 2026-03-16
> 대상: frm_svc 프로젝트 전체 (34개 파일)

---

## 목차
1. [치명적 버그 (CRITICAL BUGS)](#1-치명적-버그-critical-bugs)
2. [보안 취약점 (SECURITY)](#2-보안-취약점-security)
3. [리소스 누수 (RESOURCE LEAKS)](#3-리소스-누수-resource-leaks)
4. [사용하지 않는 코드 (DEAD CODE)](#4-사용하지-않는-코드-dead-code)
5. [중복 코드 패턴 (DUPLICATION)](#5-중복-코드-패턴-duplication)
6. [전략 효과성 분석 (STRATEGY REVIEW)](#6-전략-효과성-분석-strategy-review)
7. [권장 개선사항 (RECOMMENDATIONS)](#7-권장-개선사항-recommendations)

---

## 1. 치명적 버그 (CRITICAL BUGS)

### BUG-01: `upbitTradeMng.py` - 루프 변수 덮어쓰기로 첫 번째 마켓만 처리
- **위치:** Line 46 (`close_order`), Line 219 (`open_order`)
- **증상:** `for m in market_list` 루프에서 `m = hashlib.sha512()`로 루프 변수를 덮어씌움
- **영향:** 첫 번째 마켓 처리 후 `m['market']`에서 `AttributeError` 발생하여 나머지 마켓 처리 불가
- **심각도:** 🔴 CRITICAL

### BUG-02: `upbitOrder(이더리움sell).py` - 매도 수량이 항상 0
- **위치:** Line 157, 205
- **증상:** `sell_target_amount = 0` 초기화 후 갱신하지 않음 (실제 계산은 `sell_target_amt`에 저장)
- **영향:** `volume = sell_target_amount / 2 = 0` → 매도 수량이 항상 0
- **심각도:** 🔴 CRITICAL (변수명 불일치)

### BUG-03: `upbitOpenOrder.py` - 존재하지 않는 인덱스 접근
- **위치:** Line 186
- **증상:** SQL SELECT가 6개 컬럼만 조회하는데 `chk_ord[9]`(10번째)에 접근
- **영향:** `IndexError` 발생
- **심각도:** 🔴 CRITICAL

### BUG-04: `upbitOpenOrder.py` - None에 len() 호출
- **위치:** Line 251, 257
- **증상:** `fetchone()`이 None 반환 시 `len(None)` → `TypeError`
- **심각도:** 🔴 CRITICAL

### BUG-05: `upbitOrder.py` - 마켓 불일치
- **위치:** Line 97, 120
- **증상:** 호가 조회는 `KRW-SOL`, 실제 주문은 `KRW-BTC`로 실행
- **영향:** 솔라나 호가 기준으로 비트코인 주문 → 의도치 않은 가격에 매수
- **심각도:** 🔴 CRITICAL

### BUG-06: 다수 주문 파일 - 잔고 하드코딩으로 실제 잔고 무시
- **해당 파일:** `upbitOrder(비트코인buy).py`, `upbitOrder(솔라나buy).py`, `upbitOrder(스택스buy).py`, `upbitOrder(이더리움buy).py`
- **증상:** API로 잔고 조회 후 결과를 무시하고 `balance = 2000000` 하드코딩 사용
- **영향:** 실제 잔고와 무관하게 주문 실행 → 잔고 부족 시 에러 또는 과도한 주문
- **심각도:** 🟡 HIGH

### BUG-07: 무한 루프 탈출 불가
- **해당 파일:** `upbitOrder(비트코인buy).py` (162행), `upbitOrder(전체잔고-비트코인-호가매수).py` (164행), `upbitOrder(정액-비트코인-지정가매수).py`
- **증상:** `else: break` 분기가 주석 처리되어 잔고 부족 시에도 루프 탈출 불가
- **영향:** 15초마다 불필요한 API 호출이 영원히 반복
- **심각도:** 🟡 HIGH

### BUG-08: `upbitTrendLine1.py` - 더미 UUID로 주문 조회
- **위치:** Line 393, 565
- **증상:** 실제 주문 호출이 주석 처리되고 `order_response = {"uuid": "019c715a-..."}` 하드코딩
- **영향:** 존재하지 않는 UUID로 `get_order()` 호출 → 에러 발생
- **심각도:** 🟡 HIGH

### BUG-09: `upbitTrendLine1.py` - ZeroDivisionError 가능
- **위치:** Line 364
- **증상:** `buy_vol = loss_amt / (Decimal(buy_price) - trade_mng['cut_price'])` — `buy_price == cut_price`이면 0으로 나눔
- **심각도:** 🟡 HIGH

### BUG-10: `upbitStocastics.py` - 매수/매도 신호 뒤바뀜
- **위치:** Line 53-66
- **증상:** `upward_crossover` 함수가 실제로는 하향 교차(데드크로스) 탐지, `downward_crossover`가 상향 교차(골든크로스) 탐지
- **영향:** 변수명과 실제 동작이 정반대 → 유지보수 시 논리 오류 유발
- **심각도:** 🟠 MEDIUM (결과적으로 올바른 신호이지만 네이밍 혼란)

---

## 2. 보안 취약점 (SECURITY)

### SEC-01: DB 비밀번호 소스코드 하드코딩
- **해당 파일:** `upbitOpenOrder.py`, `upbitBalanceInfo.py`, `bithumbTradeMng.py`, `bithumbBalanceInfo.py`, `dly_balance_info_backup.py`
- **내용:** `DB_PASSWORD = "asdf1234"` — 5개 파일에 평문 노출
- **권장:** `.env` 파일로 이동 후 `os.getenv("DB_PASSWORD")` 사용

### SEC-02: Slack 토큰 소스코드 하드코딩
- **해당 파일:** `upbitPivotTrend.py` (24행), `upbitPivotTrend2.py` (24행), `upbitTrendLine1.py` (32행)
- **내용:** Slack 토큰을 문자열 분할로 숨기려 했으나, 연결하면 전체 토큰 노출
- **권장:** `.env`에서 로드하도록 변경

### SEC-03: 테스트 파일에서 API 키 콘솔 출력
- **해당 파일:** `upbitTest.py` (17-18행), `bithumbTest.py` (12-13행)
- **내용:** `print("access_key : ", access_key)` — 로그에 키 기록 가능
- **권장:** API 키 출력 코드 즉시 제거

### SEC-04: SQL 인젝션 취약점
- **해당 파일:** `upbitPivotTrend.py` (223, 294행), `upbitPivotTrend2.py` (300, 369행), TrendLine 시리즈 전체
- **내용:** 문자열 연결(`"'"+i+"'"`)로 SQL 쿼리 구성
- **권장:** 파라미터화된 쿼리 사용 (`%s` 플레이스홀더)

### SEC-05: `.env` 파일 Git 커밋 여부 확인 필요
- **내용:** `.env`에 업비트/빗썸 API 키 3개 계정분, Slack 토큰 포함
- **권장:** `.gitignore`에 `.env` 추가 확인, 이미 커밋된 경우 키 즉시 교체

---

## 3. 리소스 누수 (RESOURCE LEAKS)

### LEAK-01: DB 커넥션 누수 (전체 프로젝트)
- **증상:** `try-finally` 또는 `with` 문 없이 커넥션 사용, 예외 발생 시 `conn.close()` 미호출
- **해당 파일:** `upbitTradeMng.py`, `upbitPivotTrend.py`, `upbitPivotTrend2.py`, TrendLine 시리즈, `upbitOpenOrder.py`, `upbitBalanceInfo.py`, `bithumbBalanceInfo.py`, `dly_balance_info_backup.py`
- **영향:** 장시간 실행 시 DB 커넥션 소진 → 연결 불가

### LEAK-02: 매 신호/종목마다 새 DB 커넥션 생성
- **증상:** 루프 내에서 `psycopg2.connect()` 반복 호출 (커넥션 풀 미사용)
- **해당 파일:** `upbitPivotTrend.py`, `upbitPivotTrend2.py`, TrendLine 시리즈 전체
- **영향:** 종목 수 × 신호 수만큼 커넥션 생성/해제 → DB 서버 부하

### LEAK-03: `sent_messages` Set 무한 증가 (메모리 누수)
- **해당 파일:** `upbitPivotTrend.py`, `upbitPivotTrend2.py`, TrendLine 시리즈 전체
- **증상:** `sent_messages = set()`에 메시지 추가만 하고 삭제/정리 없음
- **영향:** 장기 실행 시 메모리 지속 증가 → OOM 가능

### LEAK-04: HTTP 연결 풀링 미사용
- **해당 파일:** 전체 프로젝트
- **증상:** 매 API 호출마다 `requests.get()`/`requests.post()` → 새 TCP 연결 생성
- **권장:** `requests.Session()` 사용으로 연결 재사용

### LEAK-05: API 호출 낭비
- **해당 파일:** `upbitTradeMng.py` (133, 290행), 주문 파일 전체
- **증상:** 매 종목/주문마다 `/v1/accounts` API 호출 (결과 미사용 포함)
- **영향:** 업비트 API rate limit(초당 10회, 분당 600회) 불필요 소모

### LEAK-06: TrendLine 시리즈 - 루프 내 불변 계산 반복
- **해당 파일:** `upbitTrendLine.py`, `upbitTrendLineLong.py`, `upbitTrendLineMid.py`, `upbitTrendLineRecent.py`
- **증상:** `check_trend()`가 200개 행 루프 내에서 매번 호출되지만 결과는 항상 동일
- **영향:** 선형 회귀를 200번 반복 계산 → 극심한 CPU 낭비

---

## 4. 사용하지 않는 코드 (DEAD CODE)

### 4-1. 미사용 Import

| 파일 | 미사용 Import |
|------|--------------|
| `upbitTradeMng.py` | `sys`, `time`, `schedule` |
| `upbitPivotTrend.py` | `numpy as np` |
| `upbitPivotTrend2.py` | `numpy as np` |
| `upbitStocastics.py` | `date` (from datetime) |
| `slackTest.py` | `linregress` (from scipy.stats) |
| `bithumbTradeMng.py` | `sys`, `timedelta` |
| `bithumbBalanceInfo.py` | `sys` |
| `dly_balance_info_backup.py` | `schedule`, `time` |

### 4-2. 미사용 함수

| 파일 | 함수명 | 비고 |
|------|--------|------|
| `upbitPivotTrend2.py` | `calculate_fibonacci()` (52-65행) | 전체 파일에서 호출 없음 |
| `upbitPivotTrend2.py` | `calculate_rsi()` (67-96행) | 주석 처리된 코드에서만 참조 |
| `upbitTrendLineRecent.py` | `determine_trends()` (188-216행) | 전체 파일에서 호출 없음 |

### 4-3. 미사용 변수

| 파일 | 변수 | 위치 |
|------|------|------|
| TrendLine 시리즈 | `timeframe_1d`, `timeframe_4h` 등 | 사용하는 타임프레임 외 나머지 모두 미사용 |
| TrendLine 시리즈 | `close`, `l_close`, `ma_200` | 할당 후 미사용 |
| TrendLine 시리즈 | `new_signal_id = result[0]` | 저장 후 미사용 |
| 주문 파일들 | `bid_price` | 조회만 하고 사용 안 함 |
| `upbitTradeMng.py` | `result_1 = []` (70, 244행) | 바로 아래에서 재할당 |

### 4-4. 실질적 사문 파일

| 파일 | 이유 |
|------|------|
| `upbitOrder(이더리움sell).py` | 매도 주문 코드가 전체 주석 처리됨 (210-227행) |
| `upbitTrendLine1.py` | 실제 주문이 주석 처리되고 더미 UUID 사용 |
| `upbitStocastics.py` | 분석 결과를 출력만 하고 DB 저장이나 알림 없음 |
| `bithumbTradeMng.py` | `ordno_order()`에 하드코딩된 주문번호 사용 |

### 4-5. 대량 주석 처리 코드
거의 모든 `upbitOrder*.py` 파일에 주석 처리된 이전 거래 UUID, 주문 로직이 남아있어 코드 가독성 저하.

---

## 5. 중복 코드 패턴 (DUPLICATION)

### DUP-01: `place_order()` 함수 — 8개 파일에 동일 코드 복사
`upbitOrder.py`, 6개 주문 파일, `upbitOrder(정액-비트코인-지정가매수).py`

### DUP-02: `get_order()` 함수 — 10개+ 파일에 동일 코드 복사
위 8개 + `upbitOpenOrder.py`, `upbitBalanceInfo.py`

### DUP-03: JWT 인증 토큰 생성 패턴 — 전체 파일에 중복
매 API 호출 전 payload 생성 → jwt.encode → Bearer 토큰 생성 패턴이 모든 파일에서 반복

### DUP-04: TrendLine 시리즈 95% 중복
`upbitTrendLine.py`, `upbitTrendLineLong.py`, `upbitTrendLineMid.py`는 타임프레임과 시간 필터만 다르고 나머지 코드가 거의 동일:
- `calculate_peaks_and_troughs()`
- `calculate_indicators()`
- `determine_trends()`
- `check_trend()`
- `get_trend_line()`
- `predict_price()`
- `get_highs_lows()`

### DUP-05: `upbitBalanceInfo.py`와 `bithumbBalanceInfo.py` 구조 동일
거래소 API 엔드포인트만 다를 뿐 함수 구조, DB 처리 로직이 동일

### DUP-06: DB 연결 정보 5개 파일에 중복
```python
DB_NAME = "universe"
DB_USER = "postgres"
DB_PASSWORD = "asdf1234"
DB_HOST = "localhost"
DB_PORT = "5432"
```

---

## 6. 전략 효과성 분석 (STRATEGY REVIEW)

### 6-1. PivotTrend 전략 — ⚠️ 실질적으로 "죽은 전략"

**upbitPivotTrend.py:**
- 매수 조건: `volume_surge AND Uptrend AND close <= support1 AND close < ma_200`
- 매도 조건: `volume_surge AND Downtrend AND close >= resistance1 AND close > ma_200`
- **문제:** 상승추세(Uptrend)인데 200일 이동평균 아래(`close < ma_200`)이고 지지선 아래라는 것은 **논리적 모순**. 이 조건이 동시에 충족될 확률이 극히 낮아 신호가 거의 발생하지 않음
- **결론:** 사실상 동작하지 않는 전략

**upbitPivotTrend2.py:**
- 롤링 윈도우 기반 동적 피봇은 개선이지만, 동일한 모순된 조건 (`Uptrend AND close_m < ma_200`) 유지
- `calculate_fibonacci()`, `calculate_rsi()` 함수가 정의만 되고 미사용 → 전략에 통합되지 않은 미완성 상태

### 6-2. Stochastic 전략 — ⚠️ 네이밍 혼란, 전략 자체는 표준적

**upbitStocastics.py:**
- 코드 논리 자체는 올바른 Stochastic 과매수/과매도 전략
- 그러나 함수명(`upward_crossover`→실제 데드크로스)이 정반대 → 수정/확장 시 실수 유발
- **결론:** DB 저장이나 알림 연동이 없어 독립 분석 도구에 불과

### 6-3. TrendLine 전략 — ✅ 가장 유효한 접근, 그러나 핵심 필터 제거됨

**upbitTrendLine.py (단기/15분봉):**
- 선형 회귀 기반 추세선 돌파/이탈 + 거래량 급등 조합
- **문제:** `determine_trends()` 호출이 주석 처리됨 → 추세 필터 없이 거래량만으로 신호 생성 → **거짓 신호 증가**
- `check_trend()`가 루프 내 200번 반복 호출 (불필요한 성능 낭비)

**upbitTrendLineLong.py (장기/4시간봉):**
- 업비트 전체 KRW 마켓(200+개) 스캔
- **문제:** API rate limit에 의해 실행 시간이 매우 길 수 있음

**upbitTrendLineMid.py (중기/1시간봉):**
- TrendLine과 95% 동일, 타임프레임만 다름

**upbitTrendLine1.py (주문 실행 포함):**
- 손절(cut_price)과 목표가(goal_price) 기반 포지션 사이징 → **리스크 관리 측면에서 우수**
- **문제:** 실제 주문이 주석 처리되고 더미 UUID 사용 → 현재 프로덕션 불가

### 6-4. TrendLineRecent 전략 — ✅ 가장 진화된 형태

**upbitTrendLineRecent.py:**
- **ATR%(Average True Range) 기반 변동성 분류**로 종목별 최적 타임프레임 자동 선택
- 거래대금 상위 10개 종목만 대상 → 유동성 리스크 감소
- DB 테이블 자동 생성, 안전한 기본값 반환 등 방어적 코딩
- **문제:** `trend_type == 'short'`(고변동성)인 종목을 건너뛰고 있어 → **고변동성 시장 기회를 놓침**
- `determine_trends()` 함수가 정의되었으나 미호출 → 추세 필터 미적용

### 6-5. 전략 종합 평가

| 전략 | 시장 적응력 | 리스크 관리 | 신호 품질 | 실전 사용 가능 | 종합 |
|------|:---------:|:---------:|:--------:|:------------:|:----:|
| PivotTrend | ❌ 모순된 조건 | ❌ 없음 | ❌ 신호 미발생 | ❌ | 🔴 |
| PivotTrend2 | 🟡 동적 피봇 | ❌ 없음 | ❌ 모순 유지 | ❌ | 🔴 |
| Stochastic | 🟡 표준적 | ❌ 없음 | 🟡 표준 신호 | ❌ 알림 없음 | 🟠 |
| TrendLine (단기) | 🟡 15분봉 | ❌ 없음 | 🟠 추세필터 제거 | 🟡 신호만 | 🟠 |
| TrendLine (중기) | 🟡 1시간봉 | ❌ 없음 | 🟠 추세필터 제거 | 🟡 신호만 | 🟠 |
| TrendLine (장기) | 🟡 4시간봉 | ❌ 없음 | 🟠 추세필터 제거 | 🟡 신호만 | 🟠 |
| TrendLine1 (주문) | 🟡 15분봉 | ✅ 손절/목표가 | 🟠 추세필터 제거 | ❌ 더미 UUID | 🟠 |
| **TrendLineRecent** | **✅ ATR 적응형** | **🟡 부분적** | **✅ 거래대금 필터** | **🟡 short 제외** | **🟢** |

### "시장을 해킹하고 있는가?"에 대한 답변

**현재 상태로는 시장을 효과적으로 공략하고 있지 않습니다.** 주요 이유:

1. **가장 유망한 전략(TrendLineRecent)도 고변동성 종목을 건너뛰고 있어** 수익 기회를 놓치고 있음
2. **PivotTrend 계열은 모순된 조건으로 신호가 거의 발생하지 않아** 사실상 비활성 상태
3. **추세 필터(`determine_trends`)가 대부분의 전략에서 제거/미사용되어** 거짓 신호 위험이 높음
4. **리스크 관리(손절/목표가)가 TrendLine1에만 존재**하고 나머지는 진입 신호만 생성
5. **실제 주문 실행 코드가 주석 처리되어** 자동매매가 불완전한 상태
6. **치명적 버그(변수 섀도잉, 인덱스 오류, 변수명 불일치)가** 정상 동작을 방해

---

## 7. 권장 개선사항 (RECOMMENDATIONS)

### 우선순위 1: 치명적 버그 수정
- [ ] `upbitTradeMng.py`: 루프 변수 `m` 섀도잉 수정 (해시 변수명을 `hash_obj` 등으로 변경)
- [ ] `upbitOrder(이더리움sell).py`: `sell_target_amount` → `sell_target_amt` 변수명 통일
- [ ] `upbitOpenOrder.py`: `chk_ord[9]` → 올바른 인덱스 수정, `fetchone()` None 체크 추가
- [ ] `upbitOrder.py`: 마켓 불일치 수정 (SOL 호가 → BTC 주문)

### 우선순위 2: 보안 강화
- [ ] DB 비밀번호를 `.env`로 이동
- [ ] Slack 토큰 하드코딩 제거 → `.env`에서 로드
- [ ] 테스트 파일의 API 키 출력 코드 제거
- [ ] SQL 인젝션 방지를 위해 파라미터화된 쿼리 사용
- [ ] `.gitignore`에 `.env` 포함 확인

### 우선순위 3: 리소스 누수 해결
- [ ] DB 커넥션을 `with` 문 또는 `try-finally`로 관리
- [ ] 커넥션 풀링 도입 (`psycopg2.pool`)
- [ ] `sent_messages` set에 TTL 기반 정리 로직 추가
- [ ] `requests.Session()` 사용으로 HTTP 연결 풀링
- [ ] TrendLine 시리즈의 루프 내 불변 계산을 루프 밖으로 이동

### 우선순위 4: 코드 구조 개선
- [ ] 공통 모듈 분리: `upbit_common.py` (JWT 인증, place_order, get_order, DB 연결)
- [ ] TrendLine 시리즈를 단일 파일로 통합 (타임프레임을 파라미터로)
- [ ] 파일명에서 한글 제거
- [ ] `logging` 모듈 도입
- [ ] 에러 핸들링 강화

### 우선순위 5: 전략 개선
- [ ] PivotTrend: 모순된 조건 수정 (200MA 조건 제거 또는 조건 방향 통일)
- [ ] TrendLine 시리즈: `determine_trends()` 호출 복원하여 추세 필터 활성화
- [ ] TrendLineRecent: `short` 변동성 타입도 분석하도록 변경
- [ ] TrendLine1의 리스크 관리 로직을 다른 전략에도 적용
- [ ] Stochastic 신호를 DB/Slack 알림에 통합

---

*이 보고서는 코드 정적 분석을 기반으로 작성되었습니다. 실제 트레이딩 성과는 백테스팅을 통해 별도 검증이 필요합니다.*
