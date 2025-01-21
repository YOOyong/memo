import jaydebeapi

# Impala JDBC 연결 설정 (이 예시에서는 약식으로 보여줍니다)
conn = jaydebeapi.connect(
    'org.apache.hive.jdbc.HiveDriver',
    'jdbc:hive2://<IMPALA_HOST>:21050/default',
    {'user': '<USERNAME>', 'password': '<PASSWORD>'},
    jars='/path/to/impala-jdbc-driver.jar'  # Impala JDBC 드라이버 경로
)

# 커서 생성
cursor = conn.cursor()

# 테이블 이름이 포함된 텍스트 파일 경로
input_file = 'tables.txt'
output_file = 'table_sizes.txt'

# 결과를 저장할 파일 열기
with open(output_file, 'w') as f_out:
    # 첫 번째 줄에 컬럼명 추가
    f_out.write("table,size\n")

    # 테이블 이름이 포함된 텍스트 파일 열기
    with open(input_file, 'r') as f_in:
        for line in f_in:
            table_name = line.strip()  # 공백 제거
            if not table_name:  # 빈 줄은 무시
                continue

            # `SHOW TABLE STATS` 쿼리 실행
            try:
                cursor.execute(f"SHOW TABLE STATS {table_name}")
                results = cursor.fetchall()

                # 마지막 행에서 total size 추출
                total_size = results[-1][2]  # 'Size'는 3번째 컬럼 (인덱스 2)에 있음

                # 텍스트 파일에 테이블 이름과 크기 저장
                f_out.write(f"{table_name},{total_size}\n")
            except Exception as e:
                # 예외 처리: 테이블 이름이 잘못된 경우 또는 다른 오류가 발생한 경우
                error_message = str(e).replace('\n', ' ')  # 줄바꿈 제거
                print(f"Error with table {table_name}: {error_message}")
                f_out.write(f"{table_name},ERROR: {error_message}\n")

# 연결 종료
cursor.close()
conn.close()

print("Processing complete.")
