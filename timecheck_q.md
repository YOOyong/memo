```sql
SELECT
  job_id,
  job_run_id,
  MAX(CASE WHEN status = 'count_check' THEN log_dt END) AS count_check_time,
  MAX(CASE WHEN status = 'insert_done' THEN log_dt END) AS insert_done_time,
  AGE(
    MAX(CASE WHEN status = 'insert_done' THEN log_dt END),
    MAX(CASE WHEN status = 'count_check' THEN log_dt END)
  ) AS duration
FROM
  your_table_name
WHERE
  status IN ('count_check', 'insert_done')
GROUP BY
  job_id,
  job_run_id
ORDER BY
  job_id,
  job_run_id;
```
- AGE() 함수는 두 timestamp 간의 차이를 interval로 반환


### 통계 내기
```sql
WITH job_durations AS (
  SELECT
    job_id,
    job_run_id,
    EXTRACT(EPOCH FROM (
      MAX(CASE WHEN status = 'insert_done' THEN log_dt END)
      - MAX(CASE WHEN status = 'count_check' THEN log_dt END)
    )) AS duration_seconds
  FROM
    your_table_name
  WHERE
    status IN ('count_check', 'insert_done')
  GROUP BY
    job_id,
    job_run_id
  HAVING
    COUNT(DISTINCT status) = 2  -- 둘 다 있는 경우만 필터링
)
```
```sql
SELECT
  job_id,
  COUNT(*) AS run_count,
  ROUND(AVG(duration_seconds), 2) AS avg_duration_sec,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_seconds) AS median_duration_sec,
  MIN(duration_seconds) AS min_duration_sec,
  MAX(duration_seconds) AS max_duration_sec
FROM
  job_durations
GROUP BY
  job_id
ORDER BY
  job_id;
```
- EXTRACT(EPOCH FROM interval)은 interval을 초 단위 숫자로 변환
- 평균은 AVG(), 중앙값은 PERCENTILE_CONT(0.5)

