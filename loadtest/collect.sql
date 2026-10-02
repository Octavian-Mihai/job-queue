select json_build_object(
  'jobs_in_db', count(*),
  'succeeded', count(*) filter (where status = 'SUCCEEDED'),
  'dead', count(*) filter (where status = 'DEAD'),
  'cancelled', count(*) filter (where status = 'CANCELLED'),
  'not_terminal', count(*) filter (where status in ('PENDING', 'RUNNING')),
  'makespan_s', extract(epoch from (max(finished_at) - min(created_at))),
  'e2e_p50_s', percentile_cont(0.50) within group (order by extract(epoch from (finished_at - created_at))),
  'e2e_p95_s', percentile_cont(0.95) within group (order by extract(epoch from (finished_at - created_at))),
  'e2e_p99_s', percentile_cont(0.99) within group (order by extract(epoch from (finished_at - created_at))),
  'e2e_max_s', max(extract(epoch from (finished_at - created_at))),
  'attempts_total', (select count(*) from job_attempts),
  'attempts_lease_expired', (select count(*) from job_attempts where outcome = 'LEASE_EXPIRED'),
  'jobs_with_more_than_one_attempt', count(*) filter (where attempts > 1),
  'succeeded_send_email', count(*) filter (where status = 'SUCCEEDED' and type = 'send-email'),
  'emails_in_outbox', (select count(*) from email_outbox),
  'dlq_entries', (select count(*) from dead_letter_jobs),
  'distinct_workers', (select count(distinct worker_id) from job_attempts)
) from jobs where finished_at is not null or status in ('PENDING', 'RUNNING')
