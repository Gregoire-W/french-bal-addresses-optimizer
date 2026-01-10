mvn clean install
rm -rf bal.db

./scripts/run_daily_file_integration.sh 2025-01-01 data/adresses-mini-step1.csv
./scripts/run_daily_file_integration.sh 2025-01-02 data/adresses-mini-step2.csv
./scripts/run_daily_file_integration.sh 2025-01-03 data/adresses-mini-step3.csv

./scripts/run_report.sh

./scripts/recompute_and_extract_dump_at_date.sh 2025-01-01 bal.db/recompute_2025-01-01
./scripts/recompute_and_extract_dump_at_date.sh 2025-01-02 bal.db/recompute_2025-01-02
./scripts/recompute_and_extract_dump_at_date.sh 2025-01-03 bal.db/recompute_2025-01-03

./scripts/compute_diff_between_files.sh bal.db/recompute_2025-01-01  bal.db/recompute_2025-01-02
./scripts/compute_diff_between_files.sh bal.db/recompute_2025-01-01  bal.db/recompute_2025-01-03
./scripts/compute_diff_between_files.sh bal.db/recompute_2025-01-02  bal.db/recompute_2025-01-03