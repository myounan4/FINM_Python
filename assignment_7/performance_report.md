# Performance Report

| metric                 |    pandas |     polars |
|:-----------------------|----------:|-----------:|
| ingest_time_s          |  0.362975 |   0.212683 |
| ingest_mem_MB          | 27.1094   |  64.8906   |
| rolling_seq_time_s     |  0.325009 |   0.110097 |
| rolling_seq_mem_MB     | 32.4648   |  30.5      |
| rolling_thread_time_s  |  0.164162 |   0.106815 |
| rolling_thread_mem_MB  | 30.5      |  24.625    |
| rolling_process_time_s |  0.281927 |   1.20279  |
| rolling_process_mem_MB |  1.17969  |  40.8398   |
| portfolio_time_s       |  0.124507 | nan        |
| portfolio_mem_MB       |  1.5625   | nan        |
