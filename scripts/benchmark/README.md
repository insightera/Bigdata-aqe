# Benchmark & Metrik Otomatis (End-to-End)

Skrip pengukuran eksperimen AQE: pipeline Medallion, workload query, perbandingan OFF vs ON, agregasi laporan, dan ekspor ke **Prometheus/Grafana**.

## Struktur

| Skrip | Fungsi | Output |
|-------|--------|--------|
| [`dataset_summary.py`](dataset_summary.py) | Statistik CSV staging + skew | `metrics/dataset_summary_*.json` |
| [`run_spark_workloads.py`](run_spark_workloads.py) | W1–W3 di Silver (Spark) | `metrics/workloads_spark_aqe_{OFF\|ON}_*.json` |
| [`run_trino_workloads.py`](run_trino_workloads.py) | W4–W6 di Gold (Trino) | `metrics/workloads_trino_ctx_{OFF\|ON}_*.json` |
| [`compare_aqe_runs.py`](compare_aqe_runs.py) | Speedup OFF vs ON | `metrics/aqe_comparison_*.json` |
| [`aggregate_results.py`](aggregate_results.py) | Ringkasan eksperimen | `metrics/experiment_summary_*.json` |
| [`metrics_exporter.py`](metrics_exporter.py) | HTTP `/metrics` Prometheus | Grafana |
| [`run_experiment.py`](run_experiment.py) | Orkestrator lengkap | Semua file di atas |

Pipeline Spark juga menulis metrik otomatis:

- `staging_to_bronze_*.json`
- `bronze_to_silver_aqe_{OFF\|ON}_*.json`
- `silver_to_gold_*.json`

## Jalankan eksperimen penuh

### Opsi A — Airflow DAG (disarankan di Docker)

```bash
# Pastikan data ada di data/staging/*.csv
python3 scripts/generate_bronze_data.py --mode full --profile aqe

./start.sh

docker exec lhaqe-airflow-scheduler airflow dags trigger aqe_full_experiment
```

Pantau: http://localhost:18681 → DAG `aqe_full_experiment`

### Opsi B — Skrip lokal (dari repo, stack hidup)

```bash
export PYTHONPATH=scripts
export AQE_METRICS_DIR=metrics
export SPARK_MASTER=spark://localhost:7077   # jika Spark di-host

python3 scripts/benchmark/run_experiment.py --mode local
```

### Opsi C — Langkah per langkah

```bash
export PYTHONPATH=scripts AQE_METRICS_DIR=metrics

python3 scripts/benchmark/dataset_summary.py --staging-dir data/staging
# ... jalankan pipeline via Airflow atau import spark ...

python3 scripts/benchmark/run_spark_workloads.py --aqe-scenario OFF
python3 scripts/benchmark/run_spark_workloads.py --aqe-scenario ON

python3 scripts/benchmark/run_trino_workloads.py --aqe-context OFF --trino-url http://localhost:18088
python3 scripts/benchmark/run_trino_workloads.py --aqe-context ON --trino-url http://localhost:18088

python3 scripts/benchmark/compare_aqe_runs.py --markdown
python3 scripts/benchmark/aggregate_results.py --write-latest
```

## Grafana & Prometheus

Setelah eksperimen:

1. **Metrics exporter:** http://localhost:9101/metrics  
2. **Prometheus:** http://localhost:19090 → target `lakehouse_aqe_metrics`  
3. **Grafana:** http://localhost:13001 → dashboard **Lakehouse AQE Experiment**

Reload Prometheus jika perlu:

```bash
curl -X POST http://localhost:19090/-/reload
```

## Pemetaan laporan (BAB IV)

| Output JSON | Subbab |
|-------------|--------|
| `dataset_summary_*.json` | §3.3 |
| `staging_to_bronze_*.json`, `silver_to_gold_*.json` | §4.1.1 |
| `bronze_to_silver_aqe_*`, `aqe_comparison_*.json` | §4.1.2 |
| `workloads_spark_*` | §4.1.3–4.1.4 |
| `workloads_trino_*` | §4.1.6 |
| `experiment_summary_latest.json` | Ringkasan seluruh eksperimen |

Panduan metodologi: [`../../docs/eksperimen/README.md`](../../docs/eksperimen/README.md)
