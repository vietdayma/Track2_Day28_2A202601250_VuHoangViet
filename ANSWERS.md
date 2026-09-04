# ANSWERS — Day 28 Track 2

**Người làm:** Vũ Hoàng Việt — làm **cá nhân**, đi qua đủ 5 vai trò (Ingestion & Orchestration, Data & ML, Serving & Retrieval, Platform & Observability, Presenter).

**Nhánh nộp:** `main` của repo cá nhân `vietdayma/Track2_Day28_2A202601250_VuHoangViet`.

**Môi trường chạy:** Windows 11, Docker Desktop (WSL2), 12 CPU. Chạy `--profile full` (core + Spark Connect + Airflow). Không có GPU cục bộ → IP07 (vLLM thật) báo **UNVERIFIED**, không làm giả.

---

## 1. Bốn chức năng đã hoàn thiện (`src/lab28_platform/integration_tasks.py`)

| Hàm | IP | Quyết định kỹ thuật |
|---|---|---|
| `event_headers` | IP01 + IP10 | Luôn phát `idempotency-key` dạng bytes. Chỉ thêm `traceparent` khi có trace đang hoạt động — không gửi chuỗi rỗng vì header W3C rỗng là header **sai định dạng**, làm hỏng việc nối trace phía consumer. Không hard-code key/trace: cả hai đến từ tham số. |
| `dedupe_latest` | IP03 | Duyệt danh sách **đúng một lần** (`for` trên iterable), gom theo `idempotency_key`, giữ bản có `(occurred_at, event_id)` lớn nhất. So sánh cặp thay vì chỉ `occurred_at` để hai bản tin trùng timestamp vẫn ra kết quả **xác định**, không phụ thuộc thứ tự Kafka giao. Trả về sắp theo key → Delta MERGE nhận nguồn không có 2 dòng cùng khóa (MERGE sẽ lỗi nếu có). Đầu vào rỗng → `[]`. |
| `feast_online_request` | IP04 | `entities = {"asker_id": [asker_id]}`, `features = list(FEATURE_REFS)`, `full_feature_names = False`. Lấy danh sách 4 đặc trưng từ `contracts.FEATURE_REFS` — một nguồn sự thật duy nhất, không chép lại registry ở nhiều nơi. |
| `readiness_status` | IP07 + IP08 | Thứ tự ưu tiên rõ ràng: có `mandatory` lỗi → `not_ready`; chỉ optional lỗi → `degraded`; còn lại → `ready`. Dùng `probe.get(...)` để chịu được probe thiếu khóa. |

Kết quả: `pytest starter-tests` 4/4 pass (trước đó 4 fail `NotImplementedError`); `pytest tests` 83 pass; không sửa/xóa test, không che `NotImplementedError`.

---

## 2. Bằng chứng 10 điểm kết nối

Chạy `--profile full`, `lab28 seed --via-gateway`, `lab28 index --source file`, `lab28 release`, rồi toàn bộ `pytest integration-tests -m "not gpu and not langsmith"` → **56 passed, 16 deselected**. J1 (12 passed / 3 skip gpu), J2 (9 passed).

| IP | Trạng thái | Bằng chứng |
|---|---|---|
| IP01 FastAPI→Kafka | ✅ | `evidence/ip01-kafka-consume.json` — message trên `data.raw`, header `idempotency-key` + `traceparent`, key = `entity_id` (giữ thứ tự theo asker), `trace_id` khớp trace do test sinh |
| IP02 Kafka→Airflow | ✅ | `evidence/ip02-airflow-run.json` — DAG `lab28_ingestion_pipeline` state `success`, task states, asset event `lab28://delta/feedback` |
| IP03 Airflow→Delta | ✅ | `evidence/ip03-delta-history.json` — `_delta_log`, MERGE history, time-travel diff. Hiện tại: `documents v6 (17 rows)`, `feedback v9 (10 rows)`. J2 chứng minh replay không tăng số dòng |
| IP04 Delta→Feast | ✅ | `evidence/ip04-feast-online.json` — entity `asker_id` đọc được sau materialize, có `delta_version` và `freshness_seconds` |
| IP05 Delta→Qdrant | ✅ | `evidence/ip05-qdrant-search.json` — collection `lab28_documents` 18 points, point ID = UUID xác định từ `doc_id` (re-index ghi đè, không nhân đôi), truy vấn hybrid có score |
| IP06 Delta→MLflow | ✅ | `evidence/ip06-mlflow-release.json` — `lab28-rag-release v1` là `champion`, tags: prompt version, vllm model id, embedding model, collection, feature service, delta version. J3 chứng minh promote → rollback alias không sửa mã |
| IP07 FastAPI→vLLM | ⚠️ UNVERIFIED | Không có GPU cục bộ. `lab28 ready` báo `vllm: not_ready — ReadTimeout`. Core vẫn phục vụ ở chế độ `degraded` (câu trả lời lạnh hơn, ghi rõ lý do trong evidence), **không làm giả** server vLLM. Để đạt IP07: chạy vLLM thật trên Kaggle T4 theo `KAGGLE_GPU_EXTENSION.md`, gate kiểm `/version`, `/v1/models`, metric `vllm:` |
| IP08 Client→Envoy→API | ✅ | `evidence/ip08-gateway.json` — cùng route: 10×200 + 20×429 (local rate limit 10 rps), mỗi phản hồi có `x-request-id` |
| IP09 →Prometheus/Grafana | ✅ | `evidence/ip09-prometheus-targets.json` (mọi job `up`, alert rules nạp) + `evidence/ip09-grafana-dashboards.json` (dashboard provisioned) |
| IP10 →OTel/Jaeger | ✅ (leg cục bộ) | `evidence/ip10-trace.json` — 1 trace ID đi xuyên `gateway.request → api.ingest → kafka.produce → kafka.consume → airflow.dag → spark.delta_merge` (6/6 span bắt buộc của luồng ingestion). Các span của luồng `ask` (feast/qdrant/mlflow/vllm) thuộc phần GPU-gated. Leg LangSmith: UNVERIFIED (không có `LANGSMITH_API_KEY`) |

`lab28 integration` (probe trực tiếp từ serving process, chỉ IP01/03/04/05/06/07): **score 83** = 5/6 verified pass, IP07 fail vì không GPU. IP02/08/09/10 để `unverified` ở lệnh này **theo thiết kế** — chúng được chứng minh bằng file evidence + integration test, không phải bằng probe nội bộ.

---

## 3. Sự cố đã tạo và khôi phục (J4 — degraded/recovery)

**Kịch bản:** tắt Feast (thành phần **không bắt buộc**), sau đó tắt Qdrant (thành phần **bắt buộc**).

| Bước | Feast down | Qdrant down |
|---|---|---|
| Dự đoán dấu hiệu | `/ready` → `degraded`, HTTP 200, pod vẫn trong rotation | `/ready` → `not_ready`, HTTP 503, gateway loại pod khỏi rotation |
| Quan sát | `_component(body,"feast").ready == False`, `status != not_ready`; `/api/v1/ask` vẫn trả lời, `evidence.degraded == True`, `degraded_reasons` chứa "feature store", counter `lab28_degraded_responses_total{reason="feast"}` tăng | `/ready` 503 có breakdown per-dependency; gateway trả lỗi Envoy (không có `components` trong body); pod vẫn trả lời khi gọi **trực tiếp** (readiness ≠ liveness) |
| Nguyên nhân | Container Feast dừng → probe HTTP fail; vì `mandatory=False` nên không leo thang thành 503 | Container Qdrant dừng → probe fail; retrieval là mandatory nên `/ready` fail-closed |
| Khôi phục | `docker compose start feast` → probe xanh lại trong ~30s | `docker compose start qdrant` → `/ready` về `degraded` (baseline khi không GPU) |
| Không mất dữ liệu | — | Bản tin không parse được → parked vào `data.raw.dlq` **kèm toạ độ** (topic/partition/offset/key); bản ghi tốt cùng lô vẫn vào Delta (J4 `test_the_good_record_in_the_same_batch_still_reached_the_lakehouse`); replay DLQ đưa lại đúng 1 dòng, không nhân đôi (`test_the_replayed_event_does_not_duplicate_the_row`) |

Lệnh dùng khi demo: `docker compose --env-file ports.template stop feast` … `start feast` (đúng cơ chế `stack.dependency_down` mà test dùng). **Không** dùng `lab28 reset` — nó xoá state trước sự cố.

---

## 4. Trade-offs đã chọn

1. **Dedup ở tầng ứng dụng, trước khi vào Spark** thay vì để MERGE tự xử lý. Được: kiểm thử được không cần JVM (`test_delta_merge_idempotency.py`), lỗi hiện ra ở diff 1 dòng thay vì stack trace JVM trong log Airflow. Mất: một quy tắc nghiệp vụ nằm ngoài engine lưu trữ, phải giữ đồng bộ giữa hai bên đọc contract.
2. **vLLM là probe không bắt buộc trong profile cục bộ** (`LAB28_VLLM_REQUIRE_REAL=false`) nên `/ready` HTTP trả `degraded`/200 và gateway giữ pod; còn `lab28 ready` (CLI, strict) vẫn báo `not_ready` để nhắc IP07 chưa verify. Trade-off: cùng một hệ thống cho hai câu trả lời khác nhau tuỳ ngữ cảnh — hợp lý (rotation vs. gate nộp bài) nhưng phải giải thích rõ khi demo.
3. **Key Kafka = `entity_id`, không phải `idempotency_key`.** Được: mọi event của một asker nằm cùng partition → đúng thứ tự. Mất: dedup không thể dựa vào key của broker, phải làm ở lakehouse (IP03).
4. **Point ID Qdrant = UUID suy ra từ `doc_id`** (`stable_point_id`). Được: re-index idempotent. Mất: không lưu được hai phiên bản của cùng `doc_id` song song.

---

## 5. Khoảng cách so với production

- **IP07 chưa verify** — cần vLLM thật (Kaggle/cluster) + gate `/version` & metric `vllm:`.
- **LangSmith leg** — chỉ có OTLP backend cục bộ; production cần `LANGSMITH_API_KEY`.
- `/ready` **probe đồng bộ mỗi request** → P50 ~5.3s do vLLM ReadTimeout (xem `evidence/load-profile.json`). Production: cache kết quả probe với TTL ngắn, hoặc probe song song có deadline, hạ timeout vLLM ở profile cục bộ.
- **1 replica cho mỗi service, replication_factor=1** cho Kafka — không chịu lỗi node. Envoy `healthy_panic_threshold` mặc định 50% với 1 upstream → cần cấu hình lại cho cụm thật.
- **Bí mật** hiện qua biến môi trường Compose; production cần secret manager + rotation.
- Airflow/Spark chạy 1 worker; chưa có autoscaling, chưa có backpressure khi Kafka lag tăng.

---

## 6. Reflection

> *(Phần này bạn tự viết lại theo trải nghiệm thật của mình — dưới đây là gợi ý dựa trên quá trình làm.)*

- **Khó nhất:** hiểu vì sao `dedupe_latest` phải so `(occurred_at, event_id)` chứ không chỉ `occurred_at` — chỉ vỡ ra khi đọc test `test_events_sharing_a_timestamp_resolve_deterministically` và nhận ra Kafka chỉ đảm bảo thứ tự *trong một partition*, không phải trong một batch.
- **Bất ngờ:** `/ready` HTTP (`degraded`) và `lab28 ready` CLI (`not_ready`) cho kết quả khác nhau trên cùng một trạng thái — hoá ra là do `mandatory` của probe vLLM phụ thuộc config, và đó chính là minh hoạ sống cho phân biệt `ready`/`degraded`/`not_ready`.
- **Sẽ cải tiến:** làm `/ready` probe song song có timeout riêng để load test không bị vLLM timeout kéo P50 lên 5s.

---

## 7. Lệnh tái lập

```powershell
uv sync --frozen --python 3.11 --extra dev --extra integration --no-editable
uv run --no-sync pytest starter-tests tests -q          # 87 passed
uv run --no-sync ruff check .                            # clean
uv run --no-sync python scripts/verify_matrix.py         # 245 checks
uv run --no-sync python scripts/check_portability.py     # OK
uv run --no-sync python scripts/validate_manifests.py    # OK

docker compose --env-file ports.template --profile full up -d --build --wait
uv run --no-sync lab28 topics
uv run --no-sync lab28 index --source file
uv run --no-sync lab28 release
uv run --no-sync lab28 seed --via-gateway
uv run --no-sync pytest integration-tests -m "not gpu and not langsmith" -q   # 56 passed
$env:PYTHONUTF8=1; uv run --no-sync lab28 evidence
$env:PYTHONUTF8=1; uv run --no-sync lab28 integration                          # score 83
uv run --no-sync python load-tests/run_profile.py --requests 200 --workers 8
```
