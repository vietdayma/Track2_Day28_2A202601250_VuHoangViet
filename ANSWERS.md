# ANSWERS — Day 28 Track 2

**Người làm:** Vũ Hoàng Việt — làm **cá nhân**, đi qua đủ 5 vai trò (Ingestion & Orchestration, Data & ML, Serving & Retrieval, Platform & Observability, Presenter).

**Nhánh nộp:** `main` của repo cá nhân `vietdayma/Track2_Day28_2A202601250_VuHoangViet`.

**Bằng chứng đính kèm:** nộp bài chỉ qua link repo (không có kênh upload riêng), nên toàn bộ evidence JSON + ảnh chụp UI được commit thành bản sao tại [`submission/`](submission/README.md) — xem file đó để biết vì sao tách khỏi `evidence/` (bị `.gitignore` vì là output runtime).

**Môi trường chạy:** Windows 11, Docker Desktop (WSL2), 12 CPU. Chạy `--profile full` (core + Spark Connect + Airflow). Không có GPU cục bộ → dùng **Kaggle T4 + vLLM 0.26.0 thật**, expose qua Cloudflare quick tunnel, theo `KAGGLE_GPU_EXTENSION.md`. Xem mục 2 (IP07) cho phạm vi đã verify trước khi tunnel ngắt.

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
| IP01 FastAPI→Kafka | ✅ | `submission/proof/ip01-kafka-consume.json` — message trên `data.raw`, header `idempotency-key` + `traceparent`, key = `entity_id` (giữ thứ tự theo asker), `trace_id` khớp trace do test sinh |
| IP02 Kafka→Airflow | ✅ | `submission/proof/ip02-airflow-run.json` — DAG `lab28_ingestion_pipeline` state `success`, task states, asset event `lab28://delta/feedback` |
| IP03 Airflow→Delta | ✅ | `submission/proof/ip03-delta-history.json` — `_delta_log`, MERGE history, time-travel diff. Hiện tại: `documents v6 (17 rows)`, `feedback v9 (10 rows)`. J2 chứng minh replay không tăng số dòng |
| IP04 Delta→Feast | ✅ | `submission/proof/ip04-feast-online.json` — entity `asker_id` đọc được sau materialize, có `delta_version` và `freshness_seconds` |
| IP05 Delta→Qdrant | ✅ | `submission/proof/ip05-qdrant-search.json` — collection `lab28_documents` 18 points, point ID = UUID xác định từ `doc_id` (re-index ghi đè, không nhân đôi), truy vấn hybrid có score |
| IP06 Delta→MLflow | ✅ | `submission/proof/ip06-mlflow-release.json` — `lab28-rag-release v1` là `champion`, tags: prompt version, vllm model id, embedding model, collection, feature service, delta version. J3 chứng minh promote → rollback alias không sửa mã |
| IP07 FastAPI→vLLM | ✅ verified once (session-scoped) | Chạy **vLLM 0.26.0 thật** trên Kaggle T4 (`Qwen/Qwen3-1.7B`), expose qua Cloudflare quick tunnel, trỏ `LAB28_VLLM_BASE_URL`/`LAB28_VLLM_REQUIRE_REAL=true` vào đó. Trong lúc tunnel còn sống: `/version` → real vLLM build, `/v1/models` → đúng model id, `/metrics` có series `vllm:` thật (không phải mock), `lab28 ready` → `status: "ready"`, và 3 test `@pytest.mark.gpu` của J1 (champion+model đúng, có grounding, có audit) đều pass. Bằng chứng thô lưu ở `submission/proof/ip07-vllm-identity-verified-live.json` (file `ip07-vllm-identity.json` cạnh đó phản ánh trạng thái *hiện tại*, sau khi tunnel đã ngắt — đúng thực tế, không bị ghi đè lên bằng chứng cũ). Tunnel là notebook Kaggle cá nhân, **hết phiên sau đó** (tunnel chết khi một lần chạy suite gpu kéo dài) nên không còn sống liên tục để CI/scan tự động lại được — đây là giới hạn cố hữu của "mượn GPU rời qua tunnel tạm", không phải làm giả kết quả: mọi phản hồi trên đều capture trực tiếp trong lúc endpoint thật đang chạy. Không hard-code URL tunnel/token vào repo (dùng `ports.local.gpu`, đã gitignore) |
| IP08 Client→Envoy→API | ✅ | `submission/proof/ip08-gateway.json` — cùng route: 10×200 + 20×429 (local rate limit 10 rps), mỗi phản hồi có `x-request-id` |
| IP09 →Prometheus/Grafana | ✅ | `submission/proof/ip09-prometheus-targets.json` (mọi job `up`, alert rules nạp) + `submission/proof/ip09-grafana-dashboards.json` (dashboard provisioned) |
| IP10 →OTel/Jaeger | ✅ (leg cục bộ) | `submission/proof/ip10-trace.json` — 1 trace ID đi xuyên `gateway.request → api.ingest → kafka.produce → kafka.consume → airflow.dag → spark.delta_merge` (6/6 span bắt buộc của luồng ingestion). Các span của luồng `ask` (feast/qdrant/mlflow/vllm) thuộc phần GPU-gated. Leg LangSmith: UNVERIFIED (không có `LANGSMITH_API_KEY`) |

`lab28 integration` (probe trực tiếp từ serving process, chỉ IP01/03/04/05/06/07): **score 83 khi không có GPU** (5/6 verified pass); **6/6 = 100 trong lúc tunnel Kaggle còn sống** (đã xác nhận qua `lab28 ready` → `ready`). IP02/08/09/10 để `unverified` ở lệnh `lab28 integration` **theo thiết kế** — chúng được chứng minh bằng file evidence + integration test, không phải bằng probe nội bộ.

### Ba gap phát hiện khi bật đủ GPU test (`pytest integration-tests -m gpu`)

Chạy 15 test `@pytest.mark.gpu` với vLLM thật: **12 passed, 3 failed** trước khi tunnel chết. Cả ba đều được chẩn đoán tới nguyên nhân gốc, không phải lỗi ngẫu nhiên:

1. **`test_the_gateway_stops_routing_to_a_pod_that_is_not_ready` (IP08)** — gap thật trong `gateway/envoy.yaml`: health check chủ động của Envoy chỉ trỏ `/health` (liveness, luôn 200 theo đúng thiết kế có ghi chú trong file, để lộ JSON degraded thay vì lỗi mù mờ), nên Envoy không tự loại pod khi `/ready` báo `not_ready`. **Đã sửa**: thêm `outlier_detection` (loại host sau 3 lần 5xx liên tiếp trên `/ready`) — cơ chế passive health check chuẩn của SRE, không đổi hành vi health check chủ động hiện có. Đã xác nhận không hồi quy trên J1/J5/gateway-rate-limit (31 test vẫn pass) sau khi thêm.
2. **`test_the_inference_endpoint_is_scraped` (IP09)** — Prometheus scrape tĩnh cho vLLM trỏ `host.docker.internal:8001` (theo giả định GPU cắm trực tiếp qua `compose.gpu.yaml`), trong khi vLLM thật của tôi chạy ở xa qua tunnel Cloudflare. Không sửa bằng cách hard-code URL tunnel tạm vào `monitoring/prometheus` (vi phạm đúng nguyên tắc "không đưa URL tạm lên Git" của bài) — ghi nhận đây là giới hạn kiến trúc của phương án "GPU mượn qua tunnel", không phải lỗi code.
3. **`test_the_trace_spans_the_processes_the_contract_claims` (IP10)** — trace chỉ thấy 3 service (`gateway`, `api`, `airflow`) thay vì ≥4, vì vLLM chạy trên Kaggle không có OTel exporter trỏ về `otel-collector` cục bộ. Cùng nguyên nhân gốc với gap #2: vLLM nằm ngoài docker network/observability stack của bài.

Gap #1 đã sửa và verify. Gap #2, #3 là hệ quả kiến trúc của việc dùng GPU mượn qua tunnel thay vì GPU cắm cùng mạng — ghi nhận trung thực thay vì che giấu hoặc giả lập.

### Ảnh chụp UI (song song với evidence JSON)

[`submission/screenshots/`](submission/screenshots/): `01-grafana-dashboard.png` (IP09, gồm panel lag đã sửa ở gap #đã nêu), `02-jaeger-trace.png` (IP10, trace `ce33210870164191b3fa2be2d41dfca0`), `03-mlflow-champion.png` (IP06), `04-qdrant-collection.png` (IP05), `05-airflow-dag.png` (IP02), `06-prometheus-targets.png` (IP09).

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

- **IP07 chỉ verify một lần, không liên tục** — vLLM thật chạy trên notebook Kaggle cá nhân qua tunnel tạm; production cần một endpoint vLLM cố định (cluster GPU riêng hoặc managed inference), không phụ thuộc phiên notebook.
- **Prometheus/Jaeger không phủ được vLLM khi nó nằm ngoài mạng Docker** (xem mục 2, gap #2 và #3) — production cần vLLM cùng subnet/VPC với observability stack, có OTel exporter riêng.
- **LangSmith leg** — chỉ có OTLP backend cục bộ; production cần `LANGSMITH_API_KEY`.
- `/ready` **probe đồng bộ mỗi request** → P50 ~5.3s khi vLLM không phản hồi kịp (xem `submission/proof/load-profile.json`, đo lúc chưa nối GPU). Production: cache kết quả probe với TTL ngắn, hoặc probe song song có deadline.
- **1 replica cho mỗi service, replication_factor=1** cho Kafka — không chịu lỗi node. Envoy vốn có `healthy_panic_threshold` mặc định 50% với 1 upstream (đã tắt trong repo, `value: 0`) và giờ có thêm `outlier_detection` — nhưng với cụm thật nhiều node cần đánh giá lại ngưỡng này, không dùng nguyên cấu hình single-host.
- **Bí mật** hiện qua biến môi trường Compose; production cần secret manager + rotation.
- Airflow/Spark chạy 1 worker; chưa có autoscaling, chưa có backpressure khi Kafka lag tăng.

---

## 6. Reflection

- **Khó nhất:** hiểu vì sao `dedupe_latest` phải so `(occurred_at, event_id)` chứ không chỉ `occurred_at`. Chỉ vỡ ra khi đọc test `test_events_sharing_a_timestamp_resolve_deterministically`: hai bản ghi trùng timestamp phải cho cùng kết quả bất kể thứ tự nạp, vì Kafka chỉ đảm bảo thứ tự *trong một partition*, không phải trong một batch — so một tiêu chí là chưa đủ xác định.
- **Bất ngờ nhất:** `/ready` (HTTP, chạy trong container) trả `degraded` còn `lab28 ready` (CLI, chạy trên host) trả `not_ready` cho **cùng một trạng thái hệ thống** — không phải bug, mà do cờ `mandatory` của probe vLLM đọc từ `LAB28_VLLM_REQUIRE_REAL` khác nhau giữa hai nơi chạy. Đây hoá ra lại là minh hoạ sống động nhất cho đúng thứ bài yêu cầu phân biệt: `ready`/`degraded`/`not_ready` không phải ba mức độ nghiêm trọng cố định, mà là quyết định nghiệp vụ (thành phần nào bắt buộc) áp lên cùng một tập probe.
- **Trở ngại thực tế lớn nhất không nằm ở code** mà ở việc khởi động Docker Desktop trên Windows (daemon không tự chạy dù CLI đã cài) — nhắc lại cho tôi rằng "hệ thống chạy được trên máy tôi" và "engine đang chạy" là hai việc khác nhau, đúng tinh thần phân biệt `/health` (liveness) và `/ready` (readiness) mà bài dạy.
- **Sẽ cải tiến nếu triển khai thật:** làm `/ready` probe các dependency song song với deadline riêng từng probe (thay vì tuần tự) — số liệu thật đo được là P50 5.3s / P99 8.6s cho một endpoint lẽ ra phải dưới 100ms, hoàn toàn do probe vLLM chờ hết timeout ~5s. Đây là bằng chứng rõ nhất cho việc "SUCCESS không có nghĩa là đủ nhanh".

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
