package com.taskscheduler.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Executes tasks based on their type.
 *
 * Supported task types (add your own real implementations here):
 *   - DATA_PROCESSING  : simulates data crunching
 *   - EMAIL_SEND       : simulates sending an email
 *   - IMAGE_RESIZE     : simulates image processing
 *   - HTTP_CALL        : simulates an outbound HTTP request
 *   - COMPUTE          : simulates CPU-bound computation
 *   - (default)        : generic sleep-based simulation
 */
public class TaskExecutor {

    private static final Logger log = LoggerFactory.getLogger(TaskExecutor.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private final Random       random = new Random();

    /**
     * Execute a task.
     *
     * @param taskType   type identifier string
     * @param payload    JSON string with task parameters
     * @param timeoutMs  max execution time in ms (throw if exceeded)
     * @return           JSON result string
     */
    public String execute(String taskType, String payload, long timeoutMs) throws Exception {
        log.debug("Executing taskType={} payload={}", taskType, payload);

        JsonNode params = parsePayload(payload);

        return switch (taskType.toUpperCase()) {
            case "DATA_PROCESSING" -> executeDataProcessing(params, timeoutMs);
            case "EMAIL_SEND"      -> executeEmailSend(params);
            case "IMAGE_RESIZE"    -> executeImageResize(params, timeoutMs);
            case "HTTP_CALL"       -> executeHttpCall(params, timeoutMs);
            case "COMPUTE"         -> executeCompute(params, timeoutMs);
            default                -> executeGeneric(params, timeoutMs);
        };
    }

    // ── Task implementations ──────────────────────────────────────────────

    private String executeDataProcessing(JsonNode params, long timeoutMs) throws Exception {
        int records = params.path("records").asInt(1000);
        log.debug("Processing {} records", records);

        // Simulate processing time proportional to record count
        long processingMs = Math.min(records / 10L, timeoutMs - 100);
        sleep(processingMs);

        // Simulate occasional failures (5% failure rate)
        if (random.nextInt(20) == 0) throw new RuntimeException("Data validation error at record 847");

        return mapper.createObjectNode()
                .put("processed", records)
                .put("valid", records - random.nextInt(records / 100 + 1))
                .put("failed", random.nextInt(records / 100 + 1))
                .put("processingMs", processingMs)
                .toString();
    }

    private String executeEmailSend(JsonNode params) throws Exception {
        String to      = params.path("to").asText("user@example.com");
        String subject = params.path("subject").asText("Notification");

        sleep(100 + random.nextInt(200)); // SMTP latency simulation

        // 2% failure rate (SMTP errors)
        if (random.nextInt(50) == 0) throw new RuntimeException("SMTP connection refused");

        return mapper.createObjectNode()
                .put("to", to)
                .put("subject", subject)
                .put("messageId", "msg-" + System.currentTimeMillis())
                .put("delivered", true)
                .toString();
    }

    private String executeImageResize(JsonNode params, long timeoutMs) throws Exception {
        int width  = params.path("width").asInt(800);
        int height = params.path("height").asInt(600);
        String fmt = params.path("format").asText("JPEG");

        long processingMs = Math.min(500L + random.nextInt(1000), timeoutMs - 100);
        sleep(processingMs);

        return mapper.createObjectNode()
                .put("originalSize", "4096x3072")
                .put("targetSize", width + "x" + height)
                .put("format", fmt)
                .put("outputPath", "/tmp/resized-" + System.currentTimeMillis() + ".jpg")
                .put("processingMs", processingMs)
                .toString();
    }

    private String executeHttpCall(JsonNode params, long timeoutMs) throws Exception {
        String url    = params.path("url").asText("https://api.example.com/data");
        String method = params.path("method").asText("GET");

        long latency = Math.min(200L + random.nextInt(800), timeoutMs - 100);
        sleep(latency);

        // 3% failure rate (network errors)
        if (random.nextInt(33) == 0) throw new RuntimeException("Connection timeout to " + url);

        return mapper.createObjectNode()
                .put("url", url)
                .put("method", method)
                .put("statusCode", 200)
                .put("latencyMs", latency)
                .put("responseSize", 1024 + random.nextInt(10240))
                .toString();
    }

    private String executeCompute(JsonNode params, long timeoutMs) throws Exception {
        int iterations = params.path("iterations").asInt(1_000_000);

        // Actually do CPU work (e.g., prime sieve)
        long start = System.currentTimeMillis();
        int count  = countPrimes(Math.min(iterations, 500_000));
        long elapsed = System.currentTimeMillis() - start;

        if (elapsed > timeoutMs) throw new RuntimeException("Compute task exceeded timeout");

        return mapper.createObjectNode()
                .put("iterations", iterations)
                .put("primesFound", count)
                .put("computeMs", elapsed)
                .toString();
    }

    private String executeGeneric(JsonNode params, long timeoutMs) throws Exception {
        long durationMs = Math.min(
                params.path("durationMs").asLong(500),
                timeoutMs - 100
        );
        sleep(durationMs);

        return mapper.createObjectNode()
                .put("status", "ok")
                .put("durationMs", durationMs)
                .put("workerId", System.getenv().getOrDefault("WORKER_ID", "unknown"))
                .toString();
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private JsonNode parsePayload(String payload) {
        try {
            return mapper.readTree(payload);
        } catch (Exception e) {
            return mapper.createObjectNode();
        }
    }

    private void sleep(long ms) throws InterruptedException {
        if (ms > 0) TimeUnit.MILLISECONDS.sleep(ms);
    }

    /** Simple prime counting (Sieve of Eratosthenes). */
    private int countPrimes(int n) {
        if (n < 2) return 0;
        boolean[] sieve = new boolean[n + 1];
        java.util.Arrays.fill(sieve, true);
        sieve[0] = sieve[1] = false;
        for (int i = 2; i * i <= n; i++) {
            if (sieve[i]) {
                for (int j = i * i; j <= n; j += i) sieve[j] = false;
            }
        }
        int count = 0;
        for (boolean b : sieve) if (b) count++;
        return count;
    }
}
