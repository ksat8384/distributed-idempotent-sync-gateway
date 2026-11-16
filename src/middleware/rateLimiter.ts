import { Request, Response, NextFunction } from "express";
import Redis from "ioredis";

const redis = new Redis(process.env.REDIS_URL || "redis://localhost:6379");

/**
 * Atomic Redis Lua Script for Token-Bucket Rate Limiting
 * Executes in a single Redis event-loop tick to prevent race conditions
 * across horizontally scaled API nodes.
 *
 * KEYS[1] = bucket key (e.g., "ratelimit:device:scanner-42")
 * ARGV[1] = bucket capacity (max burst tokens)
 * ARGV[2] = refill rate (tokens per second)
 * ARGV[3] = current unix timestamp in seconds
 * ARGV[4] = tokens requested (default 1)
 */
const TOKEN_BUCKET_LUA = `
  local key = KEYS[1]
  local capacity = tonumber(ARGV[1])
  local refill_rate = tonumber(ARGV[2])
  local now = tonumber(ARGV[3])
  local requested = tonumber(ARGV[4])

  local bucket = redis.call("HMGET", key, "tokens", "last_refill")
  local tokens = tonumber(bucket[1])
  local last_refill = tonumber(bucket[2])

  if not tokens then
    tokens = capacity
    last_refill = now
  else
    local elapsed = math.max(0, now - last_refill)
    local refilled = elapsed * refill_rate
    tokens = math.min(capacity, tokens + refilled)
    last_refill = now
  end

  local allowed = 0
  local retry_after = 0

  if tokens >= requested then
    tokens = tokens - requested
    allowed = 1
  else
    local deficit = requested - tokens
    retry_after = math.ceil(deficit / refill_rate)
  end

  redis.call("HMSET", key, "tokens", tokens, "last_refill", last_refill)
  redis.call("EXPIRE", key, math.ceil(capacity / refill_rate) * 2)

  return { allowed, math.floor(tokens), retry_after }
`;

interface RateLimiterOptions {
  capacity: number; // Max burst allowed when offline queue reconnects (e.g., 20 requests)
  refillRate: number; // Sustained requests per second per device (e.g., 5 req/sec)
}

/**
 * Distributed Token-Bucket Rate Limiter Middleware
 * Protects downstream databases from thundering-herd reconnection spikes
 * while allowing controlled burst drains from offline mobile clients.
 */
export function distributedRateLimiter(options: RateLimiterOptions) {
  const { capacity, refillRate } = options;

  return async (
    req: Request,
    res: Response,
    next: NextFunction
  ): Promise<void> => {
    const clientId = req.header("X-Device-Id") || req.ip || "anonymous";
    const bucketKey = `ratelimit:device:${clientId}`;
    const nowSeconds = Math.floor(Date.now() / 1000);

    try {
      const result = (await redis.eval(
        TOKEN_BUCKET_LUA,
        1,
        bucketKey,
        capacity,
        refillRate,
        nowSeconds,
        1
      )) as [number, number, number];

      const [allowed, remainingTokens, retryAfter] = result;

      res.setHeader("X-RateLimit-Limit", capacity.toString());
      res.setHeader("X-RateLimit-Remaining", remainingTokens.toString());

      if (allowed === 0) {
        // Add jitter hint so mobile clients do not retry in lockstep
        const jitterSeconds = Math.floor(Math.random() * 3);
        const backoffSeconds = retryAfter + jitterSeconds;

        res.setHeader("Retry-After", backoffSeconds.toString());
        res.status(429).json({
          error:
            "Rate limit exceeded during sync burst. Apply exponential backoff with jitter.",
          retryAfterSeconds: backoffSeconds,
        });
        return;
      }

      next();
    } catch (err) {
      console.error("Rate limiter Redis error:", err);
      // Fail-open on rate limiter outage so legitimate warehouse traffic is not blocked
      next();
    }
  };
}
