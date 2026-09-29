import axios from "axios";
import { EventSource } from "eventsource";

const NOXXY_BASE = process.env.NOXXY_BASE_URL || "https://am.noxxyrorr.biz.id";
const TEMPMAIL_BASE = process.env.TEMPMAIL_BASE_URL || "https://noxxyrorr.biz.id";

// Logging internal sengaja dinonaktifkan. Fungsi ini dipertahankan agar alur
// yang sudah ada tetap sederhana tanpa mencetak apa pun ke terminal.
function log() {}

function errorInfo(err) {
  return {
    message: err?.message || String(err),
    code: err?.code,
    status: err?.response?.status,
    response: err?.response?.data
  };
}

function maskUrl(url) {
  if (!url) return null;
  try {
    const parsed = new URL(url);
    return `${parsed.origin} (detail link disembunyikan)`;
  } catch {
    return "URL tidak valid";
  }
}

function extractLinks(text) {
  if (!text) return [];
  return text.match(/https?:\/\/[^\s"'<>]+/g) || [];
}

const HEADERS = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  "Origin": NOXXY_BASE,
  "Referer": `${NOXXY_BASE}/`,
  "Content-Type": "application/json"
};

let emailCounter = 0;
let generationCounter = 0;

// Endpoint temporary-mail membatasi jumlah inbox aktif dari satu IP. Jangan
// mengirim POST secara paralel dan jangan terus-menerus menembak endpoint saat
// server sudah menyatakan kuota IP habis.
const TEMPMAIL_MAX_RETRIES = Math.max(
  0,
  Number.parseInt(process.env.TEMPMAIL_MAX_RETRIES || "2", 10) || 0
);
const TEMPMAIL_RETRY_BASE_MS = Math.max(
  1000,
  Number.parseInt(process.env.TEMPMAIL_RETRY_BASE_MS || "5000", 10) || 5000
);
const TEMPMAIL_QUOTA_COOLDOWN_MS = Math.max(
  1000,
  Number.parseInt(process.env.TEMPMAIL_QUOTA_COOLDOWN_MS || "30000", 10) || 30000
);

let inboxCreationLock = Promise.resolve();
let tempMailBlockedUntil = 0;
let tempMailBlockedError = null;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function retryAfterMs(response) {
  const value = response?.headers?.get?.("retry-after");
  if (!value) return 0;

  const seconds = Number(value);
  if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000);

  const date = Date.parse(value);
  return Number.isFinite(date) ? Math.max(0, date - Date.now()) : 0;
}

function isInboxQuotaError(response, data) {
  const message = String(data?.error || data?.message || "").toLowerCase();
  return response?.status === 429 ||
    message.includes("too many inbox") ||
    message.includes("inboxes from this ip") ||
    message.includes("rate limit");
}

function quotaError(data, response) {
  const original = data?.error || data?.message || "Too many inboxes from this IP";
  const err = new Error(
    `Gagal membuat email temporary: ${original}. ` +
    "Kuota inbox dari IP ini sudah penuh; tunggu inbox lama kedaluwarsa " +
    "atau gunakan provider/IP yang memang diizinkan oleh layanan."
  );
  err.code = "TEMPMAIL_IP_LIMIT";
  err.status = response?.status;
  err.retryAfterMs = retryAfterMs(response) || TEMPMAIL_QUOTA_COOLDOWN_MS;
  return err;
}

async function withInboxCreationLock(task) {
  const previous = inboxCreationLock;
  let release;
  inboxCreationLock = new Promise(resolve => {
    release = resolve;
  });

  await previous;
  try {
    return await task();
  } finally {
    release();
  }
}

async function createTempMail() {
  return withInboxCreationLock(async () => {
    if (tempMailBlockedUntil > Date.now() && tempMailBlockedError) {
      throw tempMailBlockedError;
    }

    for (let attempt = 0; attempt <= TEMPMAIL_MAX_RETRIES; attempt++) {
      log("INFO", "Meminta inbox temporary baru", {
        url: `${TEMPMAIL_BASE}/api/inbox`,
        attempt: attempt + 1
      });

      let res;
      let data;

      try {
        res = await fetch(`${TEMPMAIL_BASE}/api/inbox`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({})
        });
        try {
          data = (await res.json()) || {};
        } catch {
          data = { error: `HTTP ${res.status}` };
        }
      } catch (err) {
        log("ERROR", "Request pembuatan inbox gagal", errorInfo(err));
        throw err;
      }

      if (res.ok && data.success && data.inbox?.address) {
        tempMailBlockedUntil = 0;
        tempMailBlockedError = null;
        emailCounter++;

        log("OK", `Inbox temporary #${emailCounter} berhasil dibuat`, {
          email: data.inbox.address,
          expiresAt: data.inbox.expiresAt,
          sessionId: data.inbox.sessionId
        });

        return {
          email: data.inbox.address,
          expiresAt: data.inbox.expiresAt
        };
      }

      if (isInboxQuotaError(res, data)) {
        const err = quotaError(data, res);
        const waitMs = retryAfterMs(res) || TEMPMAIL_RETRY_BASE_MS * 2 ** attempt;
        tempMailBlockedUntil = Date.now() + Math.max(waitMs, TEMPMAIL_QUOTA_COOLDOWN_MS);
        tempMailBlockedError = err;

        log("WARN", "Kuota inbox dari IP tercapai", {
          httpStatus: res.status,
          waitMs,
          blockedUntil: new Date(tempMailBlockedUntil).toISOString(),
          response: data
        });

        // Retry hanya jika server memberikan peluang yang masuk akal. Setelah
        // itu, semua worker memakai error yang sama tanpa membuat request baru.
        if (attempt < TEMPMAIL_MAX_RETRIES && retryAfterMs(res) > 0) {
          await sleep(retryAfterMs(res));
          continue;
        }
        throw err;
      }

      const err = new Error(`Gagal membuat email temporary: ${data.error || "unknown error"}`);
      log("ERROR", "Server menolak pembuatan inbox", {
        httpStatus: res.status,
        response: data
      });
      throw err;
    }
  });
}

async function waitInbox(email, timeoutSec = 45) {
  return new Promise((resolve, reject) => {
    const streamUrl = `${TEMPMAIL_BASE}/api/stream?address=${encodeURIComponent(email)}`;
    log("INFO", "Membuka koneksi stream inbox", {
      email,
      timeoutSec,
      url: `${TEMPMAIL_BASE}/api/stream?address=...`
    });

    const stream = new EventSource(
      streamUrl
    );
    let settled = false;

    const finish = (callback, value) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      stream.close();
      log("INFO", "Koneksi stream inbox ditutup", { email });
      callback(value);
    };

    const timeout = setTimeout(
      () => {
        log("WARN", "Timeout menunggu email masuk", { email, timeoutSec });
        finish(resolve, null);
      },
      timeoutSec * 1000
    );

    stream.addEventListener("connected", () => {
      log("OK", "Stream inbox terhubung", { email });
    });

    stream.addEventListener("new_email", event => {
      try {
        const mail = JSON.parse(event.data);
        log("INFO", "Email baru diterima", {
          email,
          from: mail.from?.address || mail.from_address || "unknown",
          subject: mail.subject || "(tanpa subjek)"
        });

        const links = [
          ...extractLinks(mail.text),
          ...extractLinks(mail.html)
        ];
        const magicLink = [...new Set(links)][0];
        log("INFO", "Link pada email dianalisis", {
          totalLinks: [...new Set(links)].length,
          linkDitemukan: Boolean(magicLink),
          preview: maskUrl(magicLink)
        });

        if (magicLink) {
          log("OK", "Magic link berhasil ditemukan", { email });
          finish(resolve, magicLink);
        }
      } catch (err) {
        log("ERROR", "Gagal membaca event email", errorInfo(err));
        finish(reject, err);
      }
    });

    stream.onerror = err => {
      if (!settled) {
        log("ERROR", "Koneksi stream inbox bermasalah", errorInfo(err));
        finish(reject, err);
      }
    };
  });
}

export async function generateAM() {
  const operationId = ++generationCounter;
  const startedAt = Date.now();
  log("INFO", `Proses generate #${operationId} dimulai`);

  let mail;
  try {
    mail = await createTempMail();
  } catch (err) {
    log("ERROR", `Proses #${operationId} gagal saat membuat inbox`, errorInfo(err));
    throw err;
  }

  const email = mail.email;
  log("INFO", `Proses #${operationId} menggunakan email`, {
    email,
    expiresAt: mail.expiresAt
  });

  let sendRes;

  try {
    log("INFO", `Proses #${operationId}: mengirim permintaan magic link`, {
      url: `${NOXXY_BASE}/api/send-link`
    });

    sendRes = await axios.post(
      `${NOXXY_BASE}/api/send-link`,
      { email },
      {
        headers: HEADERS,
        timeout: 20000
      }
    );

    log("INFO", `Proses #${operationId}: respons send-link diterima`, {
      httpStatus: sendRes.status,
      success: sendRes.data?.success,
      message: sendRes.data?.message
    });

  } catch (err) {
    log("ERROR", `Proses #${operationId}: pengiriman magic link gagal`, errorInfo(err));
    throw err;
  }

  if (!sendRes.data?.success) {
    const err = new Error(
      sendRes.data?.message || "Gagal mengirim link verifikasi ke email."
    );
    log("ERROR", `Proses #${operationId}: server gagal mengirim magic link`, {
      response: sendRes.data
    });
    throw err;
  }

  log("OK", `Proses #${operationId}: magic link berhasil diminta`, { email });

  const magicLink = await waitInbox(
    mail.email,
    90
);
  if (!magicLink) {
    const err = new Error(`Timeout menunggu magic link di email ${email}`);
    log("ERROR", `Proses #${operationId}: magic link tidak diterima`, {
      email,
      timeoutSec: 90
    });
    throw err;
  }

  log("OK", `Proses #${operationId}: magic link diterima`, {
    email,
    preview: maskUrl(magicLink)
  });

  let verifRes;

  try {
    log("INFO", `Proses #${operationId}: memverifikasi magic link`, {
      url: `${NOXXY_BASE}/api/verify-link`
    });

    verifRes = await axios.post(
      `${NOXXY_BASE}/api/verify-link`,
      {
        email,
        magicLink
      },
      {
        headers: HEADERS,
        timeout: 25000
      }
    );

    log("INFO", `Proses #${operationId}: respons verify-link diterima`, {
      httpStatus: verifRes.status,
      success: verifRes.data?.success,
      message: verifRes.data?.message
    });

  } catch (err) {
    log("ERROR", `Proses #${operationId}: verifikasi magic link gagal`, errorInfo(err));
    throw err;
  }

  const resData = verifRes.data;

  if (!resData.success) {
    const err = new Error(resData.message || "Verifikasi magic link gagal.");
    log("ERROR", `Proses #${operationId}: server menolak verifikasi`, {
      response: resData
    });
    throw err;
  }

  const user = resData.data || resData.userData || {};
  // Pertahankan tanda @ agar link yang dikembalikan mudah dibaca.
  const encodedAddress = encodeURIComponent(email).replace(/%40/gi, "@");
  const inboxLink = `${TEMPMAIL_BASE}/?address=${encodedAddress}`;

  log("OK", `Proses #${operationId} selesai`, {
    email: user.email || email,
    durationMs: Date.now() - startedAt,
    membership: user.membershipStatus,
    plan: user.planName || user.tier
  });

  return {
    status: true,
    email: user.email || email,
    inbox_link: inboxLink,
    // uid: user.uid || user.id || null,
    membership: user.membershipStatus,
    plan: user.planName || user.tier,
    subscription: user.subscription,
    // order_id: user.orderId || null,
    valid_until: user.validUntil || null,
    // id_token: user.idToken || null,
    // refresh_token: user.refreshToken || null,
    // created_at: user.createdAt || new Date().toISOString()
  };
}

export async function bulkGenerateAM(count = 5, concurrency = 1) {
  const total = parseInt(count) || 5;
  const conc = Math.max(1, parseInt(concurrency) || 1);
  const results = [];
  const errors = [];

  log("INFO", "Bulk generate dimulai", {
    total,
    concurrency: Math.min(conc, total)
  });

  const queue = Array.from({ length: total }, (_, i) => i + 1);

  async function worker() {
    while (queue.length > 0) {
      const idx = queue.shift();
      log("INFO", `Worker mengambil pekerjaan #${idx}`, {
        tersisa: queue.length
      });

      try {
        const acc = await generateAM();
        results.push(acc);
        log("OK", `Pekerjaan #${idx} berhasil`, {
          email: acc.email,
          progress: `${results.length}/${total}`
        });
      } catch (err) {
        const message = err?.message || String(err);
        errors.push({ index: idx, error: message });
        log("ERROR", `Pekerjaan #${idx} gagal`, {
          error: message,
          progress: `${results.length + errors.length}/${total}`
        });
      }
    }
  }

  const workers = Array.from({ length: Math.min(conc, total) }, () => worker());
  await Promise.all(workers);

  log("INFO", "Bulk generate selesai", {
    requested: total,
    success: results.length,
    failed: errors.length
  });

  return {
    status: true,
    service: "AM Bulk Generator",
    total_requested: total,
    total_success: results.length,
    total_failed: errors.length,
    accounts: results,
    errors: errors.length ? errors : undefined
  };
}

async function main() {
  const args = process.argv.slice(2);
  const minified = args.includes("--min");
  const filtered = args.filter(a => a !== "--min");

  if (args.includes("--help") || args.includes("-h")) {
    process.stdout.write(`${JSON.stringify({
      status: true,
      service: "AM Bulk Generator (noxxyrorr)",
      usage: "node fixbulk.js [count] [concurrency]",
      example: "node fixbulk.js 5 1"
    }, null, 2)}\n`);
    process.exit(0);
  }

  const count = parseInt(filtered[0]) || 1;
  const concurrency = Math.max(1, parseInt(filtered[1]) || 1);

  log("INFO", "Program dimulai", {
    count,
    concurrency,
    output: minified ? "minified" : "pretty"
  });

  try {
    let result = null;
    if (count === 1) {
      result = await generateAM();
    }
    if (count > 1) {
      result = await bulkGenerateAM(count, concurrency);
    }

    log("OK", "Program selesai menghasilkan output", {
      success: result?.status,
      totalSuccess: result?.total_success ?? (result?.status ? 1 : 0),
      totalFailed: result?.total_failed ?? 0
    });
    process.stdout.write(`${minified ? JSON.stringify(result) : JSON.stringify(result, null, 2)}\n`);
  } catch (err) {
    log("ERROR", "Program berhenti karena error", errorInfo(err));
    process.exit(1);
  }
}

if (process.argv[1] && process.argv[1].endsWith("fixbulk.js")) {
  main();
}
