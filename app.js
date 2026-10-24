/**
 * Fanotech / Ququa — Guest Login Bot (Socket.IO) — Heavy Load
 * ورود مهمان + WebRTC + آپلود فایل (چت غیرفعال)
 *
 * نصب:
 *   npm install socket.io-client
 *
 * اجرا (دستی، تعاملی):
 *   node --max-old-space-size=8192 app.js
 *
 * اجرا (CI / غیرتعاملی):
 *   node --max-old-space-size=8192 app.js <count> <holdSeconds>
 *   مثال: node app.js 5000 300
 */

const { io } = require('socket.io-client');
const readline = require('readline');
const { randomUUID } = require('crypto');
const http = require('http');
const fs = require('fs');

// ============================================================
// پیکربندی
// ============================================================
const SERVER = 'https://ws214.fanotech.ir';
const SOCKET_PATH = '/socket/socket/';
const ROOM_CODE = 'ququa';
const ORIGIN = 'https://conference.fanotech.ir';

const USER_AGENT =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
  '(KHTML, like Gecko) Chrome/156.0.0.0 Safari/537.36';

const MAX_USERS = 10000;
const CONCURRENCY = 500;              // ۵۰۰ اتصال موازی
const CONNECT_TIMEOUT_MS = 25000;
const DEFAULT_HOLD_MS = 300000;       // ۵ دقیقه
const JITTER_MS = 15000;              // قطع‌شدن‌ها پخش شوند

// WebRTC
const WEBRTC_ENABLED = true;
const WEBRTC_DTLS = true;

// چت (غیرفعال — برای فعال‌سازی CHAT_ENABLED = true کنید)
const CHAT_ENABLED = false;
const CHAT_INTERVAL_MS = 30000;
const CHAT_MESSAGES = [
  'سلام', 'درود', 'خوبید؟', 'من هستم', 'تست', 'کسی هست؟',
  'لطفاً', 'ممنون', 'خداحافظ', '👍', '🙂', 'این یک پیام تستی است'
];

// آپلود فایل
const UPLOAD_ENABLED = true;
const UPLOAD_ON_JOIN = true;
const UPLOAD_SIZE_BYTES = 10 * 1024;  // ۱۰KB
const UPLOAD_TMP = '/tmp/ququa_upload.bin';

// ============================================================
// اسم و فامیل ایرانی (۱۰۰ × ۱۰۰ = ۱۰٬۰۰۰ ترکیب)
// ============================================================
const FIRST_NAMES = [
  'علی','رضا','محمد','حسین','مهدی','امیر','سعید','احمد','حمید','حسن',
  'مرتضی','مجید','کاظم','یوسف','ابراهیم','ناصر','فرهاد','بهرام','کیوان','بابک',
  'سینا','آرش','آرمان','پارسا','پرهام','پویا','کیان','نیما','سامان','شاهین',
  'فرزاد','مسعود','میلاد','وحید','هادی','امید','بهنام','داریوش','رامین','شهرام',
  'زهرا','فاطمه','مریم','نرگس','سارا','مینا','الهام','سمیرا','نازنین','شیوا',
  'لیلا','پریسا','مهسا','نیلوفر','هستی','آیدا','آرزو','بهاره','ترانه','ژاله',
  'ریحانه','زینب','سحر','شادی','صدیقه','طاهره','عاطفه','فرشته','کبری','گلناز',
  'محبوبه','مرضیه','معصومه','منیره','ناهید','هدیه','یاسمن','پروانه','دنیا','رعنا',
  'سپیده','شیرین','عارفه','فریبا','فرزانه','لاله','مهتاب','نسترن','نوشین','ونوس',
  'آتوسا','آزاده','آناهیتا','بهناز','پردیس','پرستو','ترنم','حدیث','خاطره','رها'
];

const LAST_NAMES = [
  'محمدی','حسینی','رضایی','علوی','موسوی','کریمی','احمدی','صادقی','جعفری','قاسمی',
  'حیدری','اکبری','نیک‌نام','شریفی','کاظمی','مرادی','فتحی','رحیمی','سلیمانی','امینی',
  'بهرامی','پورمحمد','تهرانی','ثابتی','جلیلی','چراغی','حبیبی','خسروی','داوودی','ذبیحی',
  'رستمی','زمانی','سعیدی','شاه‌محمدی','صالحی','طاهری','عباسی','غفاری','فرهادی','قربانی',
  'کوهی','گودرزی','لطفی','محمودی','نوری','وحیدی','هاشمی','یزدانی','زندی','بابایی',
  'پناهی','جهانگیری','حجازی','خانی','دادگر','رهبری','زرین','سبحانی','شکوهی','صفری',
  'عزیزی','غنی','فاضلی','قنبری','کاشانی','گرامی','مظاهری','نجفی','هوشیار','یاری',
  'اسدی','بشیری','پاکدل','تبریزی','جوانمرد','حکیمی','خوش‌نیت','درخشان','رادمنش','زاهدی',
  'سرمدی','شمس','صابری','طالبی','عطایی','فروغی','قادری','کرمی','میرزایی','نصیری',
  'وردی','هژبری','یعقوبی','امیرخانی','برومند','پیروز','توکلی','جهانی','چگینی','حیدرزاده'
];

// ============================================================
// ابزارها
// ============================================================
const httpAgent = new http.Agent({
  keepAlive: true,
  maxSockets: CONCURRENCY + 50,
  maxFreeSockets: 50
});

function randomFullName() {
  const f = FIRST_NAMES[Math.floor(Math.random() * FIRST_NAMES.length)];
  const l = LAST_NAMES[Math.floor(Math.random() * LAST_NAMES.length)];
  return `${f} ${l}`;
}

function generateUniqueNames(count) {
  const target = Math.min(count, FIRST_NAMES.length * LAST_NAMES.length);
  const set = new Set();
  let guard = 0;
  while (set.size < target && guard < target * 50) {
    set.add(randomFullName());
    guard++;
  }
  while (set.size < target) set.add(`${randomFullName()} ${set.size}`);
  return Array.from(set);
}

function randomUserId() {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
  let s = 'm';
  for (let i = 0; i < 18; i++) s += chars[Math.floor(Math.random() * chars.length)];
  return s;
}

function randomUuid() {
  try { return randomUUID(); } catch { return randomUserId() + randomUserId(); }
}

function ask(q) {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  return new Promise(resolve => rl.question(q, a => { rl.close(); resolve(a); }));
}

// ============================================================
// ورودی: اول از argv، بعد از stdin (برای اجرای دستی)
// ============================================================
async function getInputs() {
  const argvCount = parseInt(process.argv[2], 10);
  const argvHold  = parseInt(process.argv[3], 10);

  let count, holdSec;

  if (Number.isFinite(argvCount) && argvCount >= 1) {
    count = Math.min(argvCount, MAX_USERS);
  } else {
    const raw = await ask(`چند کاربر مهمان وارد شوند؟ (1 تا ${MAX_USERS}): `);
    count = parseInt(String(raw).trim(), 10);
    if (!Number.isFinite(count) || count < 1) {
      console.error('❌ عدد نامعتبر.');
      process.exit(1);
    }
    if (count > MAX_USERS) {
      console.warn(`⚠️  محدود به ${MAX_USERS}.`);
      count = MAX_USERS;
    }
  }

  if (Number.isFinite(argvHold) && argvHold >= 0) {
    holdSec = argvHold;
  } else {
    const raw = await ask(`هر کاربر چند ثانیه در اتاق بماند؟ (Enter = ${DEFAULT_HOLD_MS / 1000}s): `);
    holdSec = parseInt(String(raw).trim(), 10);
    if (!Number.isFinite(holdSec) || holdSec < 0) holdSec = DEFAULT_HOLD_MS / 1000;
  }

  return { count, holdSec };
}

// ============================================================
// فایل آپلود تستی (یک‌بار ساخته می‌شود، بین همه سوکت‌ها share)
// ============================================================
function ensureUploadFile() {
  if (!UPLOAD_ENABLED) return null;
  try {
    if (!fs.existsSync(UPLOAD_TMP) || fs.statSync(UPLOAD_TMP).size !== UPLOAD_SIZE_BYTES) {
      fs.writeFileSync(UPLOAD_TMP, Buffer.alloc(UPLOAD_SIZE_BYTES, 0x41));
    }
    return fs.readFileSync(UPLOAD_TMP);
  } catch (e) {
    console.error('upload file error:', e.message);
    return null;
  }
}
const UPLOAD_BUFFER = ensureUploadFile();

// ============================================================
// ورود یک مهمان + WebRTC + Upload (+ Chat اگر فعال)
// ============================================================
function joinAsGuest(name, holdMs) {
  return new Promise((resolve) => {
    const userId = randomUserId();
    const socket = io(SERVER, {
      path: SOCKET_PATH,
      transports: ['websocket'],
      reconnection: false,
      timeout: CONNECT_TIMEOUT_MS,
      agent: httpAgent,
      extraHeaders: {
        Origin: ORIGIN,
        'User-Agent': USER_AGENT,
        'Accept-Language': 'fa-IR,fa;q=0.9,en;q=0.8'
      },
      transportOptions: {
        websocket: {
          extraHeaders: {
            Origin: ORIGIN,
            'User-Agent': USER_AGENT
          }
        }
      }
    });

    let done = false;
    let chatTimer = null;
    let rtcTransportId = null;

    const cleanup = () => {
      if (chatTimer) { clearInterval(chatTimer); chatTimer = null; }
      try { socket.disconnect(); } catch {}
    };

    const finish = (r) => {
      if (done) return;
      done = true;
      clearTimeout(hardTimeout);
      resolve(r);
    };

    const hardTimeout = setTimeout(() => {
      cleanup();
      finish({ ok: false, error: 'timeout' });
    }, CONNECT_TIMEOUT_MS + 5000);

    socket.on('connect_error', (e) => {
      cleanup();
      finish({ ok: false, error: 'connect: ' + (e && e.message ? e.message : String(e)) });
    });

    socket.on('connect', () => {
      // ۱) getUserInfo
      socket.emit('getUserInfo', { token: '', roomCode: ROOM_CODE }, (info) => {
        if (!info || info.success !== true) {
          cleanup();
          return finish({ ok: false, error: 'getUserInfo failed' });
        }
        if (info.guestReception !== true) {
          cleanup();
          return finish({ ok: false, error: 'guestReception=false' });
        }

        // ۲) joinRoom
        socket.emit('joinRoom', {
          name,
          token: '',
          roomCode: ROOM_CODE,
          userID: userId,
          deviceId: userId,
          tabId: randomUuid(),
          BDInfo: {
            browser: { name: 'Chrome', version: '156.0.0.0' },
            os: { name: 'Windows', version: 'NT 10.0', versionName: '10' },
            platform: { type: 'desktop' },
            engine: { name: 'Blink' }
          },
          version: '21.0.0',
          mversion: '3.15.6',
          iAmRobot: false,
          avatar: ''
        });
      });

      // ۳) تأیید ورود
      socket.on('joinedRoom?', (data) => {
        clearTimeout(hardTimeout);

        // --- WebRTC: ساخت transport ---
        if (WEBRTC_ENABLED) {
          try {
            socket.emit(
              'createWebRtcTransport',
              {
                forceTcp: false,
                rtpCapabilities: { codecs: [] },
                dtls: WEBRTC_DTLS,
                sctp: false
              },
              (resp) => {
                if (resp && resp.id) rtcTransportId = resp.id;
              }
            );
          } catch (e) {}
        }

        // --- آپلود فایل بعد از ورود ---
        if (UPLOAD_ENABLED && UPLOAD_ON_JOIN && UPLOAD_BUFFER) {
          try {
            socket.emit(
              'uploadFile',
              {
                name: `test_${userId}.bin`,
                size: UPLOAD_BUFFER.length,
                mime: 'application/octet-stream',
                data: UPLOAD_BUFFER,
                roomCode: ROOM_CODE,
                userID: userId
              },
              () => {}
            );
          } catch {}
        }

        // --- چت دوره‌ای (فقط اگر فعال باشد) ---
        if (CHAT_ENABLED) {
          chatTimer = setInterval(() => {
            try {
              const text = CHAT_MESSAGES[Math.floor(Math.random() * CHAT_MESSAGES.length)];
              socket.emit('sendMessage', {
                message: text,
                roomCode: ROOM_CODE,
                userID: userId,
                name,
                timestamp: Date.now(),
                type: 'text'
              });
            } catch {}
          }, CHAT_INTERVAL_MS + Math.floor(Math.random() * 5000));
        }

        finish({
          ok: true,
          name,
          userId,
          roomName: data && data.roomName,
          allUsers: data && data.allUsers,
          socket
        });

        // --- نگه‌داری با jitter ---
        const wait = holdMs > 0 ? holdMs + Math.floor(Math.random() * JITTER_MS) : 1500;
        setTimeout(() => {
          if (rtcTransportId) {
            try { socket.emit('closeWebRtcTransport', { transportId: rtcTransportId }); } catch {}
          }
          cleanup();
        }, wait);
      });
    });
  });
}

// ============================================================
// اجرای موازی محدود
// ============================================================
async function runPool(names, holdMs, concurrency) {
  const results = { ok: 0, fail: 0, errors: {} };
  let index = 0;

  async function worker() {
    while (true) {
      const i = index++;
      if (i >= names.length) return;
      const name = names[i];
      const r = await joinAsGuest(name, holdMs);
      if (r.ok) {
        results.ok++;
        console.log(`[${String(i + 1).padStart(5, ' ')}] ✅ "${name}" → وارد شد (آنلاین: ${r.allUsers ?? '?'})`);
      } else {
        results.fail++;
        const key = String(r.error || 'unknown');
        results.errors[key] = (results.errors[key] || 0) + 1;
        console.log(`[${String(i + 1).padStart(5, ' ')}] ❌ "${name}" → ${key}`);
      }
    }
  }

  const workers = [];
  for (let i = 0; i < Math.min(concurrency, names.length); i++) workers.push(worker());
  await Promise.all(workers);
  return results;
}

// ============================================================
// اجرای اصلی
// ============================================================
(async () => {
  console.log('══════════════════════════════════════════════════════');
  console.log('  Fanotech / Ququa — Guest Bot (Heavy: WebRTC+Upload)');
  console.log('══════════════════════════════════════════════════════');
  console.log(`  SERVER      : ${SERVER}${SOCKET_PATH}`);
  console.log(`  ROOM        : ${ROOM_CODE}`);
  console.log(`  CONCURRENCY : ${CONCURRENCY}`);
  console.log(`  HOLD        : ${DEFAULT_HOLD_MS / 1000}s (+jitter ${JITTER_MS / 1000}s)`);
  console.log(`  WebRTC      : ${WEBRTC_ENABLED} (DTLS=${WEBRTC_DTLS})`);
  console.log(`  Chat        : ${CHAT_ENABLED}`);
  console.log(`  Upload      : ${UPLOAD_ENABLED} (${UPLOAD_SIZE_BYTES} bytes/user)`);
  console.log('══════════════════════════════════════════════════════\n');

  const { count, holdSec } = await getInputs();

  console.log(`\n🚀 ورود ${count} کاربر (همزمانی: ${CONCURRENCY}، نگه‌داری: ${holdSec}s)...\n`);

  const names = generateUniqueNames(count);
  const started = Date.now();
  const results = await runPool(names, holdSec * 1000, CONCURRENCY);
  const elapsed = ((Date.now() - started) / 1000).toFixed(1);

  console.log('\n═════════════ خلاصه ═════════════');
  console.log(`✅ موفق:    ${results.ok}`);
  console.log(`❌ ناموفق:  ${results.fail}`);
  console.log(`📊 مجموع:   ${names.length}`);
  console.log(`⏱️  زمان:   ${elapsed} ثانیه`);
  if (results.fail > 0) {
    console.log('─── تفکیک خطاها ───');
    for (const [k, v] of Object.entries(results.errors)) {
      console.log(`   ${v.toString().padStart(5, ' ')} × ${k}`);
    }
  }
  console.log('════════════════════════════════════');

  // اگر هیچ اتصالی موفق نشد، job را قرمز کن تا علت پنهان نماند
  if (results.ok === 0) {
    console.error('\n❌ هیچ اتصالی موفق نشد — احتمالاً IP دیتاسنتر (GitHub Actions) توسط سرور بلاک شده است.');
    console.error('   برای تست: روی یک VPS با IP اختصاصی اجرا کن، یا step تشخیصی curl را ببین.');
    process.exit(2);
  }

  process.exit(0);
})().catch((e) => {
  console.error('💥 Fatal:', e);
  process.exit(1);
});
