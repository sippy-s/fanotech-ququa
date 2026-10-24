/**
 * Fanotech / Ququa — Guest Login Bot (Socket.IO)
 * ورود مستقیم به اتاق از طریق سرور Socket.IO، بدون مرورگر
 *
 * نصب:
 *   npm install socket.io-client
 * اجرا:
 *   node app.js
 */

const { io } = require('socket.io-client');
const readline = require('readline');
const { randomUUID } = require('crypto');

// ============================================================
// پیکربندی
// ============================================================
const SERVER = 'https://ws214.fanotech.ir';
const SOCKET_PATH = '/socket/socket/';
const ROOM_CODE = 'ququa';
const ORIGIN = 'https://conference.fanotech.ir';

const MAX_USERS = 10000;
const CONCURRENCY = 20;           // تعداد اتصال‌های همزمان
const CONNECT_TIMEOUT_MS = 25000; // تایم‌اوت هر اتصال

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

// userID و deviceId: ۱۹ کاراکتر با حروف کوچک و اعداد (شبیه شناسه‌های واقعی Fanotech)
function randomUserId() {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
  let s = 'm';
  for (let i = 0; i < 18; i++) s += chars[Math.floor(Math.random() * chars.length)];
  return s;
}

function ask(q) {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  return new Promise(resolve => rl.question(q, a => { rl.close(); resolve(a); }));
}

// ============================================================
// ورود یک مهمان
// ============================================================
function joinAsGuest(name, holdMs) {
  return new Promise((resolve) => {
    const userId = randomUserId();
    const socket = io(SERVER, {
      path: SOCKET_PATH,
      transports: ['websocket'],
      reconnection: false,
      timeout: CONNECT_TIMEOUT_MS,
      extraHeaders: { Origin: ORIGIN },
      transportOptions: {
        websocket: { extraHeaders: { Origin: ORIGIN } }
      }
    });

    let done = false;
    const finish = (r) => { if (!done) { done = true; resolve(r); } };

    const timeout = setTimeout(() => {
      try { socket.disconnect(); } catch {}
      finish({ ok: false, error: 'timeout' });
    }, CONNECT_TIMEOUT_MS + 5000);

    socket.on('connect_error', (e) => {
      clearTimeout(timeout);
      try { socket.disconnect(); } catch {}
      finish({ ok: false, error: 'connect: ' + e.message });
    });

    socket.on('connect', () => {
      // ۱) getUserInfo با ack
      socket.emit('getUserInfo', { token: '', roomCode: ROOM_CODE }, (info) => {
        if (!info || info.success !== true) {
          clearTimeout(timeout);
          try { socket.disconnect(); } catch {}
          return finish({ ok: false, error: 'getUserInfo failed' });
        }
        if (info.guestReception !== true) {
          clearTimeout(timeout);
          try { socket.disconnect(); } catch {}
          return finish({ ok: false, error: 'guestReception=false' });
        }

        // ۲) joinRoom
        socket.emit('joinRoom', {
          name,
          token: '',
          roomCode: ROOM_CODE,
          userID: userId,
          deviceId: userId,
          tabId: randomUUID(),
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

      // ۳) منتظر تأیید ورود
      socket.on('joinedRoom?', (data) => {
        clearTimeout(timeout);
        finish({
          ok: true,
          name,
          userId,
          roomName: data && data.roomName,
          allUsers: data && data.allUsers,
          socket
        });
        // نگه داشتن یا خارج شدن
        const wait = holdMs > 0 ? holdMs : 1500;
        setTimeout(() => {
          try { socket.disconnect(); } catch {}
        }, wait);
      });
    });
  });
}

// ============================================================
// اجرای موازی محدود
// ============================================================
async function runPool(names, holdMs, concurrency) {
  const results = { ok: 0, fail: 0 };
  let index = 0;

  async function worker() {
    while (true) {
      const i = index++;
      if (i >= names.length) return;
      const name = names[i];
      const r = await joinAsGuest(name, holdMs);
      if (r.ok) {
        results.ok++;
        console.log(`[${String(i + 1).padStart(4, ' ')}] ✅ "${name}" → وارد شد (آنلاین: ${r.allUsers ?? '?'})`);
      } else {
        results.fail++;
        console.log(`[${String(i + 1).padStart(4, ' ')}] ❌ "${name}" → ${r.error}`);
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
  console.log('══════════════════════════════════════════════');
  console.log('  Fanotech / Ququa — Guest Login Bot (Socket.IO)');
  console.log('══════════════════════════════════════════════\n');

  const rawCount = await ask(`چند کاربر مهمان وارد شوند؟ (1 تا ${MAX_USERS}): `);
  let count = parseInt(String(rawCount).trim(), 10);
  if (!Number.isFinite(count) || count < 1) { console.error('❌ عدد نامعتبر.'); process.exit(1); }
  if (count > MAX_USERS) { console.warn(`⚠️  محدود به ${MAX_USERS}.`); count = MAX_USERS; }

  const rawHold = await ask('هر کاربر چند ثانیه در اتاق بماند؟ (0 = ورود و خروج فوری): ');
  let holdSec = parseInt(String(rawHold).trim(), 10);
  if (!Number.isFinite(holdSec) || holdSec < 0) holdSec = 0;

  console.log(`\n🚀 ورود ${count} کاربر (همزمانی: ${CONCURRENCY}${holdSec > 0 ? `، نگه‌داری: ${holdSec}s` : ''})...\n`);

  const names = generateUniqueNames(count);
  const started = Date.now();
  const results = await runPool(names, holdSec * 1000, CONCURRENCY);
  const elapsed = ((Date.now() - started) / 1000).toFixed(1);

  console.log('\n═════════════ خلاصه ═════════════');
  console.log(`✅ موفق:    ${results.ok}`);
  console.log(`❌ ناموفق:  ${results.fail}`);
  console.log(`📊 مجموع:   ${names.length}`);
  console.log(`⏱️  زمان:   ${elapsed} ثانیه`);
  console.log('════════════════════════════════════');

  process.exit(0);
})();
