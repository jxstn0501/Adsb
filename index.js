const http = require("http");
const fs = require("fs");
const url = require("url");
const path = require("path");
const readline = require("readline");
const puppeteer = require("puppeteer");
const { exec } = require("node:child_process");
const { promisify } = require("node:util");

let page;
let targetHex = "3e0fe9"; // Fallback
let latestData = {};      // letzter Datensatz fürs aktuelle Ziel
let events = [];          // Takeoff/Landing-Events
let flightStatus = {};    // Status pro Flugzeug ("online"/"offline")
const fsp = fs.promises;
const logsDir = path.join(__dirname, "logs");
const logCounts = {};     // Zeilenanzahl pro Hex-Datei

// ===== State loading =====
fs.mkdirSync(logsDir, { recursive: true });

try {
  const files = fs.readdirSync(logsDir);
  files
    .filter(name => name.toLowerCase().endsWith(".jsonl"))
    .forEach(file => {
      const hex = path.basename(file, ".jsonl");
      try {
        const contents = fs.readFileSync(path.join(logsDir, file), "utf8");
        const lines = contents
          .split(/\r?\n/)
          .filter(line => line.trim().length > 0);
        logCounts[hex] = lines.length;
      } catch (err) {
        console.error("⚠️ Log-Datei konnte nicht gelesen werden:", file, err.message);
        logCounts[hex] = logCounts[hex] || 0;
      }
    });
} catch (e) {}
try {
  const savedEvents = fs.readFileSync("events.json", "utf8");
  events = JSON.parse(savedEvents);
} catch (e) {}
try {
  const savedTarget = fs.readFileSync("last_target.json", "utf8");
  const parsed = JSON.parse(savedTarget);
  if (parsed.hex) targetHex = parsed.hex.toLowerCase();
} catch (e) {}
try {
  const savedLatest = fs.readFileSync("latest.json", "utf8");
  latestData = JSON.parse(savedLatest);
} catch (e) {}

// ===== Helpers =====
function cleanNum(str) {
  if (!str) return null;
  const s = str.replace(/[^\d\.\-]/g, "");
  return s ? Number(s) : null;
}

function parsePos(raw) {
  if (!raw || !raw.includes(",")) return { lat: null, lon: null };
  const [lat, lon] = raw.split(",").map(s => s.replace(/[^\d\.\-]/g, "").trim());
  return { lat: lat ? Number(lat) : null, lon: lon ? Number(lon) : null };
}

function parseLastSeen(raw) {
  if (!raw) return null;
  const sec = parseInt(raw.replace(/[^\d]/g, ""), 10);
  return isNaN(sec) ? null : sec;
}

// ===== Event Detection =====
function detectEventByLastSeen(record) {
  const hex = record.hex;
  if (!hex) return;

  const lastSeenSec = record.lastSeen;
  if (lastSeenSec === null) {
    console.warn("⚠️ lastSeen fehlt für", hex);
    return;
  }

  const prev = flightStatus[hex] || "offline";
  let now = prev;

  if (lastSeenSec < 10 &&
      ((record.alt !== null && record.alt > 100) ||
       (record.vr !== null && record.vr > 0))) {
    now = "online";
  } else if (lastSeenSec > 50 &&
             ((record.alt !== null && record.alt < 100) ||
              (record.vr !== null && record.vr <= 0))) {
    now = "offline";
  }

  if (now !== prev) {
    const type = now === "online" ? "takeoff" : "landing";
    const event = {
      time: record.time,
      type,
      hex: record.hex,
      callsign: record.callsign,
      lat: record.lat,
      lon: record.lon,
      alt: record.alt,
      gs: record.gs,
      lastSeen: record.lastSeen
    };
    events.push(event);
    fs.writeFileSync("events.json", JSON.stringify(events, null, 2));
    console.log("✈️ Event erkannt:", type, record.callsign, "LastSeen:", record.lastSeen);
  }

  flightStatus[hex] = now;
  return flightStatus[hex];
}

// ===== Scraper =====
async function scrape() {
  try {
    if (!page) return;

    const data = await page.evaluate(() => {
      const get = (sel) => document.querySelector(sel)?.textContent.trim() || null;
      const hexRaw = get("#selected_icao") || "";
      const hex = hexRaw.replace(/Hex:\s*/i, "").split(/\s+/)[0] || null;

      const lastSeen = get("#selected_seen_pos") || get("#selected_seen");

      return {
        time: new Date().toISOString(),
        callsign: get("#selected_callsign"),
        hex,
        reg: get("#selected_registration"),
        type: get("#selected_icaotype"),
        gs: get("#selected_speed1"),
        alt: get("#selected_altitude1"),
        pos: get("#selected_position"),
        vr: get("#selected_vert_rate"),
        hdg: get("#selected_track1"),
        lastSeen // ggf. anderer Selektor
      };
    });

    if (!data.hex) return;

    const { lat, lon } = parsePos(data.pos);
    const record = {
      time: data.time,
      hex: data.hex.toLowerCase(),
      callsign: data.callsign,
      reg: data.reg,
      type: data.type,
      gs: cleanNum(data.gs),
      alt: cleanNum(data.alt),
      vr: cleanNum(data.vr),
      hdg: cleanNum(data.hdg),
      lat,
      lon,
      lastSeen: parseLastSeen(data.lastSeen)
    };

    if (record.lastSeen === null) {
      console.warn("⚠️ lastSeen konnte nicht ermittelt werden für", record.hex);
    }

    latestData = record;
    fs.writeFileSync("latest.json", JSON.stringify(latestData, null, 2));

    // Event-Erkennung
    const status = detectEventByLastSeen(record);

    // Logging nur wenn online
    if (status === "online") {
      await appendLogRecord(record);
    }

  } catch (err) {
    console.error("❌ Scrape-Fehler:", err.message);
  }
}

async function appendLogRecord(record) {
  const hex = record.hex;
  const filePath = path.join(logsDir, `${hex}.jsonl`);
  const line = JSON.stringify(record);

  try {
    await fsp.appendFile(filePath, line + "\n");
  } catch (err) {
    if (err.code === "ENOENT") {
      await fsp.mkdir(logsDir, { recursive: true });
      await fsp.appendFile(filePath, line + "\n");
    } else {
      throw err;
    }
  }

  logCounts[hex] = (logCounts[hex] || 0) + 1;

  if (logCounts[hex] > 5000) {
    try {
      const contents = await fsp.readFile(filePath, "utf8");
      const lines = contents
        .split(/\r?\n/)
        .filter(entry => entry.trim().length > 0);
      const trimmed = lines.slice(-5000);
      logCounts[hex] = trimmed.length;
      const payload = trimmed.join("\n") + (trimmed.length ? "\n" : "");
      await fsp.writeFile(filePath, payload);
    } catch (err) {
      console.error("⚠️ Kürzen der Log-Datei fehlgeschlagen für", hex, err.message);
    }
  }
}

function parsePositiveInt(value, fallback) {
  const parsed = parseInt(value, 10);
  if (Number.isNaN(parsed) || parsed <= 0) return fallback;
  return parsed;
}

async function handleLogRequest(q, res) {
  const hex = q.query.hex ? q.query.hex.toLowerCase() : null;
  if (!hex) {
    res.writeHead(400, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "Parameter 'hex' wird benötigt." }));
    return;
  }

  const page = parsePositiveInt(q.query.page, 1);
  const limit = parsePositiveInt(q.query.limit, 100);
  const filePath = path.join(logsDir, `${hex}.jsonl`);

  try {
    await fsp.access(filePath, fs.constants.F_OK);
  } catch (err) {
    res.writeHead(404, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ data: [], page, totalPages: 0, total: 0, limit }));
    return;
  }

  try {
    const payload = await paginateLogFile(filePath, page, limit);
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ...payload, page, limit }));
  } catch (err) {
    console.error("❌ Fehler beim Lesen der Log-Datei:", err.message);
    res.writeHead(500, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "Log-Datei konnte nicht gelesen werden." }));
  }
}

function paginateLogFile(filePath, page, limit) {
  return new Promise((resolve, reject) => {
    const input = fs.createReadStream(filePath);
    input.on("error", reject);

    const rl = readline.createInterface({ input, crlfDelay: Infinity });
    const data = [];
    const startIndex = (page - 1) * limit;
    const endIndex = startIndex + limit;
    let index = 0;

    rl.on("line", line => {
      if (!line.trim()) {
        return;
      }
      if (index >= startIndex && index < endIndex) {
        try {
          data.push(JSON.parse(line));
        } catch (err) {
          console.warn("⚠️ Ungültige Log-Zeile in", filePath, err.message);
        }
      }
      index += 1;
    });

    rl.on("close", () => {
      const total = index;
      const totalPages = limit > 0 ? Math.ceil(total / limit) : 0;
      resolve({ data, total, totalPages });
    });

    rl.on("error", reject);
  });
}

// ===== Browser Start =====
async function startBrowser() {
  const { stdout: chromiumPath } = await promisify(exec)("which chromium");
  const browser = await puppeteer.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
    executablePath: chromiumPath.trim()
  });

  page = await browser.newPage();
  await page.goto(`https://globe.adsbexchange.com/?icao=${targetHex}`, { waitUntil: "domcontentloaded" });
  console.log("🌍 Globe geladen für:", targetHex);

  setInterval(scrape, 3000);
}

// ===== Static Serve Helper =====
function serveStatic(res, filePath) {
  const ext = path.extname(filePath).toLowerCase();
  const mime = {
    ".html": "text/html",
    ".css": "text/css",
    ".js": "application/javascript",
    ".json": "application/json"
  }[ext] || "text/plain";

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end("File not found");
    } else {
      res.writeHead(200, { "Content-Type": mime });
      res.end(data);
    }
  });
}

// ===== Server =====
const server = http.createServer((req, res) => {
  const q = url.parse(req.url, true);

  if (q.pathname === "/latest") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(latestData));

  } else if (q.pathname === "/log") {
    return handleLogRequest(q, res);

  } else if (q.pathname === "/events") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(events));

  } else if (q.pathname === "/set") {
    if (q.query.hex) {
      targetHex = q.query.hex.toLowerCase();
      fs.writeFileSync("last_target.json", JSON.stringify({ hex: targetHex }));
      page.goto(`https://globe.adsbexchange.com/?icao=${targetHex}`, { waitUntil: "domcontentloaded" });
      res.end("✅ Neues Ziel gesetzt: " + targetHex);
    } else {
      res.end("❌ Bitte ?hex=xxxxxx angeben");
    }

  } else {
    let filePath = path.join(__dirname, "public", q.pathname === "/" ? "index.html" : q.pathname);
    serveStatic(res, filePath);
  }
});

server.listen(3000, () => {
  console.log("✅ Server läuft auf Port 3000");
  startBrowser();
});
