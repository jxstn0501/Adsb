const http = require("http");
const fs = require("fs");
const url = require("url");
const path = require("path");
const puppeteer = require("puppeteer");
const { exec } = require("node:child_process");
const { promisify } = require("node:util");

let page;
let targetHex = "3e0fe9"; // Fallback
let db = {};              // Logs: { hex: [records] }
let latestData = {};      // letzter Datensatz fürs aktuelle Ziel
let events = [];          // Takeoff/Landing-Events
let flightStatus = {};    // Status pro Flugzeug ("online"/"offline")

// ===== State loading =====
try {
  const savedDb = fs.readFileSync("adsb_log.json", "utf8");
  db = JSON.parse(savedDb);
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
      hex: data.hex,
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
      if (!db[record.hex]) db[record.hex] = [];
      db[record.hex].push(record);
      if (db[record.hex].length > 5000) {
        db[record.hex] = db[record.hex].slice(-5000);
      }
      fs.writeFileSync("adsb_log.json", JSON.stringify(db, null, 2));
    }

  } catch (err) {
    console.error("❌ Scrape-Fehler:", err.message);
  }
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
    res.writeHead(200, { "Content-Type": "application/json" });
    if (q.query.hex && db[q.query.hex]) {
      res.end(JSON.stringify(db[q.query.hex]));
    } else {
      res.end(JSON.stringify(db));
    }

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
