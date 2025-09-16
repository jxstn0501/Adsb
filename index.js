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
let flightStatus = {};    // Status- & Verlaufdaten pro Flugzeug
const fsp = fs.promises;
const logsDir = path.join(__dirname, "logs");
const logCounts = {};     // Zeilenanzahl pro Hex-Datei
const placesPath = path.join(__dirname, "places.json");
const configPath = path.join(__dirname, "config.json");
let places = [];

const DEFAULT_CONFIG = {
  altitudeThresholdFt: 300,
  speedThresholdKt: 40,
  offlineTimeoutSec: 60
};

let config = { ...DEFAULT_CONFIG };

// ===== Places storage =====
function loadPlacesFromDisk() {
  try {
    const raw = fs.readFileSync(placesPath, "utf8");
    if (!raw.trim()) {
      places = [];
      return;
    }

    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      places = parsed;
    } else {
      console.warn("⚠️ places.json enthält kein Array. Bestehende Werte bleiben erhalten.");
    }
  } catch (err) {
    if (err.code === "ENOENT") {
      try {
        fs.writeFileSync(placesPath, JSON.stringify([], null, 2));
        places = [];
      } catch (writeErr) {
        console.error("❌ places.json konnte nicht erstellt werden:", writeErr.message);
      }
    } else if (err.name === "SyntaxError") {
      console.error("❌ Ungültiges JSON in places.json:", err.message);
    } else {
      console.error("❌ places.json konnte nicht gelesen werden:", err.message);
    }
  }
}

function getPlaces() {
  return Array.isArray(places)
    ? places.map(place => (place && typeof place === "object" ? { ...place } : place))
    : [];
}

function savePlaces(list) {
  if (!Array.isArray(list)) {
    throw new TypeError("Places list must be an array.");
  }

  const serialized = JSON.stringify(list, null, 2);
  const parsed = JSON.parse(serialized);
  places = parsed;
  fs.writeFileSync(placesPath, serialized);
  return getPlaces();
}

function startPlacesWatcher() {
  try {
    fs.watchFile(placesPath, { interval: 1000 }, () => {
      try {
        loadPlacesFromDisk();
      } catch (err) {
        console.error("⚠️ places.json konnte nicht neu geladen werden:", err.message);
      }
    });
  } catch (err) {
    console.error("⚠️ Beobachten von places.json fehlgeschlagen:", err.message);
  }
}

function normalizeConfig(raw) {
  const normalized = { ...DEFAULT_CONFIG };

  if (!raw || typeof raw !== "object") {
    return normalized;
  }

  if (Object.prototype.hasOwnProperty.call(raw, "altitudeThresholdFt")) {
    const value = Number(raw.altitudeThresholdFt);
    if (Number.isFinite(value) && value > 0) {
      normalized.altitudeThresholdFt = value;
    }
  } else if (Object.prototype.hasOwnProperty.call(config, "altitudeThresholdFt")) {
    normalized.altitudeThresholdFt = config.altitudeThresholdFt;
  }

  if (Object.prototype.hasOwnProperty.call(raw, "speedThresholdKt")) {
    const value = Number(raw.speedThresholdKt);
    if (Number.isFinite(value) && value >= 0) {
      normalized.speedThresholdKt = value;
    }
  } else if (Object.prototype.hasOwnProperty.call(config, "speedThresholdKt")) {
    normalized.speedThresholdKt = config.speedThresholdKt;
  }

  if (Object.prototype.hasOwnProperty.call(raw, "offlineTimeoutSec")) {
    const value = Number(raw.offlineTimeoutSec);
    if (Number.isFinite(value) && value >= 5) {
      normalized.offlineTimeoutSec = Math.round(value);
    }
  } else if (Object.prototype.hasOwnProperty.call(config, "offlineTimeoutSec")) {
    normalized.offlineTimeoutSec = config.offlineTimeoutSec;
  }

  return normalized;
}

function getConfig() {
  return { ...config };
}

function getOperationalConfig() {
  return normalizeConfig(config);
}

function loadConfigFromDisk() {
  try {
    const raw = fs.readFileSync(configPath, "utf8");

    if (!raw.trim()) {
      config = { ...DEFAULT_CONFIG };
      fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
      return;
    }

    const parsed = JSON.parse(raw);
    config = normalizeConfig(parsed);
  } catch (err) {
    if (err.code === "ENOENT") {
      try {
        fs.writeFileSync(configPath, JSON.stringify(DEFAULT_CONFIG, null, 2));
      } catch (writeErr) {
        console.error("❌ config.json konnte nicht erstellt werden:", writeErr.message);
      }
      config = { ...DEFAULT_CONFIG };
    } else if (err.name === "SyntaxError") {
      console.error("❌ Ungültiges JSON in config.json:", err.message);
      config = { ...DEFAULT_CONFIG };
    } else {
      console.error("❌ config.json konnte nicht gelesen werden:", err.message);
      config = { ...DEFAULT_CONFIG };
    }
  }
}

function saveConfig(newConfig) {
  config = normalizeConfig(newConfig);
  fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
  return getConfig();
}

function updateConfig(partial) {
  const merged = { ...config, ...partial };
  return saveConfig(merged);
}

function startConfigWatcher() {
  try {
    fs.watchFile(configPath, { interval: 1000 }, () => {
      try {
        loadConfigFromDisk();
      } catch (err) {
        console.error("⚠️ config.json konnte nicht neu geladen werden:", err.message);
      }
    });
  } catch (err) {
    console.error("⚠️ Beobachten von config.json fehlgeschlagen:", err.message);
  }
}

function generatePlaceId() {
  const list = getPlaces();
  const numericIds = list
    .map(place => {
      if (!place || typeof place.id === "undefined") {
        return NaN;
      }
      const num = Number(place.id);
      return Number.isInteger(num) && num >= 0 ? num : NaN;
    })
    .filter(Number.isFinite);

  if (numericIds.length > 0) {
    return String(Math.max(...numericIds) + 1);
  }

  return Date.now().toString(36);
}

function parsePlacePayload(payload) {
  if (!payload || typeof payload !== "object") {
    throw new Error("Body muss ein Objekt sein.");
  }

  const name = typeof payload.name === "string" ? payload.name.trim() : "";
  if (!name) {
    throw new Error("Feld 'name' wird benötigt.");
  }

  const type = typeof payload.type === "string" ? payload.type.trim() : "";
  if (!type) {
    throw new Error("Feld 'type' wird benötigt.");
  }

  const lat = toFiniteNumber(payload.lat);
  if (lat === null || lat < -90 || lat > 90) {
    throw new Error("Feld 'lat' muss zwischen -90 und 90 liegen.");
  }

  const lon = toFiniteNumber(payload.lon);
  if (lon === null || lon < -180 || lon > 180) {
    throw new Error("Feld 'lon' muss zwischen -180 und 180 liegen.");
  }

  return { name, type, lat, lon };
}

function parseConfigPayload(payload) {
  if (!payload || typeof payload !== "object") {
    throw new Error("Body muss ein Objekt sein.");
  }

  const altitude = toFiniteNumber(payload.altitudeThresholdFt);
  if (altitude === null || altitude <= 0) {
    throw new Error("Feld 'altitudeThresholdFt' muss größer als 0 sein.");
  }

  const speed = toFiniteNumber(payload.speedThresholdKt);
  if (speed === null || speed < 0) {
    throw new Error("Feld 'speedThresholdKt' muss größer oder gleich 0 sein.");
  }

  const timeout = toFiniteNumber(payload.offlineTimeoutSec);
  if (timeout === null || timeout < 5) {
    throw new Error("Feld 'offlineTimeoutSec' muss mindestens 5 Sekunden betragen.");
  }

  return {
    altitudeThresholdFt: altitude,
    speedThresholdKt: speed,
    offlineTimeoutSec: Math.round(timeout)
  };
}

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

loadPlacesFromDisk();
startPlacesWatcher();
loadConfigFromDisk();
startConfigWatcher();

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

function toFiniteNumber(value) {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function haversine(lat1, lon1, lat2, lon2) {
  const coords = [lat1, lon1, lat2, lon2].map(toFiniteNumber);
  if (coords.some(value => value === null)) {
    return Infinity;
  }

  const [latA, lonA, latB, lonB] = coords;
  const toRad = (deg) => (deg * Math.PI) / 180;

  const φ1 = toRad(latA);
  const φ2 = toRad(latB);
  const Δφ = toRad(latB - latA);
  const Δλ = toRad(lonB - lonA);

  const sinHalfΔφ = Math.sin(Δφ / 2);
  const sinHalfΔλ = Math.sin(Δλ / 2);

  const a = sinHalfΔφ * sinHalfΔφ +
            Math.cos(φ1) * Math.cos(φ2) * sinHalfΔλ * sinHalfΔλ;
  const normalizedA = Math.min(1, Math.max(0, a));
  const c = 2 * Math.atan2(Math.sqrt(normalizedA), Math.sqrt(1 - normalizedA));
  const EARTH_RADIUS = 6371000; // meters

  return EARTH_RADIUS * c;
}

const LANDING_MATCH_RADIUS_METERS = 500;

function categoriseLanding(record) {
  if (!record || typeof record !== "object") {
    return { type: "external" };
  }

  const lat = toFiniteNumber(record.lat);
  const lon = toFiniteNumber(record.lon);

  if (lat === null || lon === null) {
    return { type: "external" };
  }

  const list = getPlaces();
  if (!Array.isArray(list) || list.length === 0) {
    return { type: "external" };
  }

  let nearest = null;
  let nearestDistance = Infinity;

  for (const place of list) {
    if (!place || typeof place !== "object") {
      continue;
    }

    const placeLat = toFiniteNumber(place.lat);
    const placeLon = toFiniteNumber(place.lon);

    if (placeLat === null || placeLon === null) {
      continue;
    }

    const distance = haversine(lat, lon, placeLat, placeLon);
    if (!Number.isFinite(distance)) {
      continue;
    }

    if (distance < nearestDistance) {
      nearestDistance = distance;
      nearest = { place, lat: placeLat, lon: placeLon };
    }
  }

  if (nearest && nearestDistance <= LANDING_MATCH_RADIUS_METERS) {
    const { place, lat: placeLat, lon: placeLon } = nearest;
    const name =
      typeof place.name === "string" && place.name.trim()
        ? place.name.trim()
        : (typeof place.id !== "undefined" ? String(place.id) : "Unbenannter Ort");
    const type =
      typeof place.type === "string" && place.type.trim()
        ? place.type
        : "unknown";

    return { name, lat: placeLat, lon: placeLon, type };
  }

  return { type: "external" };
}

const EVENT_WINDOW_MS = 60 * 1000;

function ensureFlightState(hex) {
  if (!flightStatus[hex]) {
    flightStatus[hex] = {
      status: "offline",
      lastStatusChange: null,
      lastAboveThreshold: null,
      lastBelowThreshold: null,
      pendingTakeoff: null,
      hasSeen: false
    };
  }
  return flightStatus[hex];
}

function registerEvent(type, record, options = {}) {
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

  if (options && Object.prototype.hasOwnProperty.call(options, "place")) {
    const placeInfo = options.place;
    event.place = placeInfo && typeof placeInfo === "object"
      ? { ...placeInfo }
      : placeInfo;
  }
  events.push(event);
  fs.writeFileSync("events.json", JSON.stringify(events, null, 2));
  console.log("✈️ Event erkannt:", type, record.callsign || record.hex, "LastSeen:", record.lastSeen);
}

// ===== Event Detection =====
function detectEventByLastSeen(record) {
  const hex = record.hex;
  if (!hex) return;

  const state = ensureFlightState(hex);
  const isFirstRecord = !state.hasSeen;

  const timestamp = Date.parse(record.time);
  if (Number.isNaN(timestamp)) {
    console.warn("⚠️ Ungültiger Zeitstempel für", hex, record.time);
    return state.status;
  }

  const { altitudeThresholdFt, speedThresholdKt, offlineTimeoutSec } = getOperationalConfig();
  const altitudeThreshold = altitudeThresholdFt > 0 ? altitudeThresholdFt : DEFAULT_CONFIG.altitudeThresholdFt;
  const effectiveSpeedThreshold = speedThresholdKt > 0 ? speedThresholdKt : null;
  const offlineTimeout = offlineTimeoutSec >= 5 ? offlineTimeoutSec : DEFAULT_CONFIG.offlineTimeoutSec;
  const onlineThresholdSec = Math.max(5, Math.min(Math.round(offlineTimeout / 3), offlineTimeout));

  const lastSeenSec = record.lastSeen;
  if (lastSeenSec === null) {
    console.warn("⚠️ lastSeen fehlt für", hex);
  }

  const prevStatus = state.status;
  let now = prevStatus;

  const exceedsAltitude = record.alt !== null && record.alt > altitudeThreshold;
  const belowAltitude = record.alt !== null && record.alt < altitudeThreshold;
  const exceedsSpeed = effectiveSpeedThreshold !== null && record.gs !== null && record.gs > effectiveSpeedThreshold;
  const belowSpeed = effectiveSpeedThreshold !== null && record.gs !== null && record.gs < effectiveSpeedThreshold;
  const climbing = record.vr !== null && record.vr > 0;
  const descendingOrLevel = record.vr !== null && record.vr <= 0;

  const isMoving = exceedsAltitude || exceedsSpeed || climbing;
  const isGrounded = belowAltitude || belowSpeed || descendingOrLevel;

  if (lastSeenSec !== null) {
    if (lastSeenSec <= onlineThresholdSec && isMoving) {
      now = "online";
    } else if (lastSeenSec >= offlineTimeout && isGrounded) {
      now = "offline";
    }
  }

  if (exceedsAltitude || exceedsSpeed) {
    state.lastAboveThreshold = timestamp;
  } else if (belowAltitude || belowSpeed) {
    state.lastBelowThreshold = timestamp;
  }

  if (now !== prevStatus) {
    state.status = now;
    state.lastStatusChange = timestamp;

    if (now === "online") {
      const skipInitialAirborne = isFirstRecord && (exceedsAltitude || exceedsSpeed);
      state.pendingTakeoff = {
        changeTime: timestamp,
        skipInitialAirborne
      };
    } else {
      state.pendingTakeoff = null;

      if (prevStatus === "online" &&
          state.lastBelowThreshold !== null &&
          timestamp - state.lastBelowThreshold <= EVENT_WINDOW_MS) {
        const place = categoriseLanding(record);
        registerEvent("landing", record, { place });
      }
    }
  }

  if (state.pendingTakeoff) {
    const { changeTime, skipInitialAirborne } = state.pendingTakeoff;

    if (timestamp - changeTime <= EVENT_WINDOW_MS) {
      if (exceedsAltitude || exceedsSpeed) {
        if (!skipInitialAirborne) {
          registerEvent("takeoff", record);
        }
        state.pendingTakeoff = null;
      }
    } else {
      state.pendingTakeoff = null;
    }
  }

  state.hasSeen = true;
  return state.status;
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
    const overview = Object.keys(logCounts)
      .sort()
      .map(key => ({ hex: key, count: logCounts[key] || 0 }));
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(overview));
    return;
  }

  const hasPagination = typeof q.query.page !== "undefined" ||
                        typeof q.query.limit !== "undefined";
  const page = hasPagination ? parsePositiveInt(q.query.page, 1) : 1;
  const limit = hasPagination ? parsePositiveInt(q.query.limit, 100) : null;
  const filePath = path.join(logsDir, `${hex}.jsonl`);

  try {
    await fsp.access(filePath, fs.constants.F_OK);
  } catch (err) {
    res.writeHead(404, { "Content-Type": "application/json" });
    res.end("[]");
    return;
  }

  try {
    const payload = await readLogFile(filePath, { page, limit });
    const headers = { "Content-Type": "application/json" };

    if (hasPagination) {
      headers["X-Total-Count"] = String(payload.total);
      res.writeHead(200, headers);
      res.end(JSON.stringify({
        data: payload.data,
        page,
        limit,
        total: payload.total,
        totalPages: payload.totalPages
      }));
    } else {
      res.writeHead(200, headers);
      res.end(JSON.stringify(payload.data));
    }
  } catch (err) {
    console.error("❌ Fehler beim Lesen der Log-Datei:", err.message);
    res.writeHead(500, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "Log-Datei konnte nicht gelesen werden." }));
  }
}

function readLogFile(filePath, { page = 1, limit = null } = {}) {
  return new Promise((resolve, reject) => {
    const input = fs.createReadStream(filePath);
    input.on("error", reject);

    const rl = readline.createInterface({ input, crlfDelay: Infinity });
    const data = [];
    const hasLimit = typeof limit === "number" && Number.isFinite(limit);
    const safePage = page > 0 ? page : 1;
    const safeLimit = hasLimit && limit > 0 ? limit : null;
    const startIndex = safeLimit ? (safePage - 1) * safeLimit : 0;
    const endIndex = safeLimit ? startIndex + safeLimit : Infinity;
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
      const totalPages = safeLimit ? Math.max(1, Math.ceil(total / safeLimit)) : 1;
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

function sendJSON(res, statusCode, payload) {
  res.writeHead(statusCode, { "Content-Type": "application/json" });
  res.end(JSON.stringify(payload));
}

function sendError(res, statusCode, message) {
  sendJSON(res, statusCode, { error: message });
}

function readJsonBody(req, res, onSuccess) {
  let body = "";
  let aborted = false;

  req.on("data", chunk => {
    if (aborted) return;
    body += chunk;
    if (body.length > 1e6) {
      aborted = true;
      sendError(res, 413, "Payload zu groß.");
      req.destroy();
    }
  });

  req.on("end", () => {
    if (aborted) return;
    try {
      const payload = body.trim() ? JSON.parse(body) : {};
      onSuccess(payload);
    } catch (err) {
      sendError(res, 400, "Ungültiger JSON-Body.");
    }
  });

  req.on("error", err => {
    if (aborted) return;
    console.error("❌ Fehler beim Empfangen des Request-Bodys:", err.message);
    sendError(res, 500, "Body konnte nicht gelesen werden.");
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

  } else if (q.pathname === "/config") {
    if (req.method === "GET") {
      sendJSON(res, 200, getOperationalConfig());
      return;
    }

    if (req.method === "POST") {
      readJsonBody(req, res, payload => {
        let configPayload;
        try {
          configPayload = parseConfigPayload(payload);
        } catch (err) {
          sendError(res, 400, err.message);
          return;
        }

        try {
          const updated = updateConfig(configPayload);
          sendJSON(res, 200, updated);
        } catch (err) {
          console.error("❌ Speichern von config.json fehlgeschlagen:", err.message);
          sendError(res, 500, "Konfiguration konnte nicht gespeichert werden.");
        }
      });
      return;
    }

    sendError(res, 405, "Methode nicht erlaubt.");
    return;

  } else if (q.pathname.startsWith("/places")) {
    const segments = q.pathname.split("/").filter(Boolean);

    if (segments.length === 1) {
      if (req.method === "GET") {
        sendJSON(res, 200, getPlaces());
        return;
      }

      if (req.method === "POST") {
        readJsonBody(req, res, payload => {
          let placeData;
          try {
            placeData = parsePlacePayload(payload);
          } catch (err) {
            sendError(res, 400, err.message);
            return;
          }

          const newPlace = { id: generatePlaceId(), ...placeData };
          const current = getPlaces();
          const updatedList = [...current, newPlace];

          try {
            savePlaces(updatedList);
            sendJSON(res, 201, newPlace);
          } catch (err) {
            console.error("❌ Speichern von places.json fehlgeschlagen:", err.message);
            sendError(res, 500, "Ort konnte nicht gespeichert werden.");
          }
        });
        return;
      }

      sendError(res, 405, "Methode nicht erlaubt.");
      return;
    }

    if (segments.length === 2) {
      let placeId;
      try {
        placeId = decodeURIComponent(segments[1]);
      } catch (err) {
        sendError(res, 400, "Ungültige ID.");
        return;
      }

      if (req.method === "GET") {
        const entry = getPlaces().find(place => place && String(place.id) === String(placeId));
        if (!entry) {
          sendError(res, 404, "Ort nicht gefunden.");
          return;
        }
        sendJSON(res, 200, entry);
        return;
      }

      if (req.method === "PUT") {
        readJsonBody(req, res, payload => {
          let placeData;
          try {
            placeData = parsePlacePayload(payload);
          } catch (err) {
            sendError(res, 400, err.message);
            return;
          }

          const current = getPlaces();
          const index = current.findIndex(place => place && String(place.id) === String(placeId));

          if (index < 0) {
            sendError(res, 404, "Ort nicht gefunden.");
            return;
          }

          const updatedPlace = { ...current[index], ...placeData, id: current[index].id ?? placeId };
          const updatedList = [...current];
          updatedList[index] = updatedPlace;

          try {
            savePlaces(updatedList);
            sendJSON(res, 200, updatedPlace);
          } catch (err) {
            console.error("❌ Speichern von places.json fehlgeschlagen:", err.message);
            sendError(res, 500, "Ort konnte nicht gespeichert werden.");
          }
        });
        return;
      }

      if (req.method === "DELETE") {
        const current = getPlaces();
        const filtered = current.filter(place => !(place && String(place.id) === String(placeId)));

        if (filtered.length === current.length) {
          sendError(res, 404, "Ort nicht gefunden.");
          return;
        }

        try {
          savePlaces(filtered);
          sendJSON(res, 200, { success: true });
        } catch (err) {
          console.error("❌ Speichern von places.json fehlgeschlagen:", err.message);
          sendError(res, 500, "Ort konnte nicht gelöscht werden.");
        }
        return;
      }

      sendError(res, 405, "Methode nicht erlaubt.");
      return;
    }

    sendError(res, 404, "Pfad nicht gefunden.");
    return;

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
