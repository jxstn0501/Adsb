const http = require("http");
const fs = require("fs");
const url = require("url");
const path = require("path");
const readline = require("readline");
const puppeteer = require("puppeteer");
const { exec } = require("node:child_process");
const { promisify } = require("node:util");

let browser;
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
async function loadPlacesFromDisk() {
  try {
    const raw = await fsp.readFile(placesPath, "utf8");
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
        await fsp.writeFile(placesPath, JSON.stringify([], null, 2));
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

async function savePlaces(list) {
  if (!Array.isArray(list)) {
    throw new TypeError("Places list must be an array.");
  }

  const serialized = JSON.stringify(list, null, 2);
  const parsed = JSON.parse(serialized);
  places = parsed;
  await fsp.writeFile(placesPath, serialized);
  return getPlaces();
}

function startPlacesWatcher() {
  try {
    fs.watchFile(placesPath, { interval: 1000 }, () => {
      loadPlacesFromDisk().catch(err => {
        console.error("⚠️ places.json konnte nicht neu geladen werden:", err.message);
      });
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

  const altitude = toFiniteNumber(raw.altitudeThresholdFt);
  if (altitude !== null && altitude > 0) {
    normalized.altitudeThresholdFt = altitude;
  }

  const speed = toFiniteNumber(raw.speedThresholdKt);
  if (speed !== null && speed >= 0) {
    normalized.speedThresholdKt = speed;
  }

  const timeout = toFiniteNumber(raw.offlineTimeoutSec);
  if (timeout !== null && timeout >= 5) {
    normalized.offlineTimeoutSec = Math.round(timeout);
  }

  return normalized;
}

function getConfig() {
  return { ...config };
}

function getOperationalConfig() {
  return normalizeConfig(config);
}

async function loadConfigFromDisk() {
  try {
    const raw = await fsp.readFile(configPath, "utf8");

    if (!raw.trim()) {
      config = { ...DEFAULT_CONFIG };
      await fsp.writeFile(configPath, JSON.stringify(config, null, 2));
      return;
    }

    const parsed = JSON.parse(raw);
    config = normalizeConfig(parsed);
  } catch (err) {
    if (err.code === "ENOENT") {
      try {
        await fsp.writeFile(configPath, JSON.stringify(DEFAULT_CONFIG, null, 2));
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

async function saveConfig(newConfig) {
  config = normalizeConfig(newConfig);
  await fsp.writeFile(configPath, JSON.stringify(config, null, 2));
  return getConfig();
}

function updateConfig(partial) {
  const merged = { ...config, ...partial };
  return saveConfig(merged);
}

function startConfigWatcher() {
  try {
    fs.watchFile(configPath, { interval: 1000 }, () => {
      loadConfigFromDisk().catch(err => {
        console.error("⚠️ config.json konnte nicht neu geladen werden:", err.message);
      });
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
async function initializeState() {
  await fsp.mkdir(logsDir, { recursive: true });

  try {
    const files = await fsp.readdir(logsDir);
    for (const file of files) {
      if (!file.toLowerCase().endsWith(".jsonl")) {
        continue;
      }

      const hex = path.basename(file, ".jsonl");
      try {
        const contents = await fsp.readFile(path.join(logsDir, file), "utf8");
        const lines = contents
          .split(/\r?\n/)
          .filter(line => line.trim().length > 0);
        logCounts[hex] = lines.length;
      } catch (err) {
        console.error("⚠️ Log-Datei konnte nicht gelesen werden:", file, err.message);
        logCounts[hex] = logCounts[hex] || 0;
      }
    }
  } catch (err) {
    console.error("⚠️ Log-Verzeichnis konnte nicht gelesen werden:", err.message);
  }

  try {
    const savedEvents = await fsp.readFile("events.json", "utf8");
    const parsed = JSON.parse(savedEvents);
    if (Array.isArray(parsed)) {
      events = parsed;
    } else {
      events = [];
    }
  } catch (err) {
    if (err.code !== "ENOENT") {
      console.error("⚠️ events.json konnte nicht geladen werden:", err.message);
    }
    events = [];
  }

  try {
    const savedTarget = await fsp.readFile("last_target.json", "utf8");
    const parsed = JSON.parse(savedTarget);
    if (parsed.hex) {
      targetHex = String(parsed.hex).toLowerCase();
    }
  } catch (err) {
    if (err.code !== "ENOENT") {
      console.error("⚠️ last_target.json konnte nicht geladen werden:", err.message);
    }
  }

  try {
    const savedLatest = await fsp.readFile("latest.json", "utf8");
    const parsed = JSON.parse(savedLatest);
    if (parsed && typeof parsed === "object") {
      latestData = parsed;
    } else {
      latestData = {};
    }
  } catch (err) {
    if (err.code !== "ENOENT") {
      console.error("⚠️ latest.json konnte nicht geladen werden:", err.message);
    }
    latestData = {};
  }

  await loadPlacesFromDisk().catch(err => {
    console.error("⚠️ places.json konnte nicht geladen werden:", err.message);
  });
  startPlacesWatcher();

  await loadConfigFromDisk().catch(err => {
    console.error("⚠️ config.json konnte nicht geladen werden:", err.message);
  });
  startConfigWatcher();
}

// ===== Page task queue & scraping scheduler =====
function createSerialTaskQueue() {
  const queue = [];
  let running = false;

  const runNext = async () => {
    if (running) return;
    const next = queue.shift();
    if (!next) return;

    running = true;
    try {
      const result = await next.fn();
      next.resolve(result);
    } catch (err) {
      next.reject(err);
    } finally {
      running = false;
      runNext();
    }
  };

  return function enqueue(fn) {
    return new Promise((resolve, reject) => {
      queue.push({ fn, resolve, reject });
      void runNext();
    });
  };
}

const runWithPage = createSerialTaskQueue();
const SCRAPE_INTERVAL_MS = 3000;
let scrapeTimer = null;
let scrapeLoopActive = false;

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

  const text = String(raw).trim().toLowerCase();
  if (!text) return null;

  if (text === "live" || text === "now") {
    return 0;
  }

  const pattern = /([\d.,]+)\s*(ms|s|sec|secs|second|seconds|m|min|minute|minutes|h|hr|hour|hours)?/g;
  let totalSeconds = 0;
  let found = false;

  for (const match of text.matchAll(pattern)) {
    const numberPart = match[1]?.replace(",", ".");
    const value = Number.parseFloat(numberPart);
    if (!Number.isFinite(value)) {
      continue;
    }

    const unit = match[2] || "s";
    found = true;

    if (unit.startsWith("h")) {
      totalSeconds += value * 3600;
    } else if (unit.startsWith("m") && unit !== "ms") {
      totalSeconds += value * 60;
    } else if (unit === "ms") {
      totalSeconds += value / 1000;
    } else {
      totalSeconds += value;
    }
  }

  if (!found) {
    const colonCandidate = text.replace(/[^0-9:.,]/g, "");
    if (colonCandidate.includes(":")) {
      const segments = colonCandidate
        .split(":")
        .map(part => part.replace(/,/g, ".").trim())
        .filter(part => part.length > 0);

      if (segments.length >= 2 && segments.length <= 3) {
        const numbers = segments.map(segment => Number(segment));
        if (numbers.every(num => Number.isFinite(num))) {
          const [first, second, third] = numbers;
          if (segments.length === 3) {
            totalSeconds = first * 3600 + second * 60 + third;
          } else {
            totalSeconds = first * 60 + second;
          }
          found = true;
        }
      }
    }
  }

  if (!found) {
    const direct = Number(text.replace(/,/g, "."));
    if (Number.isFinite(direct)) {
      totalSeconds = direct;
      found = true;
    }
  }

  if (!found) {
    return null;
  }

  const rounded = Math.round(totalSeconds);
  return rounded >= 0 ? rounded : null;
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

const PLACE_MATCH_RADIUS_METERS = 500;

function determinePlaceForRecord(record) {
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

  if (nearest && nearestDistance <= PLACE_MATCH_RADIUS_METERS) {
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
      lastBelowThreshold: null,
      pendingTakeoff: null,
      hasSeen: false
    };
  }
  return flightStatus[hex];
}

async function registerEvent(type, record, options = {}) {
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
  try {
    await fsp.writeFile("events.json", JSON.stringify(events, null, 2));
  } catch (err) {
    console.error("⚠️ events.json konnte nicht gespeichert werden:", err.message);
  }
  console.log("✈️ Event erkannt:", type, record.callsign || record.hex, "LastSeen:", record.lastSeen);
}

// ===== Event Detection =====
async function detectEventByLastSeen(record) {
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

  if (belowAltitude || belowSpeed) {
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
        const place = determinePlaceForRecord(record);
        await registerEvent("landing", record, { place });
      }
    }
  }

  if (state.pendingTakeoff) {
    const { changeTime, skipInitialAirborne } = state.pendingTakeoff;

    if (timestamp - changeTime <= EVENT_WINDOW_MS) {
      if (exceedsAltitude || exceedsSpeed) {
        if (!skipInitialAirborne) {
          const place = determinePlaceForRecord(record);
          await registerEvent("takeoff", record, { place });
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
async function scrapeOnce() {
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
  try {
    await fsp.writeFile("latest.json", JSON.stringify(latestData, null, 2));
  } catch (err) {
    console.error("⚠️ latest.json konnte nicht gespeichert werden:", err.message);
  }

  const status = await detectEventByLastSeen(record);

  if (status === "online") {
    await appendLogRecord(record);
  }
}

async function runScrapeCycle() {
  if (!scrapeLoopActive) {
    return;
  }

  try {
    await runWithPage(() => scrapeOnce());
  } catch (err) {
    console.error("❌ Scrape-Fehler:", err.message);
  } finally {
    if (scrapeLoopActive) {
      scheduleNextScrape();
    }
  }
}

function scheduleNextScrape(delay = SCRAPE_INTERVAL_MS) {
  if (!scrapeLoopActive) {
    return;
  }

  if (scrapeTimer) {
    clearTimeout(scrapeTimer);
  }

  scrapeTimer = setTimeout(() => {
    scrapeTimer = null;
    void runScrapeCycle();
  }, delay);
}

function startScrapeLoop() {
  if (scrapeLoopActive) {
    return;
  }
  scrapeLoopActive = true;
  scheduleNextScrape(0);
}

function stopScrapeLoop() {
  scrapeLoopActive = false;
  if (scrapeTimer) {
    clearTimeout(scrapeTimer);
    scrapeTimer = null;
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
async function resolveChromiumExecutable() {
  if (process.env.PUPPETEER_EXECUTABLE_PATH) {
    return process.env.PUPPETEER_EXECUTABLE_PATH;
  }

  try {
    const { stdout } = await promisify(exec)("which chromium");
    const candidate = stdout.trim();
    if (candidate) {
      return candidate;
    }
  } catch (err) {
    // ignore, we'll fall back to Puppeteer's bundled binary
  }

  try {
    if (typeof puppeteer.executablePath === "function") {
      const builtin = puppeteer.executablePath();
      if (builtin) {
        return builtin;
      }
    }
  } catch (err) {
    console.warn("⚠️ Puppeteer executablePath konnte nicht bestimmt werden:", err.message);
  }

  return undefined;
}

async function startBrowser() {
  try {
    const executablePath = await resolveChromiumExecutable();
    const launchOptions = {
      headless: true,
      args: ["--no-sandbox", "--disable-setuid-sandbox"]
    };

    if (executablePath) {
      launchOptions.executablePath = executablePath;
    }

    browser = await puppeteer.launch(launchOptions);
    page = await browser.newPage();
    await runWithPage(() => page.goto(`https://globe.adsbexchange.com/?icao=${targetHex}`, { waitUntil: "domcontentloaded" }));
    console.log("🌍 Globe geladen für:", targetHex);

    startScrapeLoop();
  } catch (err) {
    console.error("❌ Browserstart fehlgeschlagen:", err.message);
  }
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

async function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    let aborted = false;

    const abort = (err) => {
      if (aborted) return;
      aborted = true;
      reject(err);
    };

    req.on("aborted", () => {
      const err = new Error("Request abgebrochen.");
      err.statusCode = 400;
      abort(err);
    });

    req.on("data", chunk => {
      if (aborted) return;
      body += chunk;
      if (body.length > 1e6) {
        const err = new Error("Payload zu groß.");
        err.statusCode = 413;
        abort(err);
        req.destroy();
      }
    });

    req.on("end", () => {
      if (aborted) return;
      try {
        const payload = body.trim() ? JSON.parse(body) : {};
        resolve(payload);
      } catch (err) {
        const parseError = new Error("Ungültiger JSON-Body.");
        parseError.statusCode = 400;
        reject(parseError);
      }
    });

    req.on("error", err => {
      if (aborted) return;
      reject(err);
    });
  });
}

async function handleSetRequest(res, hexParam) {
  const raw = typeof hexParam === "string" ? hexParam.trim() : "";
  if (!raw) {
    sendError(res, 400, "Bitte ?hex=xxxxxx angeben.");
    return;
  }

  targetHex = raw.toLowerCase();

  try {
    await fsp.writeFile("last_target.json", JSON.stringify({ hex: targetHex }, null, 2));
  } catch (err) {
    console.error("⚠️ last_target.json konnte nicht gespeichert werden:", err.message);
  }

  if (!page) {
    console.log("🎯 Neues Ziel gespeichert, Browser noch nicht bereit:", targetHex);
    sendJSON(res, 202, { message: "Ziel gespeichert. Browser wird vorbereitet.", hex: targetHex });
    return;
  }

  try {
    await runWithPage(() => page.goto(`https://globe.adsbexchange.com/?icao=${targetHex}`, { waitUntil: "domcontentloaded" }));
    if (scrapeLoopActive) {
      scheduleNextScrape(0);
    } else {
      startScrapeLoop();
    }
    console.log("🎯 Navigiere zu neuem Ziel:", targetHex);
    sendJSON(res, 200, { success: true, hex: targetHex });
  } catch (err) {
    console.error("❌ Navigation zum neuen Ziel fehlgeschlagen:", err.message);
    sendError(res, 500, "Navigation fehlgeschlagen.");
  }
}

async function handleRequest(req, res) {
  const q = url.parse(req.url, true);

  if (q.pathname === "/latest") {
    sendJSON(res, 200, latestData);
    return;
  }

  if (q.pathname === "/log") {
    await handleLogRequest(q, res);
    return;
  }

  if (q.pathname === "/events") {
    sendJSON(res, 200, events);
    return;
  }

  if (q.pathname === "/config") {
    if (req.method === "GET") {
      sendJSON(res, 200, getOperationalConfig());
      return;
    }

    if (req.method === "POST") {
      const payload = await parseJsonBody(req);
      let configPayload;
      try {
        configPayload = parseConfigPayload(payload);
      } catch (err) {
        sendError(res, 400, err.message);
        return;
      }
      const updated = await updateConfig(configPayload);
      sendJSON(res, 200, updated);
      return;
    }

    sendError(res, 405, "Methode nicht erlaubt.");
    return;
  }

  if (q.pathname && q.pathname.startsWith("/places")) {
    const segments = q.pathname.split("/").filter(Boolean);

    if (segments.length === 1) {
      if (req.method === "GET") {
        sendJSON(res, 200, getPlaces());
        return;
      }

      if (req.method === "POST") {
        const payload = await parseJsonBody(req);
        let placeData;
        try {
          placeData = parsePlacePayload(payload);
        } catch (err) {
          sendError(res, 400, err.message);
          return;
        }
        const newPlace = { id: generatePlaceId(), ...placeData };
        const updatedList = [...getPlaces(), newPlace];
        await savePlaces(updatedList);
        sendJSON(res, 201, newPlace);
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
        const payload = await parseJsonBody(req);
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
        await savePlaces(updatedList);
        sendJSON(res, 200, updatedPlace);
        return;
      }

      if (req.method === "DELETE") {
        const current = getPlaces();
        const filtered = current.filter(place => !(place && String(place.id) === String(placeId)));

        if (filtered.length === current.length) {
          sendError(res, 404, "Ort nicht gefunden.");
          return;
        }

        await savePlaces(filtered);
        sendJSON(res, 200, { success: true });
        return;
      }

      sendError(res, 405, "Methode nicht erlaubt.");
      return;
    }

    sendError(res, 404, "Pfad nicht gefunden.");
    return;
  }

  if (q.pathname === "/set") {
    await handleSetRequest(res, q.query.hex);
    return;
  }

  const filePath = path.join(__dirname, "public", q.pathname === "/" ? "index.html" : q.pathname);
  serveStatic(res, filePath);
  return;
}

const server = http.createServer((req, res) => {
  Promise.resolve(handleRequest(req, res)).catch(err => {
    console.error("❌ Unerwarteter Fehler:", err.message);
    if (!res.writableEnded) {
      const status = err.statusCode && Number.isInteger(err.statusCode) ? err.statusCode : 500;
      const message = status === 500 ? "Interner Serverfehler." : err.message;
      sendError(res, status, message);
    }
  });
});

async function bootstrap() {
  try {
    await initializeState();
  } catch (err) {
    console.error("❌ Initialisierung fehlgeschlagen:", err.message);
  }

  server.listen(3000, () => {
    console.log("✅ Server läuft auf Port 3000");
    startBrowser().catch(err => {
      console.error("❌ Starten des Browsers fehlgeschlagen:", err.message);
    });
  });
}

bootstrap().catch(err => {
  console.error("❌ Kritischer Fehler beim Start:", err.message);
  process.exitCode = 1;
});
