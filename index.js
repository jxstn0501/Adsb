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
const logsHistoryDir = path.join(__dirname, "logs_history");
const historyLogsDir = path.join(__dirname, "logs_history");
const HISTORY_REQUEST_INTERVAL_MS = 2_000;
const HISTORY_RATELIMIT_WAIT_MS = 30_000;
const historyDownloadsInFlight = new Set();
const historyDownloadQueues = new Map();
const historyExistingDaysCache = new Map();
const logCounts = {};     // Zeilenanzahl pro Hex-Datei
const lastLogRecords = {}; // Letzter Log-Eintrag pro Hex
const placesPath = path.join(__dirname, "places.json");
const configPath = path.join(__dirname, "config.json");
const aircraftPath = path.join(__dirname, "aircraft.json");
let places = [];
let lastEventId = 0;
let aircraftProfiles = [];
let eventPlaceRecalculationPromise = null;
const eventStreamClients = new Set();
const EVENT_STREAM_HEARTBEAT_INTERVAL_MS = 30_000;

const EMBEDDED_ICON_192 = Buffer.from(
  "iVBORw0KGgoAAAANSUhEUgAAAMAAAADACAYAAABS3GwHAAAFtklEQVR4nO3dO28cVQCG4e/MXuzYDrHFLRFFihSIBpTCIEUpUBpqapS/QDpo6BD0/AUqChASAokCyiChICFEAw1VEJZjLoLYi/cyh2I8lkicyLMz6z0z3/u03pk9s953d3Zm9my4dXMUBZjKlj0AYJkIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANYIANb6yx7AaYRQb/kYmxnHgxY9rrrrT9mi/idVJR9AjNJkImnOB6zXk7Jeo0M6Np1IeT7fsiFI/cGj/x6jNBnPt+426A/SCDzpAGKUen3p6Yth7gdr/x9p/35s/MGOUdp6Kmhldb7lZ1Ppj72Tq45RGq5Iz1xK4BmyIH/9HjUeLz+CZAMIQZpOpa0ng956b0XDYbXlYyzW8dlHU33x8UTrG2HuV+uHxpZJhyPp9Tf6uvpK7/i+qtj5Ndf7b4+VPfApLMuk0YF05flMb75TcaNb5IN3x/rpx1zn1uZ/F21CKz4El/uLVfYbz2Ifs864TrtMKvvKTUlte1oRALAoBABrBABrBABrBABrBABrBABryZ4IcxejNJtprpNsJwlBD510qyLPmzmGX25PKucDCCBBMUr9fnEdUyrqxHOSfj+NCAggMTEWF4rt7UZ9+em09jtACMXFhM9eCtq+Xv2yjfL2330z087dqMGw3hO3XN/eblR/sPwICCAxMUqDowA++XBSe31ZTzrYl66+3NP29fnfUm5/PdOd2zOtb0j5rPawtLoWNCAAnKTcBXpis/7Of5YVQa2t11vP2rp0YTNobb2Zi9ea+kxRFwEkqvwQ3NR66j5p87xYTxPrSgmHQWGNAGCNAGCNAGCNAGCt80eByksAmjyTGY7Wt+wvdKO+zgcwGRcngkJo7vBdyKR/R8WX9tFunQ2gfHW++FzQS9uZVs8FxaaOX2fS+FDa3Ar/uy+0T+cDuHajp2s3FntVGQG0V2cDKMW42KkRefK3W+cD4EmKx+EwKKwRAKwRAKwRAKwRAKwRAKx1/jBojMUlEIs4Ehq4Hqj1Oh9ACGlNL4K0dDaAcvqN77+d6Yc7M62sNnst0GQsvfpaT5evZI1NXoWz1/kAfvk56qvPp9o43+xPJI0OpBdezHT5SnOzt+HsdTaA0nBFOn8hNP4bYf1BMd0I2q3zAcRYTOSUNzidRzhaZwrz2qAeDoPCGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAWiu+E1z+yEWV7+DOs0ydcZ32fsrbLeP7xPM8Hl3/3nPyAYQgDYfVf+iinAyrt8At7A+qT7xVbsNguJgxPe5+5/mxkPL2oaP7CkkHEII0m0q/3Y0aDqvNv5PnxU+Z3v87LmTOnhCkP/ei7u3E4/s6jXIbdnfO7qU1BOnwULq3EyvPYVTe/nDUzbmPwq2bo+Tf5Oq8DS/yn1Z39+Csn1CpPo7LlPQ7QCnVBz/VcT1K28Z7Fjq6ZwecDgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHAGgHA2n951kiCJu/6vQAAAABJRU5ErkJggg==",
  "base64"
);
const EMBEDDED_ICON_512 = Buffer.from(
  "iVBORw0KGgoAAAANSUhEUgAAAgAAAAIACAYAAAD0eNT6AAAJFUlEQVR4nO3YMWpUUQCG0Tv61EK3ILgHO5uQKqRMHaaXNFNYphAEK3txB6lTpcwibNyFCEqIDI4LEARhnDfJd84CHn9xL+/jLlbLm80AAFIezD0AANg9AQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAETXMP4N+9efd4PH+x3+12fbUelxfruWfs1MHRNE5O9/tK/fi+Gednt1v/7vuPT8bTZ4utf5eeb1834+1q+2eUP+33XwQA+C8EAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQJAAAIAgAQAAQQIAAIIEAAAECQAACBIAABAkAAAgSAAAQNA09wDg7js/u517wtYtXz8aL189nHvGX335/Gt8+vBz7hncUV4AACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAgqa5B3A/HR5P4/DY8QLYV14AACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAARNcw/gfrq+Wo/Li/XcM3bq4GgaJ6euFHA3eAEAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAggQAAAQJAAAIEgAAECQAACBIAABAkAAAgCABAABBAgAAghar5c1m7hEAwG55AQCAIAEAAEECAACCBAAABAkAAAgSAAAQJAAAIEgAAECQAACAIAEAAEECAACCBAAABAkAAAgSAAAQJAAAIEgAAECQAACAIAEAAEECAACCBAAABAkAAAgSAAAQJAAAIEgAAECQAACAIAEAAEECAACCBAAABAkAAAgSAAAQJAAAIEgAAECQAACAIAEAAEECAACCBAAABAkAAAgSAAAQJAAAIEgAAECQAACAIAEAAEECAACCBAAABAkAAAgSAAAQJAAAIEgAAECQAACAIAEAAEECAACCBAAABAkAAAgSAAAQJAAAIEgAAECQAACAIAEAAEECAACCBAAABAkAAAgSAAAQJAAAIEgAAECQAACAIAEAAEECAACCBAAABAkAAAgSAAAQJAAAIEgAAECQAACAIAEAAEG/AbJWKoWw2uRRAAAAAElFTkSuQmCC",
  "base64"
);

const EMBEDDED_ASSETS = new Map([
  ["/icons/icon-192.png", { buffer: EMBEDDED_ICON_192, contentType: "image/png" }],
  ["/icons/icon-512.png", { buffer: EMBEDDED_ICON_512, contentType: "image/png" }],
  ["/apple-touch-icon.png", { buffer: EMBEDDED_ICON_192, contentType: "image/png" }]
]);

const ADSB_BASE_URL = "https://globe.adsbexchange.com/?icao=";
const ADSB_REFERER = "https://globe.adsbexchange.com/";
const NAVIGATION_WAIT_UNTIL = "domcontentloaded";
const NAVIGATION_TIMEOUT_MS = 60000;
const PAGE_DEFAULT_TIMEOUT_MS = 60000;
const PAGE_OPERATION_TIMEOUT_MS = 45000;
const PUPPETEER_PROTOCOL_TIMEOUT_MS = 120000;
const PAGE_OPERATION_TIMEOUT_CODE = "PAGE_OPERATION_TIMEOUT";
const BROWSER_CLOSE_TIMEOUT_MS = 10_000;
let pageRecoveryInProgress = false;

const MAX_CONSECUTIVE_TIMEOUTS_BEFORE_BROWSER_RESTART = 3;
let consecutiveTimeoutCount = 0;

const DEFAULT_PLACE_MATCH_RADIUS_METERS = 500;

const OSM_REVERSE_URL = "https://nominatim.openstreetmap.org/reverse";
const OSM_REVERSE_CACHE = new Map();
const OSM_CACHE_MAX_SIZE = 500;
const OSM_CACHE_TTL_MS = 1000 * 60 * 60 * 12; // 12 Stunden
const OSM_CACHE_COORD_PRECISION = 3;
const OSM_DEFAULT_LANGUAGE = "de,en";
const OSM_USER_AGENT = "HeliTracker/1.0 (+https://helitracker.local)";

const DEFAULT_CONFIG = {
  altitudeThresholdFt: 300,
  speedThresholdKt: 40,
  offlineTimeoutSec: 60,
  placeMatchRadiusMeters: DEFAULT_PLACE_MATCH_RADIUS_METERS,
  notificationsEnabled: false,
  notifyOnTakeoff: true,
  notifyOnLanding: true,
  adsbHistoryAuthHeader: ""
};

let config = { ...DEFAULT_CONFIG };

// ===== Places storage =====
function sanitizePlaceEntry(place) {
  if (!place || typeof place !== "object") {
    return place;
  }

  const sanitized = { ...place };

  if (Object.prototype.hasOwnProperty.call(sanitized, "matchRadiusMeters")) {
    const radius = toFiniteNumber(sanitized.matchRadiusMeters);
    if (radius !== null && radius > 0) {
      sanitized.matchRadiusMeters = radius;
    } else {
      delete sanitized.matchRadiusMeters;
    }
  }

  return sanitized;
}

async function loadPlacesFromDisk() {
  try {
    const raw = await fsp.readFile(placesPath, "utf8");
    if (!raw.trim()) {
      places = [];
      try {
        await recalculateEventPlacesForAllEvents();
      } catch (err) {
        console.error("⚠️ Events konnten nach Löschen der Orte nicht neu berechnet werden:", err.message);
      }
      return;
    }

    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      places = parsed.map(sanitizePlaceEntry);
      try {
        await recalculateEventPlacesForAllEvents();
      } catch (err) {
        console.error("⚠️ Events konnten nach Laden der Orte nicht neu berechnet werden:", err.message);
      }
    } else {
      console.warn("⚠️ places.json enthält kein Array. Bestehende Werte bleiben erhalten.");
    }
  } catch (err) {
    if (err.code === "ENOENT") {
      try {
        await fsp.writeFile(placesPath, JSON.stringify([], null, 2));
        places = [];
        try {
          await recalculateEventPlacesForAllEvents();
        } catch (recalcErr) {
          console.error("⚠️ Events konnten nach Erstellen der Orte-Datei nicht neu berechnet werden:", recalcErr.message);
        }
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

  const normalizedList = list.map(sanitizePlaceEntry);
  const serialized = JSON.stringify(normalizedList, null, 2);
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

// ===== Aircraft storage =====
function normalizeAircraftHex(hex) {
  if (typeof hex !== "string") {
    return "";
  }
  return hex.trim().toLowerCase();
}

function sanitizeAircraftEntry(entry) {
  if (!entry || typeof entry !== "object") {
    return null;
  }

  const normalizedHex = normalizeAircraftHex(entry.hex);
  if (!normalizedHex) {
    return null;
  }

  const sanitized = { hex: normalizedHex };
  if (entry.name !== undefined && entry.name !== null) {
    const name = String(entry.name).trim();
    if (name) {
      sanitized.name = name;
    }
  }

  return sanitized;
}

function sanitizeAircraftList(list) {
  if (!Array.isArray(list)) {
    return [];
  }

  const sanitized = [];
  const seen = new Set();

  for (const entry of list) {
    const candidate = sanitizeAircraftEntry(entry);
    if (!candidate) {
      continue;
    }

    if (seen.has(candidate.hex)) {
      const index = sanitized.findIndex(item => item.hex === candidate.hex);
      if (index >= 0) {
        sanitized[index] = candidate;
      }
      continue;
    }

    seen.add(candidate.hex);
    sanitized.push(candidate);
  }

  return sanitized;
}

async function loadAircraftFromDisk() {
  try {
    const raw = await fsp.readFile(aircraftPath, "utf8");
    if (!raw.trim()) {
      aircraftProfiles = [];
      return;
    }

    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      console.warn("⚠️ aircraft.json enthält kein Array. Bestehende Werte bleiben erhalten.");
      return;
    }

    aircraftProfiles = sanitizeAircraftList(parsed);
  } catch (err) {
    if (err.code === "ENOENT") {
      try {
        await fsp.writeFile(aircraftPath, JSON.stringify([], null, 2));
      } catch (writeErr) {
        console.error("❌ aircraft.json konnte nicht erstellt werden:", writeErr.message);
      }
      aircraftProfiles = [];
    } else if (err.name === "SyntaxError") {
      console.error("❌ Ungültiges JSON in aircraft.json:", err.message);
    } else {
      console.error("❌ aircraft.json konnte nicht gelesen werden:", err.message);
    }
  }
}

function getAircraftList() {
  return Array.isArray(aircraftProfiles)
    ? aircraftProfiles.map(entry => ({ ...entry }))
    : [];
}

function getAircraftByHex(hex) {
  const normalized = normalizeAircraftHex(hex);
  if (!normalized || !Array.isArray(aircraftProfiles)) {
    return null;
  }

  const entry = aircraftProfiles.find(item => item && normalizeAircraftHex(item.hex) === normalized);
  return entry ? { ...entry } : null;
}

async function saveAircraft(list) {
  if (!Array.isArray(list)) {
    throw new TypeError("Aircraft list must be an array.");
  }

  const sanitized = sanitizeAircraftList(list);
  const serialized = JSON.stringify(sanitized, null, 2);
  const parsed = JSON.parse(serialized);
  aircraftProfiles = parsed;
  await fsp.writeFile(aircraftPath, serialized);
  return getAircraftList();
}

async function upsertAircraft(hex, name) {
  const normalizedHex = normalizeAircraftHex(hex);
  if (!normalizedHex) {
    throw new Error("Ungültiger Hex-Code.");
  }

  const trimmedName = typeof name === "string" ? name.trim() : "";
  if (!trimmedName) {
    throw new Error("Feld 'name' wird benötigt.");
  }

  const current = getAircraftList();
  const filtered = current.filter(entry => normalizeAircraftHex(entry.hex) !== normalizedHex);
  filtered.push({ hex: normalizedHex, name: trimmedName });
  await saveAircraft(filtered);
  return getAircraftByHex(normalizedHex);
}

async function deleteAircraft(hex) {
  const normalizedHex = normalizeAircraftHex(hex);
  if (!normalizedHex) {
    throw new Error("Ungültiger Hex-Code.");
  }

  const current = getAircraftList();
  const filtered = current.filter(entry => normalizeAircraftHex(entry && entry.hex) !== normalizedHex);

  if (filtered.length === current.length) {
    return false;
  }

  await saveAircraft(filtered);
  await cleanupAircraftArtifacts(normalizedHex, filtered);
  return true;
}

async function cleanupAircraftArtifacts(normalizedHex, remainingAircraft = []) {
  if (!normalizedHex) {
    return;
  }

  const logFilePath = path.join(logsDir, `${normalizedHex}.jsonl`);
  try {
    await fsp.unlink(logFilePath);
  } catch (err) {
    if (err.code !== "ENOENT") {
      console.error(`⚠️ Log-Datei konnte nicht entfernt werden (${normalizedHex}):`, err.message);
    }
  }

  const historyPath = path.join(historyLogsDir, normalizedHex);
  try {
    await fsp.rm(historyPath, { recursive: true, force: true });
  } catch (err) {
    if (err.code !== "ENOENT") {
      console.error(`⚠️ History-Verzeichnis konnte nicht entfernt werden (${normalizedHex}):`, err.message);
    }
  }

  delete logCounts[normalizedHex];
  delete lastLogRecords[normalizedHex];
  historyDownloadQueues.delete(normalizedHex);
  historyDownloadsInFlight.delete(normalizedHex);
  historyExistingDaysCache.delete(normalizedHex);
  delete flightStatus[normalizedHex];

  const eventsBefore = events.length;
  events = events.filter(event => normalizeAircraftHex(event && event.hex) !== normalizedHex);
  if (events.length !== eventsBefore) {
    await persistEvents();
  }

  const latestHex = latestData && latestData.hex ? normalizeAircraftHex(latestData.hex) : "";
  if (latestHex === normalizedHex) {
    latestData = {};
    try {
      await fsp.writeFile("latest.json", JSON.stringify(latestData, null, 2));
    } catch (err) {
      console.error("⚠️ latest.json konnte nicht gespeichert werden:", err.message);
    }
  }

  if (normalizeAircraftHex(targetHex) === normalizedHex) {
    const fallbackEntry = Array.isArray(remainingAircraft) && remainingAircraft.length > 0
      ? normalizeAircraftHex(remainingAircraft[0] && remainingAircraft[0].hex)
      : "";
    targetHex = fallbackEntry || "";
    await persistLastTargetHex(targetHex);
  }
}

async function persistLastTargetHex(hexValue) {
  const normalized = typeof hexValue === "string" && hexValue.trim()
    ? hexValue.trim().toLowerCase()
    : "";
  try {
    await fsp.writeFile("last_target.json", JSON.stringify({ hex: normalized }, null, 2));
  } catch (err) {
    console.error("⚠️ last_target.json konnte nicht gespeichert werden:", err.message);
  }
}

function startAircraftWatcher() {
  try {
    fs.watchFile(aircraftPath, { interval: 1000 }, () => {
      loadAircraftFromDisk().catch(err => {
        console.error("⚠️ aircraft.json konnte nicht neu geladen werden:", err.message);
      });
    });
  } catch (err) {
    console.error("⚠️ Beobachten von aircraft.json fehlgeschlagen:", err.message);
  }
}

function parseAircraftPayload(payload) {
  if (!payload || typeof payload !== "object") {
    throw new Error("Body muss ein Objekt sein.");
  }

  const name = typeof payload.name === "string" ? payload.name.trim() : "";
  if (!name) {
    throw new Error("Feld 'name' wird benötigt.");
  }

  return { name };
}

function sanitizeHistoryAuthHeader(value) {
  if (value === null || value === undefined) {
    return "";
  }

  if (typeof value !== "string") {
    return "";
  }

  return value.trim();
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

  const radius = toFiniteNumber(raw.placeMatchRadiusMeters);
  if (radius !== null && radius > 0) {
    normalized.placeMatchRadiusMeters = radius;
  }

  normalized.notificationsEnabled = raw.notificationsEnabled === true;
  normalized.notifyOnTakeoff = raw.notifyOnTakeoff !== false;
  normalized.notifyOnLanding = raw.notifyOnLanding !== false;

  if (Object.prototype.hasOwnProperty.call(raw, "adsbHistoryAuthHeader")) {
    normalized.adsbHistoryAuthHeader = sanitizeHistoryAuthHeader(raw.adsbHistoryAuthHeader);
  }

  return normalized;
}

function getConfig() {
  return { ...config };
}

function getOperationalConfig() {
  return normalizeConfig(config);
}

function getPlaceMatchRadiusMeters() {
  const radius = toFiniteNumber(config?.placeMatchRadiusMeters);
  if (radius !== null && radius > 0) {
    return radius;
  }
  return DEFAULT_CONFIG.placeMatchRadiusMeters;
}

function resolveConfiguredPlaceRadiusMeters(candidate) {
  const radius = toFiniteNumber(candidate?.placeMatchRadiusMeters);
  if (radius !== null && radius > 0) {
    return radius;
  }
  return DEFAULT_CONFIG.placeMatchRadiusMeters;
}

async function handleConfigRadiusChange(previousConfig, nextConfig) {
  const previousRadius = resolveConfiguredPlaceRadiusMeters(previousConfig);
  const nextRadius = resolveConfiguredPlaceRadiusMeters(nextConfig);

  if (previousRadius !== nextRadius) {
    await recalculateEventPlacesForAllEvents();
  }
}

async function loadConfigFromDisk() {
  const previousConfig = { ...config };
  try {
    const raw = await fsp.readFile(configPath, "utf8");

    if (!raw.trim()) {
      config = { ...DEFAULT_CONFIG };
      try {
        await fsp.writeFile(configPath, JSON.stringify(config, null, 2));
      } catch (writeErr) {
        console.error("❌ config.json konnte nicht erstellt werden:", writeErr.message);
      }
      await handleConfigRadiusChange(previousConfig, config);
      return;
    }

    const parsed = JSON.parse(raw);
    config = normalizeConfig(parsed);
    await handleConfigRadiusChange(previousConfig, config);
  } catch (err) {
    if (err.code === "ENOENT") {
      try {
        await fsp.writeFile(configPath, JSON.stringify(DEFAULT_CONFIG, null, 2));
      } catch (writeErr) {
        console.error("❌ config.json konnte nicht erstellt werden:", writeErr.message);
      }
      config = { ...DEFAULT_CONFIG };
      await handleConfigRadiusChange(previousConfig, config);
    } else if (err.name === "SyntaxError") {
      console.error("❌ Ungültiges JSON in config.json:", err.message);
      config = { ...DEFAULT_CONFIG };
      await handleConfigRadiusChange(previousConfig, config);
    } else {
      console.error("❌ config.json konnte nicht gelesen werden:", err.message);
      config = { ...DEFAULT_CONFIG };
      await handleConfigRadiusChange(previousConfig, config);
    }
  }
}

async function saveConfig(newConfig) {
  const previousConfig = { ...config };
  config = normalizeConfig(newConfig);
  await fsp.writeFile(configPath, JSON.stringify(config, null, 2));
  await handleConfigRadiusChange(previousConfig, config);
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

function generateEventId() {
  lastEventId += 1;
  return String(lastEventId);
}

function normalizeEventPlace(placeInfo) {
  if (placeInfo === null || placeInfo === undefined) {
    return null;
  }

  if (typeof placeInfo !== "object") {
    return placeInfo;
  }

  const snapshot = {};

  if (placeInfo.id !== undefined && placeInfo.id !== null) {
    snapshot.id = String(placeInfo.id);
  }

  if (placeInfo.name !== undefined) {
    snapshot.name = placeInfo.name;
  }

  if (placeInfo.type !== undefined) {
    snapshot.type = placeInfo.type;
  }

  if (placeInfo.source !== undefined) {
    snapshot.source = placeInfo.source;
  }

  if (placeInfo.displayName !== undefined) {
    snapshot.displayName = placeInfo.displayName;
  }

  const lat = toFiniteNumber(placeInfo.lat);
  if (lat !== null) {
    snapshot.lat = lat;
  }

  const lon = toFiniteNumber(placeInfo.lon);
  if (lon !== null) {
    snapshot.lon = lon;
  }

  if (placeInfo.city !== undefined) {
    snapshot.city = placeInfo.city;
  }

  if (placeInfo.country !== undefined) {
    snapshot.country = placeInfo.country;
  }

  if (placeInfo.state !== undefined) {
    snapshot.state = placeInfo.state;
  }

  if (placeInfo.osmId !== undefined && placeInfo.osmId !== null) {
    snapshot.osmId = String(placeInfo.osmId);
  }

  if (placeInfo.osmType !== undefined) {
    snapshot.osmType = placeInfo.osmType;
  }

  return snapshot;
}

// ===== OSM helpers =====
function sanitizeOsmString(value) {
  if (typeof value !== "string") {
    return "";
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : "";
}

function pickFirstNonEmpty(values) {
  for (const candidate of values) {
    const text = sanitizeOsmString(candidate);
    if (text) {
      return text;
    }
  }
  return "";
}

function buildOsmCacheKey(lat, lon) {
  const latNum = toFiniteNumber(lat);
  const lonNum = toFiniteNumber(lon);
  if (latNum === null || lonNum === null) {
    return null;
  }
  return `${latNum.toFixed(OSM_CACHE_COORD_PRECISION)},${lonNum.toFixed(OSM_CACHE_COORD_PRECISION)}`;
}

function getCachedOsmPlace(key) {
  if (!key || !OSM_REVERSE_CACHE.has(key)) {
    return undefined;
  }

  const entry = OSM_REVERSE_CACHE.get(key);
  if (!entry || (entry.expiresAt && entry.expiresAt <= Date.now())) {
    OSM_REVERSE_CACHE.delete(key);
    return undefined;
  }

  if (!entry.place) {
    return null;
  }

  return { ...entry.place };
}

function setCachedOsmPlace(key, place) {
  if (!key) {
    return;
  }

  OSM_REVERSE_CACHE.set(key, {
    place: place ? { ...place } : null,
    expiresAt: Date.now() + OSM_CACHE_TTL_MS
  });

  if (OSM_REVERSE_CACHE.size > OSM_CACHE_MAX_SIZE) {
    const oldestKey = OSM_REVERSE_CACHE.keys().next().value;
    if (oldestKey !== undefined) {
      OSM_REVERSE_CACHE.delete(oldestKey);
    }
  }
}

function normalizeOsmPlaceResponse(data, lat, lon) {
  if (!data || typeof data !== "object") {
    return null;
  }

  const address = data.address && typeof data.address === "object" ? data.address : {};

  const name = pickFirstNonEmpty([
    data.name,
    address.amenity,
    address.building,
    address.leisure,
    address.tourism,
    address.shop,
    address.office,
    address.hospital,
    address.university,
    address.school,
    address.college,
    address.library,
    address.museum,
    address.theatre,
    address.attraction,
    address.aeroway,
    address.airport,
    address.helipad,
    address.bus_station,
    address.train_station,
    address.suburb,
    address.neighbourhood,
    address.quarter,
    address.road,
    address.village,
    address.town
  ]);

  const city = pickFirstNonEmpty([
    address.city,
    address.town,
    address.village,
    address.municipality,
    address.county,
    address.state
  ]);

  const displayName = sanitizeOsmString(data.display_name);
  const country = pickFirstNonEmpty([address.country]);
  const state = pickFirstNonEmpty([address.state]);

  if (!name && !city && !displayName) {
    return null;
  }

  const latNum = toFiniteNumber(lat);
  const lonNum = toFiniteNumber(lon);

  const place = { type: "external", source: "osm" };

  if (latNum !== null) {
    place.lat = latNum;
  }

  if (lonNum !== null) {
    place.lon = lonNum;
  }

  if (name) {
    place.name = name;
  }

  if (city && (!name || city.toLowerCase() !== name.toLowerCase())) {
    place.city = city;
  }

  if (country) {
    place.country = country;
  }

  if (state && (!place.state || place.state.toLowerCase() !== state.toLowerCase())) {
    place.state = state;
  }

  if (displayName) {
    place.displayName = displayName;
  }

  if (data.osm_id !== undefined && data.osm_id !== null) {
    place.osmId = String(data.osm_id);
  }

  const osmType = sanitizeOsmString(data.osm_type);
  if (osmType) {
    place.osmType = osmType;
  }

  return place;
}

async function fetchNearestOsmPlace(lat, lon) {
  const key = buildOsmCacheKey(lat, lon);
  if (!key) {
    return null;
  }

  const cached = getCachedOsmPlace(key);
  if (cached !== undefined) {
    return cached ? { ...cached } : null;
  }

  const latNum = toFiniteNumber(lat);
  const lonNum = toFiniteNumber(lon);
  if (latNum === null || lonNum === null) {
    setCachedOsmPlace(key, null);
    return null;
  }

  const params = new URLSearchParams({
    format: "jsonv2",
    lat: String(latNum),
    lon: String(lonNum),
    zoom: "15",
    addressdetails: "1"
  });

  try {
    const response = await fetch(`${OSM_REVERSE_URL}?${params.toString()}`, {
      headers: {
        "User-Agent": OSM_USER_AGENT,
        "Accept-Language": OSM_DEFAULT_LANGUAGE
      }
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();
    const place = normalizeOsmPlaceResponse(data, latNum, lonNum);
    setCachedOsmPlace(key, place);
    return place ? { ...place } : null;
  } catch (err) {
    console.warn("⚠️ OpenStreetMap Reverse Lookup fehlgeschlagen:", err.message);
    setCachedOsmPlace(key, null);
    return null;
  }
}

async function persistEvents() {
  try {
    await fsp.writeFile("events.json", JSON.stringify(events, null, 2));
  } catch (err) {
    console.error("⚠️ events.json konnte nicht gespeichert werden:", err.message);
  }
}

function removeEventStreamClient(client) {
  if (!client) {
    return;
  }

  if (client.heartbeat) {
    clearInterval(client.heartbeat);
  }

  eventStreamClients.delete(client);

  try {
    client.res.end();
  } catch (err) {
    // ignore
  }
}

function broadcastEventToStream(event) {
  if (!event || eventStreamClients.size === 0) {
    return;
  }

  const payload = JSON.stringify({ event });
  const chunk = `event: event\nid: ${event.id}\ndata: ${payload}\n\n`;

  for (const client of [...eventStreamClients]) {
    try {
      client.res.write(chunk);
    } catch (err) {
      console.warn("[SSE] Clientverbindung beendet:", err.message);
      removeEventStreamClient(client);
    }
  }
}

function handleEventStreamRequest(req, res) {
  if (req.method && req.method.toUpperCase() !== "GET") {
    sendError(res, 405, "Methode nicht erlaubt.");
    return;
  }

  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive"
  });

  if (typeof res.flushHeaders === "function") {
    res.flushHeaders();
  }

  res.write(`retry: ${EVENT_STREAM_HEARTBEAT_INTERVAL_MS}\n`);
  res.write(`event: init\ndata: ${JSON.stringify({ lastEventId })}\n\n`);

  const client = {
    res,
    heartbeat: setInterval(() => {
      try {
        res.write(`:keep-alive ${Date.now()}\n\n`);
      } catch (err) {
        console.warn("[SSE] Heartbeat fehlgeschlagen:", err.message);
        removeEventStreamClient(client);
      }
    }, EVENT_STREAM_HEARTBEAT_INTERVAL_MS)
  };

  eventStreamClients.add(client);

  const closeHandler = () => {
    removeEventStreamClient(client);
    req.removeListener("close", closeHandler);
    res.removeListener("close", closeHandler);
    res.removeListener("error", closeHandler);
  };

  req.on("close", closeHandler);
  res.on("close", closeHandler);
  res.on("error", closeHandler);
}

function eventPlaceMatchesTarget(eventPlace, { targetId, refLat, refLon, refName, radiusMeters }) {
  if (!eventPlace) {
    return false;
  }

  const comparisonRadius = toFiniteNumber(radiusMeters);
  const fallbackRadius = comparisonRadius !== null && comparisonRadius > 0
    ? comparisonRadius
    : getPlaceMatchRadiusMeters();

  if (typeof eventPlace === "object") {
    const candidateId = eventPlace.id !== undefined && eventPlace.id !== null
      ? String(eventPlace.id)
      : null;
    if (targetId && candidateId && candidateId === targetId) {
      return true;
    }

    const lat = toFiniteNumber(eventPlace.lat);
    const lon = toFiniteNumber(eventPlace.lon);
    if (refLat !== null && refLon !== null && lat !== null && lon !== null) {
      const distance = haversine(lat, lon, refLat, refLon);
      if (Number.isFinite(distance) && distance <= fallbackRadius) {
        return true;
      }
    }

    const candidateName = typeof eventPlace.name === "string"
      ? eventPlace.name.trim().toLowerCase()
      : "";
    if (refName && candidateName && candidateName === refName) {
      return true;
    }
  } else if (typeof eventPlace === "string" && refName) {
    if (eventPlace.trim().toLowerCase() === refName) {
      return true;
    }
  }

  return false;
}

async function applyPlaceUpdateToEvents(originalPlace, updatedPlace) {
  if (!Array.isArray(events) || events.length === 0) {
    return;
  }

  const normalizedUpdated = normalizeEventPlace(updatedPlace);
  const normalizedOriginal = normalizeEventPlace(originalPlace);
  const reference = normalizedUpdated || normalizedOriginal;
  const referenceSource = updatedPlace || originalPlace || null;

  if (!reference) {
    return;
  }

  const targetId = reference.id ? String(reference.id) : null;
  const refLat = normalizedOriginal && normalizedOriginal.lat !== undefined
    ? toFiniteNumber(normalizedOriginal.lat)
    : (normalizedUpdated ? toFiniteNumber(normalizedUpdated.lat) : null);
  const refLon = normalizedOriginal && normalizedOriginal.lon !== undefined
    ? toFiniteNumber(normalizedOriginal.lon)
    : (normalizedUpdated ? toFiniteNumber(normalizedUpdated.lon) : null);
  const refName = normalizedOriginal && normalizedOriginal.name
    ? String(normalizedOriginal.name).trim().toLowerCase()
    : (normalizedUpdated && normalizedUpdated.name
        ? String(normalizedUpdated.name).trim().toLowerCase()
        : "");

  const radiusOverride = referenceSource ? getEffectiveRadiusMetersForPlace(referenceSource) : null;
  const criteria = { targetId, refLat, refLon, refName, radiusMeters: radiusOverride };
  const replacement = normalizedUpdated ? { ...normalizedUpdated } : null;
  let changed = false;

  for (const event of events) {
    if (!event || typeof event !== "object" || !event.place) {
      continue;
    }

    if (eventPlaceMatchesTarget(event.place, criteria)) {
      event.place = replacement;
      changed = true;
    }
  }

  if (changed) {
    await persistEvents();
  }
}

function normalizeEventPlaceSnapshot(placeInfo) {
  const normalized = normalizeEventPlace(placeInfo);
  if (normalized === null || normalized === undefined) {
    return null;
  }

  if (typeof normalized === "string") {
    const trimmed = normalized.trim();
    return trimmed ? trimmed : null;
  }

  if (typeof normalized !== "object") {
    return null;
  }

  const keys = Object.keys(normalized);
  if (keys.length === 0) {
    return null;
  }

  return normalized;
}

function areEventPlaceSnapshotsEqual(a, b) {
  if (!a && !b) {
    return true;
  }

  if (!a || !b) {
    return false;
  }

  if (typeof a === "string" || typeof b === "string") {
    return a === b;
  }

  return JSON.stringify(a) === JSON.stringify(b);
}

async function recalculateEventPlacesForAllEvents() {
  if (eventPlaceRecalculationPromise) {
    return eventPlaceRecalculationPromise;
  }

  eventPlaceRecalculationPromise = (async () => {
    if (!Array.isArray(events) || events.length === 0) {
      return false;
    }

    let changed = false;

    for (const event of events) {
      if (!event || typeof event !== "object") {
        continue;
      }

      const candidatePlace = await determinePlaceForRecord(event);
      const candidateSnapshot = normalizeEventPlaceSnapshot(candidatePlace);
      const existingSnapshot = Object.prototype.hasOwnProperty.call(event, "place")
        ? normalizeEventPlaceSnapshot(event.place)
        : null;

      if (!areEventPlaceSnapshotsEqual(existingSnapshot, candidateSnapshot)) {
        if (candidateSnapshot) {
          event.place = candidateSnapshot;
        } else if (Object.prototype.hasOwnProperty.call(event, "place")) {
          delete event.place;
        }
        changed = true;
      }
    }

    if (changed) {
      await persistEvents();
    }

    return changed;
  })();

  try {
    return await eventPlaceRecalculationPromise;
  } finally {
    eventPlaceRecalculationPromise = null;
  }
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

  let includeRadius = false;
  let matchRadiusMeters = null;

  if (Object.prototype.hasOwnProperty.call(payload, "matchRadiusMeters")) {
    includeRadius = true;
    const rawRadius = payload.matchRadiusMeters;
    if (rawRadius === null || rawRadius === "" || rawRadius === undefined) {
      matchRadiusMeters = null;
    } else {
      const parsedRadius = toFiniteNumber(rawRadius);
      if (parsedRadius === null || parsedRadius <= 0) {
        throw new Error("Feld 'matchRadiusMeters' muss größer als 0 sein.");
      }
      matchRadiusMeters = parsedRadius;
    }
  }

  const result = { name, type, lat, lon };
  if (includeRadius) {
    if (matchRadiusMeters === null) {
      result.matchRadiusMeters = null;
    } else {
      result.matchRadiusMeters = matchRadiusMeters;
    }
  }

  return result;
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

  const radius = toFiniteNumber(payload.placeMatchRadiusMeters);
  if (radius === null || radius <= 0) {
    throw new Error("Feld 'placeMatchRadiusMeters' muss größer als 0 sein.");
  }

  const currentConfig = getConfig();
  const notificationsEnabled = payload.notificationsEnabled !== undefined
    ? payload.notificationsEnabled === true
    : !!currentConfig.notificationsEnabled;
  const notifyOnTakeoff = payload.notifyOnTakeoff !== undefined
    ? payload.notifyOnTakeoff !== false
    : currentConfig.notifyOnTakeoff !== false;
  const notifyOnLanding = payload.notifyOnLanding !== undefined
    ? payload.notifyOnLanding !== false
    : currentConfig.notifyOnLanding !== false;
  let adsbHistoryAuthHeader = sanitizeHistoryAuthHeader(currentConfig.adsbHistoryAuthHeader);

  if (Object.prototype.hasOwnProperty.call(payload, "adsbHistoryAuthHeader")) {
    const rawHeader = payload.adsbHistoryAuthHeader;
    if (rawHeader === null || rawHeader === undefined) {
      adsbHistoryAuthHeader = "";
    } else if (typeof rawHeader === "string") {
      adsbHistoryAuthHeader = rawHeader.trim();
    } else {
      throw new Error("Feld 'adsbHistoryAuthHeader' muss eine Zeichenkette sein oder leer gelassen werden.");
    }
  }

  return {
    altitudeThresholdFt: altitude,
    speedThresholdKt: speed,
    offlineTimeoutSec: Math.round(timeout),
    placeMatchRadiusMeters: radius,
    notificationsEnabled,
    notifyOnTakeoff,
    notifyOnLanding,
    adsbHistoryAuthHeader
  };
}

async function initializeHistoryCache() {
  historyExistingDaysCache.clear();

  try {
    const entries = await fsp.readdir(historyLogsDir, { withFileTypes: true });
    for (const entry of entries) {
      if (!entry.isDirectory()) {
        continue;
      }

      const normalizedHex = normalizeAircraftHex(entry.name);
      if (!normalizedHex) {
        continue;
      }

      const hexDirPath = path.join(historyLogsDir, entry.name);
      const daySet = new Set();
      try {
        const files = await fsp.readdir(hexDirPath);
        for (const file of files) {
          if (file.toLowerCase().endsWith(".json")) {
            daySet.add(path.basename(file, ".json"));
          }
        }
      } catch (err) {
        console.error(`⚠️ Verlaufseinträge konnten nicht gelesen werden (${entry.name}):`, err.message);
      }

      historyExistingDaysCache.set(normalizedHex, daySet);
    }
  } catch (err) {
    if (err && err.code !== "ENOENT") {
      console.error("⚠️ History-Verzeichnis konnte nicht gelesen werden:", err.message);
    }
  }
}

// ===== State loading =====
async function initializeState() {
  await fsp.mkdir(logsDir, { recursive: true });
  await fsp.mkdir(historyLogsDir, { recursive: true });
  await initializeHistoryCache();

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
        if (lines.length > 0) {
          const lastLine = lines[lines.length - 1];
          try {
            lastLogRecords[hex] = JSON.parse(lastLine);
          } catch (parseErr) {
            console.warn("⚠️ Letzter Log-Eintrag konnte nicht geparst werden für", hex, parseErr.message);
          }
        }
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
      let maxExistingId = 0;
      for (const entry of parsed) {
        const numericId = Number(entry && entry.id);
        if (Number.isFinite(numericId) && numericId > maxExistingId) {
          maxExistingId = numericId;
        }
      }

      lastEventId = maxExistingId;

      const sanitized = [];
      let requiresPersist = false;

      for (const entry of parsed) {
        if (!entry || typeof entry !== "object") {
          continue;
        }

        if (entry.id === undefined || entry.id === null || entry.id === "") {
          entry.id = generateEventId();
          requiresPersist = true;
        } else {
          entry.id = String(entry.id);
        }

        if (Object.prototype.hasOwnProperty.call(entry, "place")) {
          entry.place = normalizeEventPlace(entry.place);
        }

        sanitized.push(entry);
      }

      events = sanitized;

      if (requiresPersist) {
        await persistEvents();
      }
    } else {
      events = [];
      lastEventId = 0;
    }
  } catch (err) {
    if (err.code !== "ENOENT") {
      console.error("⚠️ events.json konnte nicht geladen werden:", err.message);
    }
    events = [];
    lastEventId = 0;
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

  await loadAircraftFromDisk().catch(err => {
    console.error("⚠️ aircraft.json konnte nicht geladen werden:", err.message);
  });
  startAircraftWatcher();

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

  const enqueue = function enqueue(fn) {
    return new Promise((resolve, reject) => {
      queue.push({ fn, resolve, reject });
      void runNext();
    });
  };

  enqueue.clear = () => {
    queue.length = 0;
  };

  return enqueue;
}

const runWithPage = createSerialTaskQueue();

// ===== History download support =====
function delay(ms) {
  const numeric = Number(ms);
  const safeDelay = Number.isFinite(numeric) && numeric > 0 ? numeric : 0;
  return new Promise(resolve => setTimeout(resolve, safeDelay));
}

function formatDateForHistory(date) {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

async function ensureHistoryDirectory(hex) {
  const normalizedHex = normalizeAircraftHex(hex);
  if (!normalizedHex) {
    throw new Error("Ungültiger Hex-Code für History-Verzeichnis.");
  }

  await fsp.mkdir(historyLogsDir, { recursive: true });
  const hexDir = path.join(historyLogsDir, normalizedHex);
  await fsp.mkdir(hexDir, { recursive: true });

  let existingDays = historyExistingDaysCache.get(normalizedHex);
  if (!existingDays) {
    existingDays = new Set();
    historyExistingDaysCache.set(normalizedHex, existingDays);
    try {
      const files = await fsp.readdir(hexDir);
      for (const file of files) {
        if (file.toLowerCase().endsWith(".json")) {
          existingDays.add(path.basename(file, ".json"));
        }
      }
    } catch (err) {
      console.error("⚠️ Verlaufseinträge konnten nicht gelesen werden für", normalizedHex, err.message);
    }
  }

  return { directory: hexDir, existingDays };
}

function buildHistoryUrl(hex, dateString) {
  return `https://globe.adsbexchange.com/globe_history/data/${dateString}/traces/icao/${hex}_trace_full.json`;
}

function parseHistoryAuthHeaderConfig(rawHeader) {
  const sanitized = sanitizeHistoryAuthHeader(rawHeader);
  if (!sanitized) {
    return null;
  }

  const lines = sanitized.split(/\r?\n/).map(line => line.trim()).filter(Boolean);
  if (lines.length === 0) {
    return null;
  }

  const headers = {};
  const cookieParts = [];

  for (const line of lines) {
    const separatorIndex = line.indexOf(":");
    if (separatorIndex === -1) {
      cookieParts.push(line);
      continue;
    }

    const name = line.slice(0, separatorIndex).trim();
    const value = line.slice(separatorIndex + 1).trim();

    if (!name || !value) {
      continue;
    }

    if (name.toLowerCase() === "referer") {
      continue;
    }

    headers[name] = value;
  }

  if (cookieParts.length > 0) {
    const combinedCookie = cookieParts.join("; ");
    if (headers.Cookie) {
      headers.Cookie = `${headers.Cookie}; ${combinedCookie}`;
    } else {
      headers.Cookie = combinedCookie;
    }
  }

  return Object.keys(headers).length > 0 ? headers : null;
}

function buildHistoryRequestHeaders() {
  const headers = { Referer: ADSB_REFERER };
  const configured = parseHistoryAuthHeaderConfig(config?.adsbHistoryAuthHeader);

  if (configured) {
    for (const [key, value] of Object.entries(configured)) {
      if (!key || value === undefined || value === null || value === "") {
        continue;
      }

      if (String(key).toLowerCase() === "referer") {
        continue;
      }

      headers[key] = value;
    }
  }

  return headers;
}

async function downloadHistoryForHex(hex, days = 14) {
  const normalizedHex = normalizeAircraftHex(hex);
  if (!normalizedHex) {
    return;
  }

  let directoryInfo;
  try {
    directoryInfo = await ensureHistoryDirectory(normalizedHex);
  } catch (err) {
    console.error("⚠️ Verlaufverzeichnis konnte nicht vorbereitet werden für", normalizedHex, err.message);
    return;
  }

  const { directory, existingDays } = directoryInfo;
  const now = new Date();

  for (let offset = 0; offset < days; offset++) {
    const targetDate = new Date(now);
    targetDate.setUTCDate(targetDate.getUTCDate() - (offset + 1));
    const dateString = formatDateForHistory(targetDate);
    const fileName = `${dateString}.json`;
    const filePath = path.join(directory, fileName);

    if (!existingDays.has(dateString)) {
      try {
        await fsp.access(filePath, fs.constants.F_OK);
        existingDays.add(dateString);
      } catch (err) {
        if (err.code !== "ENOENT") {
          console.error(`⚠️ Verlauf-Datei konnte nicht überprüft werden (${normalizedHex} ${dateString}):`, err.message);
        }
      }
    }

    if (existingDays.has(dateString)) {
      continue;
    }

    const historyUrl = buildHistoryUrl(normalizedHex, dateString);
    let performedRequestForDay = false;

    while (true) {
      let response;
      try {
        response = await fetch(historyUrl, {
          headers: buildHistoryRequestHeaders()
        });
        performedRequestForDay = true;
      } catch (err) {
        console.error(`⚠️ Verlauf-Download fehlgeschlagen (${normalizedHex} ${dateString}):`, err.message);
        break;
      }

      if (response.status === 429) {
        console.warn(`⚠️ Verlauf-Download rate-limitiert (${normalizedHex} ${dateString}). Warte ${HISTORY_RATELIMIT_WAIT_MS} ms.`);
        await delay(HISTORY_RATELIMIT_WAIT_MS);
        continue;
      }

      if (response.status === 200) {
        try {
          const body = await response.text();
          await fsp.writeFile(filePath, body, "utf8");
          existingDays.add(dateString);
          console.log(`💾 Verlauf gespeichert für ${normalizedHex} (${dateString}).`);
        } catch (err) {
          console.error(`❌ Verlauf-Datei konnte nicht gespeichert werden (${normalizedHex} ${dateString}):`, err.message);
        }
      } else {
        console.warn(`⚠️ Verlauf-Download für ${normalizedHex} (${dateString}) mit Status ${response.status}.`);
      }

      break;
    }

    if (offset < days - 1 && performedRequestForDay) {
      await delay(HISTORY_REQUEST_INTERVAL_MS);
    }
  }
}

function getHistoryDownloadQueue(hex) {
  let queue = historyDownloadQueues.get(hex);
  if (!queue) {
    queue = createSerialTaskQueue();
    historyDownloadQueues.set(hex, queue);
  }
  return queue;
}

function queueHistoryDownload(hex) {
  const normalizedHex = normalizeAircraftHex(hex);
  if (!normalizedHex) {
    return;
  }

  if (historyDownloadsInFlight.has(normalizedHex)) {
    return;
  }

  const queue = getHistoryDownloadQueue(normalizedHex);
  historyDownloadsInFlight.add(normalizedHex);

  queue(async () => {
    try {
      await downloadHistoryForHex(normalizedHex);
    } catch (err) {
      console.error(`⚠️ Verlauf konnte nicht heruntergeladen werden für ${normalizedHex}:`, err.message);
    } finally {
      historyDownloadsInFlight.delete(normalizedHex);
    }
  }).catch(err => {
    historyDownloadsInFlight.delete(normalizedHex);
    console.error(`⚠️ Verlauf-Warteschlange schlug fehl für ${normalizedHex}:`, err.message);
  });
}

function getTargetUrl(hexValue = targetHex) {
  const fallback = typeof targetHex === "string" && targetHex.trim()
    ? targetHex.trim().toLowerCase()
    : "3e0fe9";
  const candidate = typeof hexValue === "string" && hexValue.trim()
    ? hexValue.trim().toLowerCase()
    : fallback;
  return `${ADSB_BASE_URL}${encodeURIComponent(candidate)}`;
}

function applyPageDefaults(pageInstance) {
  if (!pageInstance) {
    return;
  }
  if (typeof pageInstance.setDefaultTimeout === "function") {
    pageInstance.setDefaultTimeout(PAGE_DEFAULT_TIMEOUT_MS);
  }
  if (typeof pageInstance.setDefaultNavigationTimeout === "function") {
    pageInstance.setDefaultNavigationTimeout(NAVIGATION_TIMEOUT_MS);
  }
}

async function runPageOperation(operation, { timeoutMs = PAGE_OPERATION_TIMEOUT_MS } = {}) {
  if (typeof operation !== "function") {
    throw new TypeError("operation must be a function returning a promise");
  }

  const operationPromise = Promise.resolve().then(operation);

  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    return operationPromise;
  }

  let timeoutId;
  let timedOut = false;

  const timeoutPromise = new Promise((_, reject) => {
    timeoutId = setTimeout(() => {
      timedOut = true;
      const timeoutError = new Error(`Page operation timed out after ${timeoutMs} ms`);
      timeoutError.code = PAGE_OPERATION_TIMEOUT_CODE;
      reject(timeoutError);
    }, timeoutMs);
  });

  try {
    return await Promise.race([operationPromise, timeoutPromise]);
  } catch (err) {
    if (timedOut) {
      operationPromise.catch(innerErr => {
        if (!innerErr) {
          return;
        }
        const message = typeof innerErr.message === "string" ? innerErr.message : String(innerErr);
        console.warn("⚠️ Seite meldete verspäteten Fehler:", message);
      });
    }
    throw err;
  } finally {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
  }
}

async function navigateToTarget({ hex = targetHex, waitUntil = NAVIGATION_WAIT_UNTIL, timeoutMs = NAVIGATION_TIMEOUT_MS, browserPage = null } = {}) {
  const activePage = browserPage ?? page;
  if (!activePage) {
    throw new Error("Browserseite ist nicht initialisiert.");
  }

  const url = getTargetUrl(hex);
  await activePage.goto(url, { waitUntil, timeout: timeoutMs });
  return url;
}

function isTimeoutLikeError(err) {
  if (!err) {
    return false;
  }

  if (err.code === PAGE_OPERATION_TIMEOUT_CODE) {
    return true;
  }

  if (puppeteer && puppeteer.errors && typeof puppeteer.errors.TimeoutError === "function" && err instanceof puppeteer.errors.TimeoutError) {
    return true;
  }

  const message = typeof err.message === "string" ? err.message : "";
  if (!message) {
    return false;
  }

  return /timed?\s*out/i.test(message) || /protocolTimeout/i.test(message);
}

async function rebuildPage(reason = "timeout") {
  if (!browser) {
    console.warn("⚠️ Kein Browser verfügbar, starte Browser neu...");
    await startBrowser();
    if (!browser) {
      throw new Error("Browser konnte nicht neu gestartet werden.");
    }
    return;
  }

  await runWithPage(async () => {
    const previousPage = page;
    page = null;

    if (previousPage) {
      try {
        await previousPage.close({ runBeforeUnload: true });
      } catch (err) {
        console.warn("⚠️ Alte Browserseite konnte nicht geschlossen werden:", err.message);
      }
    }

    let newPage;
    try {
      newPage = await browser.newPage();
      applyPageDefaults(newPage);
      await navigateToTarget({ browserPage: newPage });
      page = newPage;
    } catch (err) {
      if (newPage) {
        try {
          await newPage.close({ runBeforeUnload: true });
        } catch (closeErr) {
          console.warn("⚠️ Neue Browserseite konnte nach Fehler nicht geschlossen werden:", closeErr.message);
        }
      }
      throw err;
    }
  });

  console.log(`🔄 Browserseite neu initialisiert (${reason}):`, targetHex);

  if (scrapeLoopActive) {
    scheduleNextScrape(0);
  }
}

async function closeBrowserInstance(instance, { timeoutMs = BROWSER_CLOSE_TIMEOUT_MS } = {}) {
  if (!instance) {
    return;
  }

  const closeOperation = Promise.resolve().then(() => instance.close());

  if (Number.isFinite(timeoutMs) && timeoutMs > 0) {
    let timeoutId;

    const timeoutPromise = new Promise((_, reject) => {
      timeoutId = setTimeout(() => {
        reject(new Error(`Browser close timed out after ${timeoutMs} ms`));
      }, timeoutMs);
    });

    try {
      await Promise.race([closeOperation, timeoutPromise]);
    } catch (err) {
      const message = typeof err?.message === "string" ? err.message : String(err);
      console.warn("⚠️ Browser konnte nicht geschlossen werden:", message);
      const proc = typeof instance.process === "function" ? instance.process() : null;
      if (proc && !proc.killed) {
        try {
          proc.kill("SIGKILL");
          console.warn("⚠️ Browser-Prozess wurde hart beendet.");
        } catch (killErr) {
          console.warn("⚠️ Browser-Prozess konnte nicht hart beendet werden:", killErr.message);
        }
      }
    } finally {
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
    }
  } else {
    try {
      await closeOperation;
    } catch (err) {
      const message = typeof err?.message === "string" ? err.message : String(err);
      console.warn("⚠️ Browser konnte nicht geschlossen werden:", message);
      const proc = typeof instance.process === "function" ? instance.process() : null;
      if (proc && !proc.killed) {
        try {
          proc.kill("SIGKILL");
          console.warn("⚠️ Browser-Prozess wurde hart beendet.");
        } catch (killErr) {
          console.warn("⚠️ Browser-Prozess konnte nicht hart beendet werden:", killErr.message);
        }
      }
    }
  }
}

async function attemptPageRecovery(reason = "timeout", { forceBrowserRestart = false } = {}) {
  if (pageRecoveryInProgress) {
    return;
  }

  const restartBrowserInstance = async (logMessage) => {
    if (logMessage) {
      console.warn(logMessage);
    }

    if (browser) {
      if (typeof runWithPage.clear === "function") {
        runWithPage.clear();
      }
      await closeBrowserInstance(browser);
    }

    browser = null;
    page = null;
    consecutiveTimeoutCount = 0;

    await startBrowser();
  };

  pageRecoveryInProgress = true;
  try {
    if (forceBrowserRestart) {
      await restartBrowserInstance(`♻️ Browser-Neustart nach Anforderung (${reason}).`);
      return;
    }

    await rebuildPage(reason);
  } catch (err) {
    const message = typeof err?.message === "string" ? err.message : String(err);
    console.error("❌ Wiederherstellung der Seite fehlgeschlagen:", message);
    await restartBrowserInstance(`♻️ Browser-Neustart nach Fehler (${reason}).`);
  } finally {
    pageRecoveryInProgress = false;
  }
}

const SCRAPE_INTERVAL_MS = 3000;
let scrapeTimer = null;
let scrapeLoopActive = false;

// ===== Helpers =====
function cleanNum(value) {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }

  const str = String(value).trim();
  if (!str) {
    return null;
  }

  const normalized = str.replace(/,/g, ".").replace(/[^0-9+\-.]/g, "");
  if (!normalized) {
    return null;
  }

  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function pickFirstNumber(...values) {
  for (const value of values) {
    const parsed = cleanNum(value);
    if (parsed !== null) {
      return parsed;
    }
  }
  return null;
}

function pickFirstString(...values) {
  for (const value of values) {
    if (typeof value !== "string") {
      continue;
    }
    const trimmed = value.trim();
    if (trimmed) {
      return trimmed;
    }
  }
  return "";
}

function extractHexFromDisplay(raw) {
  if (raw === null || raw === undefined) {
    return null;
  }

  const text = String(raw).trim();
  if (!text) {
    return null;
  }

  const cleaned = text
    .replace(/\b(hex|icao|icao24|mode\s*s)\b\s*:?/gi, " ")
    .replace(/[^0-9a-f]/gi, " ")
    .trim();

  const parts = cleaned.split(/\s+/);
  for (const part of parts) {
    if (/^[0-9a-f]{6}$/i.test(part)) {
      return part.toLowerCase();
    }
  }

  if (/^[0-9a-f]{6}$/i.test(text)) {
    return text.toLowerCase();
  }

  return null;
}

function parsePos(raw) {
  if (!raw || !raw.includes(",")) return { lat: null, lon: null };
  const [lat, lon] = raw.split(",").map(s => s.replace(/[^\d\.\-]/g, "").trim());
  return { lat: lat ? Number(lat) : null, lon: lon ? Number(lon) : null };
}

function parseLastSeen(raw) {
  if (raw === null || raw === undefined) {
    return null;
  }

  if (typeof raw === "number") {
    return Number.isFinite(raw) ? raw : null;
  }

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

function getPlaceSpecificRadiusMeters(place) {
  if (!place || typeof place !== "object") {
    return null;
  }

  if (!Object.prototype.hasOwnProperty.call(place, "matchRadiusMeters")) {
    return null;
  }

  const radius = toFiniteNumber(place.matchRadiusMeters);
  if (radius !== null && radius > 0) {
    return radius;
  }

  return null;
}

function getEffectiveRadiusMetersForPlace(place) {
  const specific = getPlaceSpecificRadiusMeters(place);
  if (specific !== null) {
    return specific;
  }
  return getPlaceMatchRadiusMeters();
}

async function determinePlaceForRecord(record) {
  if (!record || typeof record !== "object") {
    return { type: "external" };
  }

  const lat = toFiniteNumber(record.lat);
  const lon = toFiniteNumber(record.lon);

  if (lat === null || lon === null) {
    return { type: "external" };
  }

  const list = getPlaces();
  if (Array.isArray(list) && list.length > 0) {
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

      const effectiveRadius = getEffectiveRadiusMetersForPlace(place);
      if (distance <= effectiveRadius && distance < nearestDistance) {
        nearestDistance = distance;
        nearest = { place, lat: placeLat, lon: placeLon };
      }
    }

    if (nearest) {
      const { place, lat: placeLat, lon: placeLon } = nearest;
      const name =
        typeof place.name === "string" && place.name.trim()
          ? place.name.trim()
          : (typeof place.id !== "undefined" ? String(place.id) : "Unbenannter Ort");
      const type =
        typeof place.type === "string" && place.type.trim()
          ? place.type
          : "unknown";

      const snapshot = { name, lat: placeLat, lon: placeLon, type, source: "user" };
      if (place.id !== undefined && place.id !== null) {
        snapshot.id = String(place.id);
      }
      if (place.city !== undefined) {
        snapshot.city = place.city;
      }
      if (place.country !== undefined) {
        snapshot.country = place.country;
      }
      if (place.state !== undefined) {
        snapshot.state = place.state;
      }
      return snapshot;
    }
  }

  const fallback = await fetchNearestOsmPlace(lat, lon);
  return fallback || { type: "external" };
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

  event.id = generateEventId();

  if (options && Object.prototype.hasOwnProperty.call(options, "place")) {
    const placeInfo = options.place;
    event.place = normalizeEventPlace(placeInfo);
  }
  events.push(event);
  await persistEvents();
  broadcastEventToStream(event);
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
        const place = await determinePlaceForRecord(record);
        await registerEvent("landing", record, { place });
      }
    }
  }

  if (state.pendingTakeoff) {
    const { changeTime, skipInitialAirborne } = state.pendingTakeoff;

    if (timestamp - changeTime <= EVENT_WINDOW_MS) {
      if (exceedsAltitude || exceedsSpeed) {
        if (!skipInitialAirborne) {
          const place = await determinePlaceForRecord(record);
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

  const data = await runPageOperation(() => page.evaluate(() => {
    const textContent = selector => {
      const element = document.querySelector(selector);
      if (!element) {
        return null;
      }
      const value = element.textContent;
      return typeof value === "string" ? value.trim() : null;
    };

    const findSelectedAircraft = () => {
      const candidates = [
        window?.selectedAircraft,
        window?.selectedAc,
        window?.ac,
        window?.selected
      ];

      for (const candidate of candidates) {
        if (candidate && typeof candidate === "object") {
          return candidate;
        }
      }
      return null;
    };

    const readLastSeen = (selectedCandidate) => {
      const seenElements = [
        document.querySelector("#selected_seen_pos"),
        document.querySelector("#selected_seen"),
        document.querySelector('[data-testid="selected-seen"]'),
        document.querySelector('[data-testid="selectedSeen"]')
      ];

      let lastSeenText = null;
      let lastSeenSeconds = null;

      for (const element of seenElements) {
        if (!element) {
          continue;
        }

        if (lastSeenSeconds === null) {
          const candidates = [
            element.getAttribute("data-seconds"),
            element.getAttribute("data-last-seconds"),
            element.getAttribute("data-lastseen"),
            element.dataset?.seconds,
            element.dataset?.lastSeen,
            element.dataset?.lastseen,
            element.dataset?.lastSeconds
          ];

          for (const candidate of candidates) {
            if (candidate === undefined || candidate === null || candidate === "") {
              continue;
            }
            const parsed = Number(candidate);
            if (Number.isFinite(parsed)) {
              lastSeenSeconds = parsed;
              break;
            }
          }
        }

        if (!lastSeenText) {
          const value = element.textContent;
          if (typeof value === "string" && value.trim()) {
            lastSeenText = value.trim();
          }
        }

        if (lastSeenText && lastSeenSeconds !== null) {
          break;
        }
      }

      let selectedSeconds = null;
      if (selectedCandidate && typeof selectedCandidate === "object") {
        const candidates = [
          selectedCandidate.seen_pos,
          selectedCandidate.seenPos,
          selectedCandidate.seen,
          selectedCandidate.lastSeen,
          selectedCandidate.lastSeenSeconds
        ];

        for (const candidate of candidates) {
          if (typeof candidate === "number" && Number.isFinite(candidate)) {
            selectedSeconds = candidate;
            break;
          }
        }
      }

      return {
        text: lastSeenText,
        seconds: lastSeenSeconds,
        selectedSeconds
      };
    };

    const selectedCandidate = findSelectedAircraft();
    let selectedHex = null;
    if (selectedCandidate && typeof selectedCandidate === "object") {
      const hexKeys = ["icao", "icao24", "hex", "icaoHex"];
      for (const key of hexKeys) {
        const value = selectedCandidate[key];
        if (value) {
          selectedHex = value;
          break;
        }
      }
    }

    const lastSeenInfo = readLastSeen(selectedCandidate);

    const toCandidateNumber = (value) => {
      if (value === null || value === undefined) {
        return null;
      }
      if (typeof value === "number") {
        return Number.isFinite(value) ? value : null;
      }
      if (typeof value === "string") {
        const normalized = value.trim().replace(/,/g, ".").replace(/[^0-9+\-.]/g, "");
        if (!normalized) {
          return null;
        }
        const parsed = Number(normalized);
        return Number.isFinite(parsed) ? parsed : null;
      }
      return null;
    };

    const pickCandidateNumber = (candidate, keys) => {
      if (!candidate || typeof candidate !== "object") {
        return null;
      }
      for (const key of keys) {
        if (!key || !(key in candidate)) {
          continue;
        }
        const value = toCandidateNumber(candidate[key]);
        if (value !== null) {
          return value;
        }
      }
      return null;
    };

    const pickCandidateString = (candidate, keys) => {
      if (!candidate || typeof candidate !== "object") {
        return "";
      }
      for (const key of keys) {
        if (!key || !(key in candidate)) {
          continue;
        }
        const value = candidate[key];
        if (typeof value === "string") {
          const trimmed = value.trim();
          if (trimmed) {
            return trimmed;
          }
        }
      }
      return "";
    };

    const candidateData = {
      callsign: pickCandidateString(selectedCandidate, [
        "callsign",
        "flight",
        "flt",
        "identifier"
      ]),
      reg: pickCandidateString(selectedCandidate, [
        "registration",
        "reg",
        "r"
      ]),
      type: pickCandidateString(selectedCandidate, [
        "icaoType",
        "icaotype",
        "type",
        "aircraft",
        "aircraftType"
      ]),
      gs: pickCandidateNumber(selectedCandidate, [
        "gs",
        "speed",
        "spd",
        "gndspd",
        "groundspeed",
        "velocity",
        "vel"
      ]),
      alt: pickCandidateNumber(selectedCandidate, [
        "alt_baro",
        "baro_altitude",
        "altitude",
        "alt",
        "geom_altitude",
        "geomAlt",
        "geoAltitude"
      ]),
      vr: pickCandidateNumber(selectedCandidate, [
        "baro_rate",
        "vert_rate",
        "vertical_rate",
        "rateOfClimb",
        "roc",
        "rocd",
        "rate"
      ]),
      hdg: pickCandidateNumber(selectedCandidate, [
        "track",
        "heading",
        "hdg",
        "trk",
        "course"
      ]),
      lat: pickCandidateNumber(selectedCandidate, [
        "lat",
        "latitude"
      ]),
      lon: pickCandidateNumber(selectedCandidate, [
        "lon",
        "longitude",
        "lng"
      ]),
      lastSeen: pickCandidateNumber(selectedCandidate, [
        "seen",
        "lastSeen",
        "seen_pos",
        "seenPos",
        "lastSeenSeconds",
        "lastSeenSec"
      ])
    };

    return {
      time: new Date().toISOString(),
      hexRaw: textContent("#selected_icao"),
      selectedHex,
      callsign: textContent("#selected_callsign"),
      reg: textContent("#selected_registration"),
      type: textContent("#selected_icaotype"),
      gs: textContent("#selected_speed1"),
      alt: textContent("#selected_altitude1"),
      pos: textContent("#selected_position"),
      vr: textContent("#selected_vert_rate"),
      hdg: textContent("#selected_track1"),
      lastSeenText: lastSeenInfo.text,
      lastSeenSeconds: lastSeenInfo.seconds,
      lastSeenSelectedSeconds: lastSeenInfo.selectedSeconds,
      candidate: candidateData
    };
  }));

  const hex = extractHexFromDisplay(data.hexRaw) || extractHexFromDisplay(data.selectedHex);
  if (!hex) return;

  const candidate = data && typeof data.candidate === "object" && data.candidate !== null
    ? data.candidate
    : {};

  const coordsFromText = parsePos(data.pos);
  const lat = pickFirstNumber(coordsFromText.lat, candidate.lat);
  const lon = pickFirstNumber(coordsFromText.lon, candidate.lon);

  const record = {
    time: data.time,
    hex,
    callsign: pickFirstString(data.callsign, candidate.callsign),
    reg: pickFirstString(data.reg, candidate.reg),
    type: pickFirstString(data.type, candidate.type),
    gs: pickFirstNumber(data.gs, candidate.gs),
    alt: pickFirstNumber(data.alt, candidate.alt),
    vr: pickFirstNumber(data.vr, candidate.vr),
    hdg: pickFirstNumber(data.hdg, candidate.hdg),
    lat,
    lon,
    lastSeen: null
  };

  const lastSeenCandidates = [
    data.lastSeenSeconds,
    data.lastSeenSelectedSeconds,
    candidate.lastSeen
  ];

  for (const value of lastSeenCandidates) {
    const parsed = cleanNum(value);
    if (parsed !== null) {
      record.lastSeen = parsed;
      break;
    }
  }

  if (record.lastSeen === null) {
    record.lastSeen = parseLastSeen(data.lastSeenText);
  }

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
    if (consecutiveTimeoutCount !== 0) {
      consecutiveTimeoutCount = 0;
    }
  } catch (err) {
    const message = typeof err?.message === "string" ? err.message : String(err);
    console.error("❌ Scrape-Fehler:", message);
    if (isTimeoutLikeError(err)) {
      consecutiveTimeoutCount += 1;
      console.warn(`⏱️ Timeout beim Scrape (${consecutiveTimeoutCount}/${MAX_CONSECUTIVE_TIMEOUTS_BEFORE_BROWSER_RESTART}).`);
      const shouldRestartBrowser = consecutiveTimeoutCount >= MAX_CONSECUTIVE_TIMEOUTS_BEFORE_BROWSER_RESTART;
      if (shouldRestartBrowser) {
        console.warn("♻️ Zu viele aufeinanderfolgende Timeouts. Browser wird komplett neu gestartet.");
      }
      await attemptPageRecovery(
        shouldRestartBrowser ? "scrape-timeout-threshold" : "scrape-timeout",
        { forceBrowserRestart: shouldRestartBrowser }
      );
      if (shouldRestartBrowser) {
        consecutiveTimeoutCount = 0;
      }
    } else {
      consecutiveTimeoutCount = 0;
    }
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
  lastLogRecords[hex] = { ...record };

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
    const keys = new Set([
      ...Object.keys(logCounts),
      ...Object.keys(lastLogRecords)
    ]);

    const overview = Array.from(keys)
      .map(key => {
        const last = lastLogRecords[key];
        return {
          hex: key,
          count: logCounts[key] || 0,
          last: last ? { ...last } : null
        };
      })
      .sort((a, b) => {
        const timeA = Date.parse(a.last && a.last.time ? a.last.time : 0);
        const timeB = Date.parse(b.last && b.last.time ? b.last.time : 0);
        const aInvalid = Number.isNaN(timeA);
        const bInvalid = Number.isNaN(timeB);
        if (aInvalid && bInvalid) {
          return a.hex.localeCompare(b.hex);
        }
        if (aInvalid) return 1;
        if (bInvalid) return -1;
        return timeB - timeA;
      });
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

function parseHistoryDateFromFileName(fileName) {
  if (typeof fileName !== "string") {
    return null;
  }

  const parsed = path.parse(fileName);
  const baseName = parsed.name || fileName;
  const match = baseName.match(/(\d{4}-\d{2}-\d{2})/);
  return match ? match[1] : baseName;
}

async function readHistoryFileRecords(filePath) {
  let raw;
  try {
    raw = await fsp.readFile(filePath, "utf8");
  } catch (err) {
    throw new Error(`Datei konnte nicht gelesen werden: ${err.message}`);
  }

  const trimmed = raw.trim();
  if (!trimmed) {
    return [];
  }

  try {
    const parsed = JSON.parse(trimmed);
    if (Array.isArray(parsed)) {
      return parsed;
    }
    if (parsed && typeof parsed === "object") {
      return [parsed];
    }
  } catch (err) {
    // Fallback auf JSONL
  }

  const lines = trimmed.split(/\r?\n/).map(line => line.trim()).filter(Boolean);
  const records = [];
  for (const line of lines) {
    try {
      records.push(JSON.parse(line));
    } catch (err) {
      console.warn("⚠️ Ungültiger History-Eintrag in", filePath, err.message);
    }
  }
  return records;
}

async function loadHistoryDayDetail(hex, fileInfo, { includeRecords = false } = {}) {
  if (!fileInfo || typeof fileInfo !== "object") {
    return null;
  }

  const { fileName, filePath, date, stats } = fileInfo;
  let fileStats = stats || null;
  if (!fileStats) {
    try {
      fileStats = await fsp.stat(filePath);
    } catch (err) {
      console.warn("⚠️ History-Datei konnte nicht inspiziert werden:", filePath, err.message);
      fileStats = null;
    }
  }

  let records = [];
  try {
    records = await readHistoryFileRecords(filePath);
  } catch (err) {
    console.error("❌ Historie konnte nicht gelesen werden:", filePath, err.message);
    records = [];
  }

  const recordCount = Array.isArray(records) ? records.length : 0;
  const firstRecord = recordCount > 0 ? records[0] : null;
  const lastRecord = recordCount > 0 ? records[recordCount - 1] : null;

  const detail = {
    hex,
    date: date || parseHistoryDateFromFileName(fileName),
    fileName,
    fileSize: fileStats ? fileStats.size : null,
    modified: fileStats && fileStats.mtime instanceof Date ? fileStats.mtime.toISOString() : null,
    recordCount,
    firstTimestamp: firstRecord && firstRecord.time ? firstRecord.time : null,
    lastTimestamp: lastRecord && lastRecord.time ? lastRecord.time : null,
    sampleFirstRecord: firstRecord && typeof firstRecord === "object" ? { ...firstRecord } : null,
    sampleLastRecord: lastRecord && typeof lastRecord === "object" ? { ...lastRecord } : null
  };

  if (includeRecords) {
    detail.records = records;
  }

  return detail;
}

async function listHistoryDays(hex, { includeRecords = false, limit = null } = {}) {
  const normalizedHex = normalizeAircraftHex(hex);
  if (!normalizedHex) {
    return [];
  }

  const directory = path.join(logsHistoryDir, normalizedHex);
  let entries;
  try {
    entries = await fsp.readdir(directory, { withFileTypes: true });
  } catch (err) {
    if (err.code === "ENOENT") {
      return [];
    }
    throw err;
  }

  const files = await Promise.all(
    entries
      .filter(entry => entry && entry.isFile && entry.isFile())
      .map(async entry => {
        const filePath = path.join(directory, entry.name);
        let stats = null;
        try {
          stats = await fsp.stat(filePath);
        } catch (err) {
          console.warn("⚠️ History-Datei konnte nicht gelesen werden:", filePath, err.message);
          return null;
        }
        return {
          fileName: entry.name,
          filePath,
          date: parseHistoryDateFromFileName(entry.name),
          stats
        };
      })
  );

  const validFiles = files.filter(Boolean);

  validFiles.sort((a, b) => {
    if (a.date && b.date) {
      const cmp = String(b.date).localeCompare(String(a.date));
      if (cmp !== 0) {
        return cmp;
      }
    }
    const timeA = a.stats && a.stats.mtime instanceof Date ? a.stats.mtime.getTime() : 0;
    const timeB = b.stats && b.stats.mtime instanceof Date ? b.stats.mtime.getTime() : 0;
    return timeB - timeA;
  });

  const limitValue = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : null;
  const selectedFiles = limitValue ? validFiles.slice(0, limitValue) : validFiles;

  const details = [];
  for (const fileInfo of selectedFiles) {
    const detail = await loadHistoryDayDetail(normalizedHex, fileInfo, { includeRecords });
    if (detail) {
      details.push(detail);
    }
  }

  return details;
}

async function readHistoryDay(hex, date, { includeRecords = true } = {}) {
  const normalizedHex = normalizeAircraftHex(hex);
  if (!normalizedHex || !date) {
    return null;
  }

  const directory = path.join(logsHistoryDir, normalizedHex);
  let entries;
  try {
    entries = await fsp.readdir(directory, { withFileTypes: true });
  } catch (err) {
    if (err.code === "ENOENT") {
      return null;
    }
    throw err;
  }

  const match = entries.find(entry => entry && entry.isFile && entry.isFile() && parseHistoryDateFromFileName(entry.name) === date);
  if (!match) {
    return null;
  }

  const filePath = path.join(directory, match.name);
  let stats = null;
  try {
    stats = await fsp.stat(filePath);
  } catch (err) {
    console.warn("⚠️ History-Datei konnte nicht gelesen werden:", filePath, err.message);
  }

  return loadHistoryDayDetail(normalizedHex, { fileName: match.name, filePath, date, stats }, { includeRecords });
}

async function aggregateRecentHistory(hex, { limitDays = 14 } = {}) {
  const normalizedHex = normalizeAircraftHex(hex);
  if (!normalizedHex) {
    return { records: [], days: [] };
  }

  const safeLimit = Number.isFinite(limitDays) && limitDays > 0 ? Math.floor(limitDays) : 14;
  const days = await listHistoryDays(normalizedHex, { includeRecords: true, limit: safeLimit });
  if (days.length === 0) {
    return { records: [], days: [] };
  }

  const records = [];
  for (const day of days.slice().reverse()) {
    if (Array.isArray(day.records)) {
      for (const record of day.records) {
        records.push(record);
      }
    }
    delete day.records;
  }

  const sanitizedDays = days.map(day => {
    const { records: _ignored, ...rest } = day;
    return rest;
  });

  return { records, days: sanitizedDays };
}

function paginateArray(array, page = 1, limit = null) {
  const total = Array.isArray(array) ? array.length : 0;
  const hasLimit = Number.isFinite(limit) && limit > 0;
  const safeLimit = hasLimit ? Math.floor(limit) : null;
  const safePage = page > 0 ? Math.floor(page) : 1;
  const startIndex = safeLimit ? (safePage - 1) * safeLimit : 0;
  const endIndex = safeLimit ? startIndex + safeLimit : total;
  const data = Array.isArray(array)
    ? array.slice(startIndex, endIndex)
    : [];
  const totalPages = safeLimit ? Math.max(1, Math.ceil(total / safeLimit)) : (total > 0 ? 1 : 0);

  return {
    data,
    total,
    totalPages,
    page: safePage,
    limit: safeLimit
  };
}

function streamJsonResponse(res, { headers = {}, objectFields = {}, arrayFieldName = "data", arrayItems = [] } = {}) {
  const finalHeaders = { "Content-Type": "application/json", ...headers };
  res.writeHead(200, finalHeaders);

  res.write("{");
  let wroteField = false;

  const writeField = (key, value) => {
    if (value === undefined) {
      return;
    }
    if (wroteField) {
      res.write(",");
    }
    res.write(JSON.stringify(key));
    res.write(":");
    res.write(JSON.stringify(value));
    wroteField = true;
  };

  for (const [key, value] of Object.entries(objectFields)) {
    if (key === arrayFieldName) {
      continue;
    }
    writeField(key, value);
  }

  if (Array.isArray(arrayItems)) {
    if (wroteField) {
      res.write(",");
    }
    res.write(JSON.stringify(arrayFieldName));
    res.write(":");
    res.write("[");
    arrayItems.forEach((item, index) => {
      if (index > 0) {
        res.write(",");
      }
      res.write(JSON.stringify(item));
    });
    res.write("]");
    wroteField = true;
  }

  res.write("}");
  res.end();
}

async function buildHistoryOverview() {
  let entries;
  try {
    entries = await fsp.readdir(logsHistoryDir, { withFileTypes: true });
  } catch (err) {
    if (err.code === "ENOENT") {
      return [];
    }
    throw err;
  }

  const overview = [];

  for (const entry of entries) {
    if (!entry || !entry.isDirectory || !entry.isDirectory()) {
      continue;
    }

    const hex = normalizeAircraftHex(entry.name);
    if (!hex) {
      continue;
    }

    let days;
    try {
      days = await listHistoryDays(hex);
    } catch (err) {
      console.warn("⚠️ Historie konnte nicht geladen werden für", hex, err.message);
      continue;
    }

    if (!Array.isArray(days) || days.length === 0) {
      continue;
    }

    const totalEntries = days.reduce((sum, day) => sum + (day.recordCount || 0), 0);
    const latestDay = days[0];
    const aircraftEntry = getAircraftByHex(hex);
    const displayName = aircraftEntry && aircraftEntry.name
      ? aircraftEntry.name
      : (latestDay && latestDay.sampleLastRecord && latestDay.sampleLastRecord.callsign
        ? latestDay.sampleLastRecord.callsign
        : null);

    overview.push({
      hex,
      name: displayName || null,
      totalDays: days.length,
      totalEntries,
      lastDate: latestDay ? latestDay.date : null,
      lastTimestamp: latestDay ? latestDay.lastTimestamp : null,
      days: days.map(day => ({
        date: day.date,
        recordCount: day.recordCount,
        firstTimestamp: day.firstTimestamp,
        lastTimestamp: day.lastTimestamp,
        fileName: day.fileName,
        fileSize: day.fileSize
      }))
    });
  }

  overview.sort((a, b) => {
    const timeA = Date.parse(a.lastTimestamp || a.lastDate || 0);
    const timeB = Date.parse(b.lastTimestamp || b.lastDate || 0);
    const aInvalid = Number.isNaN(timeA);
    const bInvalid = Number.isNaN(timeB);

    if (!aInvalid && !bInvalid && timeA !== timeB) {
      return timeB - timeA;
    }

    if (aInvalid && !bInvalid) {
      return 1;
    }

    if (!aInvalid && bInvalid) {
      return -1;
    }

    return a.hex.localeCompare(b.hex);
  });

  return overview;
}

async function handleHistoryLogRequest(q, res) {
  const hexParam = typeof q.query.hex === "string" ? q.query.hex : null;
  const normalizedHex = hexParam ? normalizeAircraftHex(hexParam) : null;

  if (!normalizedHex) {
    const overview = await buildHistoryOverview();
    if (!overview.length) {
      sendError(res, 404, "Keine Historie vorhanden.");
      return;
    }
    sendJSON(res, 200, overview);
    return;
  }

  let daysMeta;
  try {
    daysMeta = await listHistoryDays(normalizedHex);
  } catch (err) {
    console.error("❌ Historie konnte nicht ermittelt werden für", normalizedHex, err.message);
    sendError(res, 500, "Historie konnte nicht gelesen werden.");
    return;
  }

  if (!Array.isArray(daysMeta) || daysMeta.length === 0) {
    sendError(res, 404, "Keine Historie vorhanden.");
    return;
  }

  const dateParamRaw = typeof q.query.date === "string" ? q.query.date.trim() : "";
  const dateParam = dateParamRaw || null;
  const hasPagination = typeof q.query.page !== "undefined" || typeof q.query.limit !== "undefined";
  const page = hasPagination ? parsePositiveInt(q.query.page, 1) : 1;
  const limit = hasPagination ? parsePositiveInt(q.query.limit, 100) : null;

  if (dateParam) {
    const detail = await readHistoryDay(normalizedHex, dateParam);
    if (!detail) {
      sendError(res, 404, "Keine Historie für dieses Datum.");
      return;
    }

    const records = Array.isArray(detail.records) ? detail.records : [];
    const pagination = paginateArray(records, page, limit);
    const headers = hasPagination ? { "X-Total-Count": String(pagination.total) } : {};

    const availableDays = daysMeta.map(day => ({
      date: day.date,
      recordCount: day.recordCount
    }));

    streamJsonResponse(res, {
      headers,
      objectFields: {
        hex: normalizedHex,
        date: detail.date,
        page: pagination.page,
        limit: pagination.limit,
        total: pagination.total,
        totalPages: pagination.totalPages,
        firstTimestamp: detail.firstTimestamp,
        lastTimestamp: detail.lastTimestamp,
        availableDays
      },
      arrayFieldName: "data",
      arrayItems: pagination.data
    });
    return;
  }

  const aggregated = await aggregateRecentHistory(normalizedHex, { limitDays: 14 });
  const records = Array.isArray(aggregated.records) ? aggregated.records : [];
  const pagination = paginateArray(records, page, limit);
  const headers = hasPagination ? { "X-Total-Count": String(pagination.total) } : {};

  const rangeDays = Array.isArray(aggregated.days)
    ? aggregated.days.map(day => ({
        date: day.date,
        recordCount: day.recordCount,
        firstTimestamp: day.firstTimestamp,
        lastTimestamp: day.lastTimestamp,
        fileName: day.fileName,
        fileSize: day.fileSize
      }))
    : [];

  streamJsonResponse(res, {
    headers,
    objectFields: {
      hex: normalizedHex,
      page: pagination.page,
      limit: pagination.limit,
      total: pagination.total,
      totalPages: pagination.totalPages,
      range: {
        limitDays: 14,
        totalDays: rangeDays.length,
        days: rangeDays
      }
    },
    arrayFieldName: "data",
    arrayItems: pagination.data
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
      args: ["--no-sandbox", "--disable-setuid-sandbox"],
      protocolTimeout: PUPPETEER_PROTOCOL_TIMEOUT_MS
    };

    if (executablePath) {
      launchOptions.executablePath = executablePath;
    }

    browser = await puppeteer.launch(launchOptions);
    browser.on("disconnected", () => {
      console.error("⚠️ Browserverbindung verloren. Starte Neuinitialisierung...");
      browser = null;
      page = null;
      void attemptPageRecovery("browser-disconnected");
    });

    page = await browser.newPage();
    applyPageDefaults(page);
    await runWithPage(() => navigateToTarget());
    console.log("🌍 Globe geladen für:", targetHex);

    stopScrapeLoop();
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

function sendJSON(res, statusCode, payload, extraHeaders = {}) {
  const headers = Object.assign(
    {
      "Content-Type": "application/json",
      "Cache-Control": "no-store, must-revalidate"
    },
    extraHeaders || {}
  );
  res.writeHead(statusCode, headers);
  res.end(JSON.stringify(payload));
}

function sendError(res, statusCode, message) {
  sendJSON(res, statusCode, { error: message });
}

function tryServeEmbeddedAsset(req, res, pathname) {
  const asset = EMBEDDED_ASSETS.get(pathname);
  if (!asset) {
    return false;
  }

  const method = req.method ? req.method.toUpperCase() : "GET";
  if (method !== "GET" && method !== "HEAD") {
    res.writeHead(405, { Allow: "GET, HEAD" });
    res.end();
    return true;
  }

  const headers = {
    "Content-Type": asset.contentType,
    "Content-Length": asset.buffer.length,
    "Cache-Control": "public, max-age=604800, immutable"
  };

  res.writeHead(200, headers);

  if (method === "HEAD") {
    res.end();
  } else {
    res.end(asset.buffer);
  }

  return true;
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

async function handleSetRequest(req, res, hexParam) {
  const method = req.method ? req.method.toUpperCase() : "GET";
  if (method !== "GET" && method !== "POST") {
    res.writeHead(405, { Allow: "GET, POST" });
    res.end();
    return;
  }

  let raw = typeof hexParam === "string" ? hexParam.trim() : "";

  if (!raw && method === "POST") {
    let body;
    try {
      body = await parseJsonBody(req);
    } catch (err) {
      const statusCode = err && err.statusCode ? err.statusCode : 400;
      const message = err && err.message ? err.message : "Ungültiger JSON-Body.";
      sendError(res, statusCode, message);
      return;
    }

    if (body && typeof body === "object" && body !== null) {
      const bodyHex = typeof body.hex === "string" ? body.hex.trim() : "";
      if (bodyHex) {
        raw = bodyHex;
      }
    }

    if (!raw) {
      sendError(res, 400, "Feld 'hex' wird benötigt.");
      return;
    }
  }

  if (!raw) {
    sendError(res, 400, "Bitte ?hex=xxxxxx angeben.");
    return;
  }

  const normalizedHex = normalizeAircraftHex(raw);
  if (!normalizedHex) {
    sendError(res, 400, "Ungültiger Hex-Code.");
    return;
  }

  targetHex = normalizedHex;
  const aircraftEntry = getAircraftByHex(targetHex);
  const aircraftName = aircraftEntry && aircraftEntry.name ? aircraftEntry.name : null;

  await persistLastTargetHex(targetHex);

  if (!page) {
    console.log("🎯 Neues Ziel gespeichert, Browser noch nicht bereit:", targetHex);
    const payload = { message: "Ziel gespeichert. Browser wird vorbereitet.", hex: targetHex };
    if (aircraftName) {
      payload.name = aircraftName;
    }
    sendJSON(res, 202, payload);
    return;
  }

  try {
    await runWithPage(() => navigateToTarget({ hex: targetHex }));
    if (scrapeLoopActive) {
      scheduleNextScrape(0);
    } else {
      startScrapeLoop();
    }
    queueHistoryDownload(targetHex);
    console.log("🎯 Navigiere zu neuem Ziel:", targetHex);
    const responsePayload = { success: true, hex: targetHex };
    if (aircraftName) {
      responsePayload.name = aircraftName;
    }
    if (latestData && typeof latestData === "object") {
      const latestHex = latestData.hex ? String(latestData.hex).toLowerCase() : null;
      if (latestHex && latestHex === targetHex && latestData.callsign) {
        responsePayload.callsign = latestData.callsign;
      }
    }
    sendJSON(res, 200, responsePayload);
  } catch (err) {
    console.error("❌ Navigation zum neuen Ziel fehlgeschlagen:", err.message);
    if (isTimeoutLikeError(err)) {
      await attemptPageRecovery("navigation-timeout");
    }
    sendError(res, 500, "Navigation fehlgeschlagen.");
  }
}

async function handleRequest(req, res) {
  const q = url.parse(req.url, true);

  if (q.pathname === "/latest") {
    sendJSON(res, 200, latestData);
    return;
  }

  if (q.pathname === "/history-log") {
    await handleHistoryLogRequest(q, res);
    return;
  }

  if (q.pathname === "/log") {
    await handleLogRequest(q, res);
    return;
  }

  if (q.pathname === "/events/stream") {
    handleEventStreamRequest(req, res);
    return;
  }

  if (q.pathname === "/events") {
    sendJSON(res, 200, events);
    return;
  }

  if (q.pathname && q.pathname.startsWith("/aircraft")) {
    const segments = q.pathname.split("/").filter(Boolean);

    if (segments.length === 1) {
      if (req.method === "GET") {
        sendJSON(res, 200, getAircraftList());
        return;
      }

      sendError(res, 405, "Methode nicht erlaubt.");
      return;
    }

    if (segments.length === 2) {
      let hexSegment;
      try {
        hexSegment = decodeURIComponent(segments[1]);
      } catch (err) {
        sendError(res, 400, "Ungültiger Hex-Code.");
        return;
      }

      const normalizedHex = normalizeAircraftHex(hexSegment);
      if (!normalizedHex) {
        sendError(res, 400, "Ungültiger Hex-Code.");
        return;
      }

      if (req.method === "GET") {
        const entry = getAircraftByHex(normalizedHex);
        if (!entry) {
          sendError(res, 404, "Flugzeug nicht gefunden.");
          return;
        }
        sendJSON(res, 200, entry);
        return;
      }

      if (req.method === "PUT") {
        const payload = await parseJsonBody(req);
        let aircraftData;
        try {
          aircraftData = parseAircraftPayload(payload);
        } catch (err) {
          sendError(res, 400, err.message);
          return;
        }

        try {
          const updated = await upsertAircraft(normalizedHex, aircraftData.name);
          sendJSON(res, 200, updated || { hex: normalizedHex, name: aircraftData.name });
        } catch (err) {
          sendError(res, 400, err.message || "Speichern fehlgeschlagen.");
        }
        return;
      }

      if (req.method === "DELETE") {
        try {
          const removed = await deleteAircraft(normalizedHex);
          if (!removed) {
            sendError(res, 404, "Flugzeug nicht gefunden.");
            return;
          }
          sendJSON(res, 200, { success: true });
        } catch (err) {
          sendError(res, 400, err.message || "Löschen fehlgeschlagen.");
        }
        return;
      }

      sendError(res, 405, "Methode nicht erlaubt.");
      return;
    }

    sendError(res, 404, "Pfad nicht gefunden.");
    return;
  }

  if (q.pathname && q.pathname.startsWith("/events/")) {
    const segments = q.pathname.split("/").filter(Boolean);
    if (segments.length === 2) {
      let eventId;
      try {
        eventId = decodeURIComponent(segments[1]);
      } catch (err) {
        sendError(res, 400, "Ungültige Event-ID.");
        return;
      }

      if (req.method === "DELETE") {
        const before = events.length;
        events = events.filter(event => !(event && String(event.id) === String(eventId)));

        if (events.length === before) {
          sendError(res, 404, "Event nicht gefunden.");
          return;
        }

        await persistEvents();
        sendJSON(res, 200, { success: true });
        return;
      }

      sendError(res, 405, "Methode nicht erlaubt.");
      return;
    }

    sendError(res, 404, "Pfad nicht gefunden.");
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
        if (Object.prototype.hasOwnProperty.call(newPlace, "matchRadiusMeters")) {
          if (newPlace.matchRadiusMeters === null) {
            delete newPlace.matchRadiusMeters;
          } else {
            const radius = toFiniteNumber(newPlace.matchRadiusMeters);
            if (radius !== null && radius > 0) {
              newPlace.matchRadiusMeters = radius;
            } else {
              delete newPlace.matchRadiusMeters;
            }
          }
        }
        const updatedList = [...getPlaces(), newPlace];
        await savePlaces(updatedList);
        await recalculateEventPlacesForAllEvents();
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

        const originalPlace = current[index];
        const updatedPlace = { ...originalPlace, ...placeData, id: originalPlace?.id ?? placeId };
        if (Object.prototype.hasOwnProperty.call(placeData, "matchRadiusMeters")) {
          if (placeData.matchRadiusMeters === null) {
            delete updatedPlace.matchRadiusMeters;
          } else {
            const radius = toFiniteNumber(placeData.matchRadiusMeters);
            if (radius !== null && radius > 0) {
              updatedPlace.matchRadiusMeters = radius;
            } else {
              delete updatedPlace.matchRadiusMeters;
            }
          }
        }
        const updatedList = [...current];
        updatedList[index] = updatedPlace;
        await savePlaces(updatedList);
        await applyPlaceUpdateToEvents(originalPlace, updatedPlace).catch(err => {
          console.error("⚠️ Events konnten nach Orts-Update nicht angepasst werden:", err.message);
        });
        await recalculateEventPlacesForAllEvents();
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
        await recalculateEventPlacesForAllEvents();
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
    await handleSetRequest(req, res, q.query.hex);
    return;
  }

  if (q.pathname && tryServeEmbeddedAsset(req, res, q.pathname)) {
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
