const fs = require("fs");
const path = require("path");

const source = path.join(__dirname, "adsb_log.json");
const logsDir = path.join(__dirname, "logs");

if (!fs.existsSync(source)) {
  console.error("Quelle adsb_log.json nicht gefunden.");
  process.exit(1);
}

fs.mkdirSync(logsDir, { recursive: true });

let raw;
try {
  raw = fs.readFileSync(source, "utf8");
} catch (err) {
  console.error("Lesen von adsb_log.json fehlgeschlagen:", err.message);
  process.exit(1);
}

let data;
try {
  data = JSON.parse(raw);
} catch (err) {
  console.error("adsb_log.json ist kein gültiges JSON:", err.message);
  process.exit(1);
}

if (typeof data !== "object" || Array.isArray(data) || data === null) {
  console.error("adsb_log.json hat ein unerwartetes Format.");
  process.exit(1);
}

Object.entries(data).forEach(([hex, records]) => {
  if (!Array.isArray(records)) {
    console.warn(`⚠️ Überspringe ${hex}: kein Array.`);
    return;
  }

  const normalizedHex = hex.toLowerCase();
  const trimmed = records.slice(-5000);
  const lines = trimmed
    .map(entry => {
      const record = { ...entry };
      if (!record.hex) {
        record.hex = normalizedHex;
      } else {
        record.hex = String(record.hex).toLowerCase();
      }
      return JSON.stringify(record);
    });

  const filePath = path.join(logsDir, `${normalizedHex}.jsonl`);
  const payload = lines.join("\n") + (lines.length ? "\n" : "");
  fs.writeFileSync(filePath, payload, "utf8");
  console.log(`✅ ${normalizedHex}: ${lines.length} Einträge migriert.`);
});

console.log("✨ Migration abgeschlossen.");
