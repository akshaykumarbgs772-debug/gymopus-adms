/**
 * GymOpus ADMS Cloud Server for ESSL / ZKTeco / Biomax Machines.
 *
 * Implements the standard ZKTeco ADMS (Automatic Data Master Server) Push Protocol:
 * - GET  /iclock/cdata       -> Handshake with machine (device options & config)
 * - POST /iclock/cdata       -> Receives attendance punch logs (with 60s member cache & dedup)
 * - GET  /iclock/getrequest  -> Heartbeat & command polling (dispatches ENROLL_FP / DATA USER)
 * - POST /iclock/devicecmd   -> Confirms fingerprint capture ONLY on ENROLL_FP completion
 * - POST /api/enroll         -> API for web app to queue remote fingerprint enrollment
 *
 * Saves attendance and biometrics directly into Firebase Firestore `prakash-dry-clean`.
 */

import http from "node:http";
import url from "node:url";

const PORT = process.env.PORT || 8080;
const PROJECT_ID = process.env.FIREBASE_PROJECT_ID || "prakash-dry-clean";

console.log("=".repeat(60));
console.log("  GymOpus ADMS Cloud Biometric Server");
console.log("  Project: " + PROJECT_ID);
console.log("  Port:    " + PORT);
console.log("=".repeat(60));

// In-memory registry for pending device commands
let commandCounter = 100;
const commandRegistry = new Map(); // id -> { docId, memberId, pin, name, type: "USER"|"ENROLL" }

// In-memory cache for gym members (refreshed every 60s to handle 1000s of ATTLOG lines instantly)
let cachedMembers = [];
let lastMembersFetchTime = 0;

async function getGymMembers() {
  const now = Date.now();
  if (cachedMembers.length > 0 && now - lastMembersFetchTime < 60 * 1000) {
    return cachedMembers;
  }

  try {
    let pageToken = "";
    const allDocs = [];
    do {
      const membersUrl = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/members?pageSize=300${pageToken ? `&pageToken=${pageToken}` : ""}`;
      const res = await fetch(membersUrl);
      if (!res.ok) break;
      const json = await res.json();
      if (json.documents) allDocs.push(...json.documents);
      pageToken = json.nextPageToken || "";
    } while (pageToken);

    cachedMembers = allDocs.map((doc) => {
      const docId = doc.name.split("/").pop();
      const f = doc.fields || {};
      return {
        id: docId,
        biometricId: f.biometricId?.stringValue || "",
        memberCode: f.memberCode?.stringValue || "",
        name: f.name?.stringValue || f.fullName?.stringValue || `Member #${docId}`,
      };
    });
    lastMembersFetchTime = now;
  } catch (err) {
    console.warn("[MEMBER CACHE] Failed to fetch members:", err.message);
  }
  return cachedMembers;
}

// In-memory set for deduplicating punches within the same session
const seenPunches = new Set();

// In-memory ring buffer for live request logging (visible at /logs)
const recentLogs = [];
function recordLog(req, sn, note = "") {
  const item = {
    id: Date.now() + Math.random().toString(36).substring(2, 6),
    iso: new Date().toISOString(),
    ist: new Date().toLocaleString("en-IN", { timeZone: "Asia/Kolkata" }),
    method: req.method,
    url: req.url,
    sn: sn || "N/A",
    ip: (req.headers["x-forwarded-for"] || req.socket.remoteAddress || "").replace(/^::ffff:/, ""),
    userAgent: req.headers["user-agent"] || "",
    note: note
  };
  recentLogs.unshift(item);
  if (recentLogs.length > 100) recentLogs.pop();
}

/**
 * Record attendance punch with member lookup & deduplication
 */
async function recordAttendanceToFirestore({ userId, timestamp, sn }) {
  try {
    const parts = timestamp.split(" ");
    const dateParts = (parts[0] || "").split(/[-/]/); // Handles YYYY-MM-DD, DD/MM/YYYY, etc.
    const timeParts = (parts[1] || "").split(":"); // HH:MM:SS

    // Convert date to standard dd-MM-yyyy format
    let formattedDate = parts[0] || "";
    if (dateParts.length === 3) {
      if (dateParts[0].length === 4) {
        // YYYY-MM-DD -> DD-MM-YYYY
        formattedDate = `${dateParts[2].padStart(2, "0")}-${dateParts[1].padStart(2, "0")}-${dateParts[0]}`;
      } else {
        // DD-MM-YYYY
        formattedDate = `${dateParts[0].padStart(2, "0")}-${dateParts[1].padStart(2, "0")}-${dateParts[2]}`;
      }
    }

    // Convert time to 12-hour AM/PM format
    let formattedTime = parts[1] || "";
    if (timeParts.length >= 2) {
      let hours = parseInt(timeParts[0], 10);
      const minutes = timeParts[1];
      const ampm = hours >= 12 ? "PM" : "AM";
      hours = hours % 12 || 12;
      formattedTime = `${String(hours).padStart(2, "0")}:${minutes} ${ampm}`;
    }

    // Deduplication check
    const punchKey = `${userId}_${formattedDate}_${formattedTime}`;
    if (seenPunches.has(punchKey)) {
      return false;
    }

    // Fast O(1) in-memory lookup against cached members
    const members = await getGymMembers();
    const matched = members.find((m) => {
      const cleanCode = m.memberCode.replace(/\D/g, "");
      return (
        String(userId) === m.id ||
        String(userId) === m.biometricId ||
        String(userId) === m.memberCode ||
        (cleanCode && cleanCode === String(userId))
      );
    });

    // Check if punch is from today (IST / local date)
    const todayStr = new Date().toLocaleDateString("en-IN", { timeZone: "Asia/Kolkata" }).replace(/\//g, "-");
    const todayParts = todayStr.split("-"); // [DD, MM, YYYY] or similar
    const todayFormatted = `${(todayParts[0] || "").padStart(2, "0")}-${(todayParts[1] || "").padStart(2, "0")}-${todayParts[2]}`;
    const isToday = formattedDate === todayFormatted || parts[0] === new Date().toISOString().split("T")[0];

    let memberId = "";
    let memberName = "";

    if (matched) {
      memberId = matched.id;
      memberName = matched.name;
    } else if (isToday) {
      // Live demo punch! Allow today's live test even if PIN is not yet mapped to a named member
      console.log(`[ATTENDANCE LIVE] Today's punch for unassigned PIN ${userId} on device ${sn}. Recording live!`);
      memberId = `pin_${userId}`;
      memberName = `Biometric PIN #${userId} (Unassigned)`;
    } else {
      // Historical buffer upload from old days - skip to avoid database pollution
      console.log(`[ATTENDANCE SKIP] Historical buffer PIN ${userId} date ${formattedDate} on device ${sn}. Skipping.`);
      return false;
    }

    // Use a deterministic document ID to prevent duplicate records in Firestore even across server restarts
    const cleanDate = formattedDate.replace(/[^a-zA-Z0-9]/g, "");
    const cleanTime = formattedTime.replace(/[^a-zA-Z0-9]/g, "");
    const customDocId = `att_${memberId}_${cleanDate}_${cleanTime}`;

    const attUrl = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/attendance?documentId=${customDocId}`;
    const payload = {
      fields: {
        member_id: { stringValue: memberId },
        memberName: { stringValue: memberName },
        biometricId: { stringValue: String(userId) },
        date: { stringValue: formattedDate },
        time: { stringValue: formattedTime },
        status: { stringValue: "present" },
        type: { stringValue: "Biometric Check-in" },
        source: { stringValue: "biometric" },
        deviceSn: { stringValue: sn || "ESSL" },
        member: {
          mapValue: {
            fields: {
              id: { stringValue: memberId },
              name: { stringValue: memberName },
            },
          },
        },
        createdAt: { stringValue: new Date().toISOString() },
      },
    };

    const postRes = await fetch(attUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    // Even if 409 Conflict occurs (already recorded), treat as success deduplication
    seenPunches.add(punchKey);
    if (seenPunches.size > 2000) {
      const first = seenPunches.values().next().value;
      seenPunches.delete(first);
    }

    if (postRes.ok) {
      console.log(`[ATTENDANCE] Recorded: ${memberName} (UID: ${userId}) at ${formattedDate} ${formattedTime}`);
      return true;
    } else {
      const errText = await postRes.text();
      if (errText.includes("ALREADY_EXISTS") || postRes.status === 409) {
        console.log(`[ATTENDANCE DEDUP] Already exists in Firestore for ${memberName} at ${formattedTime}`);
        return true;
      }
      console.error("[ATTENDANCE] Failed to save:", errText);
      return false;
    }
  } catch (err) {
    console.error("[ATTENDANCE] Error recording:", err);
    return false;
  }
}

/**
 * Fetch pending biometric commands from Firestore
 */
async function getPendingCommandsFromFirestore(sn) {
  try {
    const cmdUrl = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/biometric_commands`;
    const res = await fetch(cmdUrl);
    if (!res.ok) return [];
    const json = await res.json();
    const docs = json.documents || [];

    const pending = [];
    for (const doc of docs) {
      const docId = doc.name.split("/").pop();
      const f = doc.fields || {};
      const status = f.status?.stringValue || "";
      const deviceSn = f.deviceSn?.stringValue || "";

      // Match pending commands for this device or all devices
      if (status === "pending" && (!deviceSn || deviceSn === sn || sn.includes(deviceSn))) {
        pending.push({
          docId,
          command: f.command?.stringValue || "ENROLL_FP",
          memberId: f.memberId?.stringValue || "",
          memberName: f.memberName?.stringValue || "Member",
          pin: f.pin?.stringValue || "1",
        });
      }
    }
    return pending;
  } catch (err) {
    console.warn("[COMMANDS] Error fetching pending commands:", err.message);
    return [];
  }
}

/**
 * Mark command status in Firestore
 */
async function updateCommandStatusInFirestore(docId, status, extra = {}) {
  try {
    const docPath = `projects/${PROJECT_ID}/databases/(default)/documents/biometric_commands/${docId}`;
    const fields = {
      status: { stringValue: status },
      updatedAt: { stringValue: new Date().toISOString() },
    };

    let mask = "updateMask.fieldPaths=status&updateMask.fieldPaths=updatedAt";
    if (extra.completedAt) {
      fields.completedAt = { stringValue: extra.completedAt };
      mask += "&updateMask.fieldPaths=completedAt";
    }

    await fetch(`https://firestore.googleapis.com/v1/${docPath}?${mask}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ fields }),
    });
  } catch (err) {
    console.warn(`[COMMANDS] Failed to update command ${docId}:`, err.message);
  }
}

/**
 * Update member record to mark biometric enrolled
 */
async function updateMemberBiometricRegistered(memberId, pin) {
  try {
    if (!memberId) return;
    const docPath = `projects/${PROJECT_ID}/databases/(default)/documents/members/${memberId}`;
    const fields = {
      biometricRegistered: { booleanValue: true },
      biometricId: { stringValue: String(pin) },
    };

    const patchUrl = `https://firestore.googleapis.com/v1/${docPath}?updateMask.fieldPaths=biometricRegistered&updateMask.fieldPaths=biometricId`;
    const res = await fetch(patchUrl, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ fields }),
    });

    if (res.ok) {
      console.log(`[MEMBER ENROLLED] Member ${memberId} successfully marked enrolled with PIN ${pin}!`);
      // Invalidate member cache so subsequent punches recognize the new biometric PIN
      cachedMembers = [];
      lastMembersFetchTime = 0;
    }
  } catch (err) {
    console.warn(`[MEMBER ENROLLED] Failed to update member ${memberId}:`, err.message);
  }
}

const activeDevices = {};

/**
 * Update or auto-register device heartbeat in Firestore
 */
async function updateDeviceHeartbeat(sn) {
  try {
    if (!sn) return;
    const nowIso = new Date().toISOString();
    const nowStr = new Date().toLocaleString("en-IN", { timeZone: "Asia/Kolkata" });
    activeDevices[sn] = {
      lastSeenIso: nowIso,
      lastHeartbeat: nowStr,
      status: "online",
    };

    const devUrl = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/biometric_devices`;
    const res = await fetch(devUrl);
    if (!res.ok) return;
    const json = await res.json();
    const docs = json.documents || [];
    const match = docs.find((d) => {
      const s = d.fields?.serialNumber?.stringValue || "";
      const docId = d.name.split("/").pop();
      return s === sn || docId === sn;
    });

    if (match) {
      const docPath = match.name;
      await fetch(`https://firestore.googleapis.com/v1/${docPath}?updateMask.fieldPaths=lastHeartbeat&updateMask.fieldPaths=lastSeenIso&updateMask.fieldPaths=status`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          fields: {
            status: { stringValue: "online" },
            lastHeartbeat: { stringValue: nowStr },
            lastSeenIso: { stringValue: nowIso },
          },
        }),
      });
    } else {
      // Auto-upsert device if not yet in database
      await fetch(`${devUrl}?documentId=${sn}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          fields: {
            serialNumber: { stringValue: sn },
            deviceName: { stringValue: "The Rock Gym Teghra (Biometric)" },
            model: { stringValue: "eSSL ZLM60 / K90 Pro" },
            branchName: { stringValue: "The Rock Gym Teghra" },
            location: { stringValue: "Gym Entrance" },
            status: { stringValue: "online" },
            lastHeartbeat: { stringValue: nowStr },
            lastSeenIso: { stringValue: nowIso },
            createdAt: { stringValue: nowIso },
          },
        }),
      });
      console.log(`[DEVICE REGISTERED] Auto-registered device ${sn} in biometric_devices!`);
    }
  } catch (err) {
    console.warn("[HEARTBEAT] Error:", err.message);
  }
}

const server = http.createServer((req, res) => {
  // Handle CORS for web dashboard requests
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") {
    res.writeHead(204);
    res.end();
    return;
  }

  const parsed = url.parse(req.url, true);
  const pathname = parsed.pathname;
  const query = parsed.query;
  // Case-insensitive SN lookup with safe fallback to active device SN
  const rawSn = (query.SN || query.sn || query.Sn || query.serialNumber || "").toString().trim();
  const sn = rawSn || "NFZ8252804596";

  const timestamp = new Date().toLocaleTimeString();
  console.log(`[${timestamp}] ${req.method} ${pathname}?SN=${sn}`);
  recordLog(req, sn);

  // 1. Handshake: GET /iclock/cdata (also supports HEAD for health probes)
  if ((req.method === "GET" || req.method === "HEAD") && pathname === "/iclock/cdata") {
    console.log(`[DEVICE HANDSHAKE] Serial Number: ${sn}`);
    if (sn) updateDeviceHeartbeat(sn);

    // Exact official ZKTeco / eSSL ADMS protocol response with \r\n line endings
    const configResponse = [
      `GET OPTION FROM: ${sn}`,
      "ATTLOGStamp=None",
      "OPERLOGStamp=None",
      "ATTPHOTOStamp=None",
      "Stamp=9999",
      "OpStamp=9999",
      "ErrorDelay=30",
      "Delay=10",
      "TransTimes=00:00;14:05",
      "TransInterval=1",
      "TransFlag=1111000000",
      "TimeZone=5.5",
      "Realtime=1",
      "Encrypt=0",
      "ServerVersion=3.1.1",
      "PushProtVer=2.4.1",
      "",
    ].join("\r\n");

    res.writeHead(200, {
      "Content-Type": "text/plain",
      "Content-Length": Buffer.byteLength(configResponse),
    });
    if (req.method === "HEAD") {
      res.end();
    } else {
      res.end(configResponse);
    }
    return;
  }

  // 2. Attendance Upload: POST /iclock/cdata?table=ATTLOG
  if (req.method === "POST" && pathname === "/iclock/cdata") {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk.toString();
    });

    req.on("end", async () => {
      if (sn) updateDeviceHeartbeat(sn);
      const table = (query.table || "").toString().toUpperCase();
      console.log(`[DATA UPLOAD] Table: ${table}, Body length: ${body.length}`);

      if (table === "ATTLOG") {
        const lines = body.trim().split(/\r?\n/);
        let count = 0;

        // Process all punches with fast in-memory member lookup
        for (const line of lines) {
          if (!line.trim()) continue;
          let cols = line.split("\t");
          if (cols.length < 2) {
            const spaceCols = line.trim().split(/\s+/);
            if (spaceCols.length >= 3) {
              cols = [spaceCols[0], `${spaceCols[1]} ${spaceCols[2]}`];
            }
          }
          const userId = (cols[0] || "").trim();
          const punchTime = (cols[1] || "").trim();

          if (userId && punchTime) {
            const recorded = await recordAttendanceToFirestore({ userId, timestamp: punchTime, sn });
            if (recorded) count++;
          }
        }

        // Return number of parsed lines so machine flushes transmit buffer
        const reply = `OK: ${lines.length}\r\n`;
        res.writeHead(200, {
          "Content-Type": "text/plain",
          "Content-Length": Buffer.byteLength(reply),
        });
        res.end(reply);
        return;
      }

      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("OK\r\n");
    });
    return;
  }

  // 3. Heartbeat / Device Command Polling: GET /iclock/getrequest
  if (req.method === "GET" && pathname === "/iclock/getrequest") {
    if (sn) updateDeviceHeartbeat(sn);

    (async () => {
      const pendingCmds = await getPendingCommandsFromFirestore(sn);

      if (pendingCmds.length > 0) {
        const cmd = pendingCmds[0];
        console.log(`[DISPATCH COMMAND] Sending to device ${sn}:`, cmd);

        const cmdId = ++commandCounter;
        // Direct single ENROLL_FP command - activates physical sensor and beeps instantly
        commandRegistry.set(String(cmdId), { ...cmd, type: "ENROLL" });

        const commandString = `C:${cmdId}:ENROLL_FP PIN=${cmd.pin}\tFID=0\tRETRY=3\tOVERWRITE=1\r\n`;

        // Mark as dispatched
        await updateCommandStatusInFirestore(cmd.docId, "dispatched");

        res.writeHead(200, {
          "Content-Type": "text/plain",
          "Content-Length": Buffer.byteLength(commandString),
        });
        res.end(commandString);
        return;
      }

      // Default heartbeat reply
      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("OK\r\n");
    })();
    return;
  }

  // 4. Device Command Execution Result: POST /iclock/devicecmd
  // CRITICAL FIX: Only complete command when ENROLL_FP (actual fingerprint scan) succeeds!
  if (req.method === "POST" && pathname === "/iclock/devicecmd") {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk.toString();
    });

    req.on("end", async () => {
      console.log(`[DEVICE CMD RESULT] Device ${sn} replied:`, body.trim());

      const params = new URLSearchParams(body.trim());
      const cmdId = params.get("ID");
      const ret = params.get("Return") || "";
      const cmdName = params.get("CMD") || "";

      // Check success conditions: numeric >= 0 or "OK"
      const isSuccess =
        ret === "0" ||
        ret === "1" ||
        parseInt(ret, 10) >= 0 ||
        ret.toUpperCase() === "OK" ||
        body.includes("Return=0") ||
        body.includes("Return=OK");

      if (cmdId && commandRegistry.has(cmdId)) {
        const reg = commandRegistry.get(cmdId);

        if (reg.type === "USER") {
          // User created in device memory -> DO NOT mark complete yet, waiting for finger scan!
          console.log(`[USER SYNCED ON DEVICE] PIN ${reg.pin} created in device memory.`);
          commandRegistry.delete(cmdId);
        } else if (reg.type === "ENROLL") {
          // Physical fingerprint sensor completed 3 scans!
          if (isSuccess) {
            console.log(`[ENROLLMENT SUCCESS] Fingerprint captured on machine for PIN ${reg.pin}!`);
            await updateCommandStatusInFirestore(reg.docId, "completed", {
              completedAt: new Date().toISOString(),
            });
            await updateMemberBiometricRegistered(reg.memberId, reg.pin);
          } else {
            console.warn(`[ENROLLMENT FAILED / CANCELLED] Device reported code ${ret} for PIN ${reg.pin}`);
            await updateCommandStatusInFirestore(reg.docId, "failed");
          }
          commandRegistry.delete(cmdId);
        }
      } else {
        // Fallback: if body explicitly contains ENROLL_FP success, find any pending ENROLL in registry or Firestore
        if ((body.includes("ENROLL_FP") || cmdName === "ENROLL_FP") && isSuccess) {
          for (const [id, reg] of commandRegistry.entries()) {
            if (reg.type === "ENROLL") {
              await updateCommandStatusInFirestore(reg.docId, "completed", {
                completedAt: new Date().toISOString(),
              });
              await updateMemberBiometricRegistered(reg.memberId, reg.pin);
              commandRegistry.delete(id);
              break;
            }
          }
        }
      }

      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("OK\n");
    });
    return;
  }

  // 5. REST API: POST /api/enroll (allows web app or testing to queue enrollment directly)
  if (req.method === "POST" && pathname === "/api/enroll") {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk.toString();
    });

    req.on("end", async () => {
      try {
        const data = JSON.parse(body || "{}");
        const { memberId, memberName, pin, deviceSn } = data;

        const cmdUrl = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/biometric_commands`;
        const payload = {
          fields: {
            command: { stringValue: "ENROLL_FP" },
            memberId: { stringValue: String(memberId || "") },
            memberName: { stringValue: String(memberName || "Member") },
            pin: { stringValue: String(pin || "1") },
            deviceSn: { stringValue: String(deviceSn || sn || "NFZ8252804596") },
            status: { stringValue: "pending" },
            createdAt: { stringValue: new Date().toISOString() },
          },
        };

        const postRes = await fetch(cmdUrl, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });

        if (postRes.ok) {
          const resJson = await postRes.json();
          const docId = resJson.name.split("/").pop();
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ success: true, commandId: docId }));
          return;
        } else {
          res.writeHead(500, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ success: false, error: await postRes.text() }));
          return;
        }
      } catch (err) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ success: false, error: err.message }));
      }
    });
    return;
  }

  // 6. Live Request Logs (HTML Dashboard & JSON API)
  if (pathname === "/logs" || pathname === "/live-logs") {
    const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="refresh" content="3">
  <title>GymOpus ADMS - Live Request Logs</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0f172a; color: #f8fafc; margin: 0; padding: 20px; }
    h1 { font-size: 20px; margin-bottom: 4px; display: flex; align-items: center; gap: 10px; }
    .status { font-size: 12px; background: #10b98120; color: #34d399; border: 1px solid #10b98140; padding: 3px 8px; rounded: 6px; border-radius: 9999px; }
    .desc { font-size: 13px; color: #94a3b8; margin-bottom: 20px; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; background: #1e293b; border-radius: 8px; overflow: hidden; }
    th { background: #334155; text-align: left; padding: 10px 14px; font-size: 11px; text-transform: uppercase; color: #cbd5e1; }
    td { padding: 10px 14px; border-bottom: 1px solid #334155; }
    tr:hover { background: #283548; }
    .badge { display: inline-block; padding: 2px 6px; border-radius: 4px; font-weight: bold; font-size: 11px; }
    .get { background: #0284c720; color: #38bdf8; border: 1px solid #0284c750; }
    .post { background: #16a34a20; color: #4ade80; border: 1px solid #16a34a50; }
    .time { font-family: monospace; color: #94a3b8; font-size: 12px; }
    .sn { font-family: monospace; font-weight: bold; color: #a78bfa; background: #a78bfa15; padding: 2px 6px; border-radius: 4px; }
    .ip { font-family: monospace; color: #facc15; }
    .url { font-family: monospace; word-break: break-all; color: #e2e8f0; }
  </style>
</head>
<body>
  <h1>
    GymOpus ADMS Server — Live Packet Inspector
    <span class="status">Auto-Refreshing (3s)</span>
  </h1>
  <p class="desc">Listening on Port 80 for eSSL / ZKTeco Biometric Machine (SN: NFZ8252804596). Total logged: ${recentLogs.length}</p>
  <table>
    <thead>
      <tr>
        <th>Time (IST)</th>
        <th>Method</th>
        <th>Device SN</th>
        <th>Client IP</th>
        <th>Request URL</th>
      </tr>
    </thead>
    <tbody>
      ${recentLogs.length === 0 ? '<tr><td colspan="5" style="text-align:center; padding: 30px; color: #64748b;">Waiting for device connection... Power ON the machine.</td></tr>' : ''}
      ${recentLogs.map(l => `
        <tr>
          <td class="time">${l.ist}</td>
          <td><span class="badge ${l.method.toLowerCase()}">${l.method}</span></td>
          <td><span class="sn">${l.sn}</span></td>
          <td class="ip">${l.ip}</td>
          <td class="url">${l.url}</td>
        </tr>
      `).join('')}
    </tbody>
  </table>
</body>
</html>`;
    res.writeHead(200, { "Content-Type": "text/html" });
    res.end(html);
    return;
  }

  if (pathname === "/api/logs") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ logs: recentLogs }));
    return;
  }

  // 7. Health Check
  if (pathname === "/" || pathname === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        status: "online",
        service: "GymOpus ADMS Cloud Server",
        timestamp: new Date().toISOString(),
        supportedTables: ["ATTLOG", "OPERLOG"],
        supportedCommands: ["DATA USER", "ENROLL_FP"],
        activeDevices: activeDevices,
      })
    );
    return;
  }

  res.writeHead(404);
  res.end("Not Found");
});

server.listen(PORT, () => {
  console.log(`\n✓ ADMS Server listening on HTTP port ${PORT}`);
  console.log("Ready to receive biometric punches from ESSL / ZKTeco machines!\n");

  // Auto Keep-Alive: Prevents free hosting (like Render) from sleeping
  const selfUrl = process.env.RENDER_EXTERNAL_URL || process.env.SELF_URL;
  if (selfUrl) {
    console.log(`[KEEP-ALIVE] Auto self-ping enabled for ${selfUrl}`);
    setInterval(async () => {
      try {
        const pingUrl = `${selfUrl.replace(/\/$/, "")}/health`;
        const res = await fetch(pingUrl);
        if (res.ok) {
          console.log(`[KEEP-ALIVE] Self-ping OK at ${new Date().toLocaleTimeString()}`);
        }
      } catch (err) {
        console.warn("[KEEP-ALIVE] Ping failed:", err.message);
      }
    }, 4 * 60 * 1000); // 4 minutes
  }
});
