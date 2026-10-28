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
    const membersUrl = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/members?pageSize=300`;
    const res = await fetch(membersUrl);
    if (res.ok) {
      const json = await res.json();
      const docs = json.documents || [];
      cachedMembers = docs.map((doc) => {
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
    }
  } catch (err) {
    console.warn("[MEMBER CACHE] Failed to fetch members:", err.message);
  }
  return cachedMembers;
}

// In-memory set for deduplicating punches within the same session
const seenPunches = new Set();

/**
 * Record attendance punch with member lookup & deduplication
 */
async function recordAttendanceToFirestore({ userId, timestamp, sn }) {
  try {
    const parts = timestamp.split(" ");
    const dateParts = (parts[0] || "").split("-"); // YYYY-MM-DD
    const timeParts = (parts[1] || "").split(":"); // HH:MM:SS

    // Convert date to dd-MM-yyyy format standard in GymOpus
    let formattedDate = parts[0] || "";
    if (dateParts.length === 3) {
      formattedDate = `${dateParts[2].padStart(2, "0")}-${dateParts[1].padStart(2, "0")}-${dateParts[0]}`;
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

    // Skip unknown PINs (protects against old historical buffer upload from previous software)
    if (!matched) {
      console.log(`[ATTENDANCE SKIP] Unknown PIN ${userId} on device ${sn}. Ignoring to protect database.`);
      return false;
    }

    const memberId = matched.id;
    const memberName = matched.name;

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

/**
 * Update or auto-register device heartbeat in Firestore
 */
async function updateDeviceHeartbeat(sn) {
  try {
    if (!sn) return;
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

    const nowStr = new Date().toLocaleString("en-IN", { timeZone: "Asia/Kolkata" });
    if (match) {
      const docPath = match.name;
      await fetch(`https://firestore.googleapis.com/v1/${docPath}?updateMask.fieldPaths=lastHeartbeat&updateMask.fieldPaths=status`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          fields: {
            status: { stringValue: "online" },
            lastHeartbeat: { stringValue: nowStr },
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
            createdAt: { stringValue: new Date().toISOString() },
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
  const sn = (query.SN || "").toString();

  const timestamp = new Date().toLocaleTimeString();
  console.log(`[${timestamp}] ${req.method} ${pathname}?SN=${sn}`);

  // 1. Handshake: GET /iclock/cdata
  if (req.method === "GET" && pathname === "/iclock/cdata") {
    console.log(`[DEVICE CONNECTED] Serial Number: ${sn}`);
    if (sn) updateDeviceHeartbeat(sn);

    const configResponse = [
      `GET OPTION FROM: ${sn}`,
      "Stamp=9999",
      "OpStamp=9999",
      "ErrorDelay=30",
      "Delay=10",
      "TransTimes=00:00;14:00",
      "TransInterval=1",
      "TransFlag=1111000000",
      "TimeZone=5.5",
      "Realtime=1",
      "Encrypt=0",
      "",
    ].join("\r\n");

    res.writeHead(200, {
      "Content-Type": "text/plain",
      "Content-Length": Buffer.byteLength(configResponse),
    });
    res.end(configResponse);
    return;
  }

  // 2. Attendance Upload: POST /iclock/cdata?table=ATTLOG
  if (req.method === "POST" && pathname === "/iclock/cdata") {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk.toString();
    });

    req.on("end", async () => {
      const table = (query.table || "").toString().toUpperCase();
      console.log(`[DATA UPLOAD] Table: ${table}, Body length: ${body.length}`);

      if (table === "ATTLOG") {
        const lines = body.trim().split(/\r?\n/);
        let count = 0;

        // Process all punches with fast in-memory member lookup
        for (const line of lines) {
          if (!line.trim()) continue;
          const cols = line.split("\t");
          const userId = (cols[0] || "").trim();
          const punchTime = (cols[1] || "").trim();

          if (userId && punchTime) {
            const recorded = await recordAttendanceToFirestore({ userId, timestamp: punchTime, sn });
            if (recorded) count++;
          }
        }

        const reply = `OK: ${count}\n`;
        res.writeHead(200, {
          "Content-Type": "text/plain",
          "Content-Length": Buffer.byteLength(reply),
        });
        res.end(reply);
        return;
      }

      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("OK\n");
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

        const cmdId1 = ++commandCounter;
        const cmdId2 = ++commandCounter;

        // cmdId1: create user in memory
        commandRegistry.set(String(cmdId1), { ...cmd, type: "USER" });
        // cmdId2: activate fingerprint sensor on machine
        commandRegistry.set(String(cmdId2), { ...cmd, type: "ENROLL" });

        // Build ZKTeco ADMS command response:
        // C:<id>:DATA USER PIN=<pin>\tName=<name>\tPri=0
        // C:<id>:ENROLL_FP PIN=<pin>\tFID=0\tRETRY=3\tOVERWRITE=1
        const cleanName = (cmd.memberName || "Member").replace(/[\t\r\n]/g, " ").slice(0, 20);
        const commandString = [
          `C:${cmdId1}:DATA USER PIN=${cmd.pin}\tName=${cleanName}\tPri=0`,
          `C:${cmdId2}:ENROLL_FP PIN=${cmd.pin}\tFID=0\tRETRY=3\tOVERWRITE=1`,
          "",
        ].join("\r\n");

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
      res.end("OK\n");
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

  // 6. Health Check
  if (pathname === "/" || pathname === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        status: "online",
        service: "GymOpus ADMS Cloud Server",
        timestamp: new Date().toISOString(),
        supportedTables: ["ATTLOG", "OPERLOG"],
        supportedCommands: ["DATA USER", "ENROLL_FP"],
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
