/**
 * GymOpus ADMS Cloud Server for ESSL / ZKTeco / Biomax Machines.
 *
 * Implements the standard ZKTeco ADMS (Automatic Data Master Server) Push Protocol:
 * - GET  /iclock/cdata       -> Handshake with machine (device options & config)
 * - POST /iclock/cdata       -> Receives attendance punch logs (table=ATTLOG)
 * - GET  /iclock/getrequest  -> Heartbeat & command polling (dispatches ENROLL_FP / DATA USER)
 * - POST /iclock/devicecmd   -> Device command execution confirmation
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
const commandRegistry = new Map(); // id -> { docId, memberId, pin, name, type }

/**
 * Push an attendance punch directly to Firebase Firestore via REST API
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

    // 1. Try to find member name from members collection
    let memberName = `Member #${userId}`;
    let memberId = String(userId);

    try {
      const membersUrl = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/members`;
      const res = await fetch(membersUrl);
      if (res.ok) {
        const json = await res.json();
        const docs = json.documents || [];
        for (const doc of docs) {
          const docId = doc.name.split("/").pop();
          const f = doc.fields || {};
          const bId = f.biometricId?.stringValue || "";
          const mCode = f.memberCode?.stringValue || "";
          const name = f.name?.stringValue || f.fullName?.stringValue || "";

          if (String(userId) === docId || String(userId) === bId || String(userId) === mCode) {
            memberName = name || memberName;
            memberId = docId;
            break;
          }
        }
      }
    } catch (e) {
      console.warn("Member lookup note:", e.message);
    }

    // 2. Post attendance record to Firestore
    const attUrl = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/attendance`;
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

    if (postRes.ok) {
      console.log(`[ATTENDANCE] Recorded: ${memberName} (UID: ${userId}) at ${formattedDate} ${formattedTime}`);
      return true;
    } else {
      console.error("[ATTENDANCE] Failed to save:", await postRes.text());
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
    }
  } catch (err) {
    console.warn(`[MEMBER ENROLLED] Failed to update member ${memberId}:`, err.message);
  }
}

/**
 * Update device heartbeat in Firestore
 */
async function updateDeviceHeartbeat(sn) {
  try {
    const devUrl = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/biometric_devices`;
    const res = await fetch(devUrl);
    if (!res.ok) return;
    const json = await res.json();
    const docs = json.documents || [];
    const match = docs.find((d) => d.fields?.serialNumber?.stringValue === sn);

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
    }
  } catch {
    // quiet heartbeat
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

        for (const line of lines) {
          if (!line.trim()) continue;
          const cols = line.split("\t");
          const userId = (cols[0] || "").trim();
          const punchTime = (cols[1] || "").trim();

          if (userId && punchTime) {
            await recordAttendanceToFirestore({ userId, timestamp: punchTime, sn });
            count++;
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

        commandRegistry.set(String(cmdId1), { ...cmd, type: "USER" });
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
  if (req.method === "POST" && pathname === "/iclock/devicecmd") {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk.toString();
    });

    req.on("end", async () => {
      console.log(`[DEVICE CMD RESULT] Device ${sn} replied:`, body.trim());

      // Parse ID=102&Return=0&CMD=ENROLL_FP format
      const params = new URLSearchParams(body.trim());
      const cmdId = params.get("ID");
      const ret = params.get("Return");
      const cmdName = params.get("CMD") || "";

      if (cmdId && commandRegistry.has(cmdId)) {
        const reg = commandRegistry.get(cmdId);
        if (ret === "0" || ret === "1" || parseInt(ret, 10) >= 0) {
          console.log(`[COMMAND COMPLETE] Success for PIN ${reg.pin}!`);
          await updateCommandStatusInFirestore(reg.docId, "completed", {
            completedAt: new Date().toISOString(),
          });
          await updateMemberBiometricRegistered(reg.memberId, reg.pin);
        } else {
          console.warn(`[COMMAND FAILED] Device reported code ${ret} for PIN ${reg.pin}`);
          await updateCommandStatusInFirestore(reg.docId, "failed");
        }
      } else {
        // Fallback: check if body contains "Return=0"
        if (body.includes("Return=0") || body.includes("Return=OK")) {
          // Find any dispatched command and complete it
          for (const [id, reg] of commandRegistry.entries()) {
            await updateCommandStatusInFirestore(reg.docId, "completed", {
              completedAt: new Date().toISOString(),
            });
            await updateMemberBiometricRegistered(reg.memberId, reg.pin);
            commandRegistry.delete(id);
            break;
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
