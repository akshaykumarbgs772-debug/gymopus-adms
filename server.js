/**
 * GymOpus ADMS Cloud Server for ESSL / ZKTeco / Biomax Machines.
 *
 * Implements the standard ZKTeco ADMS (Automatic Data Master Server) Push Protocol:
 * - GET  /iclock/cdata       -> Handshake with machine (device options & config)
 * - POST /iclock/cdata       -> Receives attendance punch logs (table=ATTLOG)
 * - GET  /iclock/getrequest  -> Heartbeat & command polling (responds OK)
 * - POST /iclock/devicecmd   -> Device command response
 *
 * Saves attendance records directly into Firebase Firestore `attendance` collection.
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
 * Update device status in Firestore
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
    // quiet heartbeat update
  }
}

const server = http.createServer((req, res) => {
  const parsed = url.parse(req.url, true);
  const pathname = parsed.pathname;
  const query = parsed.query;
  const sn = (query.SN || "").toString();

  // Log incoming request
  const timestamp = new Date().toLocaleTimeString();
  console.log(`[${timestamp}] ${req.method} ${pathname}?SN=${sn}`);

  // 1. Handshake: GET /iclock/cdata
  if (req.method === "GET" && pathname === "/iclock/cdata") {
    console.log(`[DEVICE CONNECTED] Serial Number: ${sn}`);
    if (sn) updateDeviceHeartbeat(sn);

    // Standard ADMS device handshake response
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
        // Body format: lines of tab-separated fields:
        // PIN \t Time \t VerifyType \t WorkCode \t Reserved
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

      // Default OK for other tables (e.g. OPERLOG)
      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("OK\n");
    });
    return;
  }

  // 3. Heartbeat / Device Command Polling: GET /iclock/getrequest
  if (req.method === "GET" && pathname === "/iclock/getrequest") {
    if (sn) updateDeviceHeartbeat(sn);
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("OK\n");
    return;
  }

  // 4. Device Command Result: POST /iclock/devicecmd
  if (req.method === "POST" && pathname === "/iclock/devicecmd") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("OK\n");
    return;
  }

  // Fallback for root or health check
  if (pathname === "/" || pathname === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        status: "online",
        service: "GymOpus ADMS Cloud Server",
        timestamp: new Date().toISOString(),
        supportedTables: ["ATTLOG", "OPERLOG"],
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
    }, 4 * 60 * 1000); // Pings every 4 minutes (Render sleeps at 15 min)
  }
});
