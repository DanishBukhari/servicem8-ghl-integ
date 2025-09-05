// import express from "express";
// import axios from "axios";
// import fs from "fs";
// import dotenv from "dotenv";
// import moment from "moment-timezone";

// dotenv.config();

// const app = express();
// app.use(express.json()); // Enable JSON body parsing for webhooks

// const PORT = process.env.PORT || 3000;

// // GHL credentials
// const CLIENT_ID = process.env.GHL_CLIENT_ID;
// const CLIENT_SECRET = process.env.GHL_CLIENT_SECRET;
// const REDIRECT_URI = process.env.GHL_REDIRECT_URI || "http://localhost:3000/callback";

// // ServiceM8 credentials
// const SERVICE_M8_API_KEY = process.env.SERVICE_M8_API_KEY;
// const SERVICE_M8_STAFF_UUID = process.env.SERVICE_M8_STAFF_UUID;

// // File-based token storage
// const TOKEN_FILE = "./tokens.json";

// // Utility: Save tokens
// function saveTokens(tokens) {
//   fs.writeFileSync(TOKEN_FILE, JSON.stringify(tokens, null, 2));
// }

// // Utility: Load tokens
// function loadTokens() {
//   if (fs.existsSync(TOKEN_FILE)) {
//     return JSON.parse(fs.readFileSync(TOKEN_FILE));
//   }
//   return null;
// }

// // STEP 1: Redirect to GHL auth page
// app.get("/auth", (req, res) => {
//   const url = `https://marketplace.gohighlevel.com/oauth/chooselocation?response_type=code&client_id=${CLIENT_ID}&redirect_uri=${encodeURIComponent(
//     REDIRECT_URI
//   )}&scope=calendars.readonly+calendars.write+calendars%2Fevents.write+calendars%2Fevents.readonly+users.readonly`;
//   res.redirect(url);
// });

// // STEP 2: Handle callback and exchange code for tokens
// app.get("/callback", async (req, res) => {
//   const code = req.query.code;
//   if (!code) return res.status(400).send("No code provided!");

//   try {
//     const params = new URLSearchParams({
//       client_id: CLIENT_ID,
//       client_secret: CLIENT_SECRET,
//       grant_type: "authorization_code",
//       code,
//       redirect_uri: REDIRECT_URI,
//     });

//     const response = await axios.post(
//       "https://services.leadconnectorhq.com/oauth/token",
//       params,
//       {
//         headers: { "Content-Type": "application/x-www-form-urlencoded" },
//       }
//     );

//     const tokens = {
//       ...response.data,
//       created_at: Math.floor(Date.now() / 1000),
//     };

//     saveTokens(tokens);

//     res.send("✅ Tokens saved! You can now use the API.");
//   } catch (err) {
//     console.error("Token exchange error:", err.response?.data || err.message);
//     res.status(500).send("Failed to exchange code for tokens.");
//   }
// });

// // STEP 3: Auto-refresh token if expired
// async function getAccessToken() {
//   let tokens = loadTokens();

//   if (!tokens) {
//     throw new Error("No tokens found. Visit /auth first.");
//   }

//   const now = Math.floor(Date.now() / 1000);
//   const expiry = tokens.created_at ? tokens.created_at + tokens.expires_in : 0;

//   if (now >= expiry) {
//     console.log("🔄 Refreshing access token...");

//     const params = new URLSearchParams({
//       client_id: CLIENT_ID,
//       client_secret: CLIENT_SECRET,
//       grant_type: "refresh_token",
//       refresh_token: tokens.refresh_token,
//     });

//     const response = await axios.post(
//       "https://services.leadconnectorhq.com/oauth/token",
//       params,
//       {
//         headers: { "Content-Type": "application/x-www-form-urlencoded" },
//       }
//     );

//     tokens = {
//       ...response.data,
//       refresh_token: tokens.refresh_token,
//       created_at: Math.floor(Date.now() / 1000),
//     };

//     saveTokens(tokens);
//   }

//   return tokens.access_token;
// }

// // ServiceM8 Axios instance
// const serviceM8Api = axios.create({
//   baseURL: "https://api.servicem8.com/api_1.0",
//   headers: {
//     Accept: "application/json",
//     "X-API-Key": SERVICE_M8_API_KEY,
//   },
// });

// // GHL Axios instance
// const ghlApi = axios.create({
//   baseURL: "https://rest.gohighlevel.com/v1",
//   headers: {
//     Accept: "application/json",
//   },
// });

// // Middleware to add GHL auth header dynamically
// ghlApi.interceptors.request.use(async (config) => {
//   const accessToken = await getAccessToken();
//   config.headers.Authorization = `Bearer ${accessToken}`;
//   return config;
// });

// // Store processed appointment IDs to prevent duplicates
// const PROCESSED_FILE = "./processed_appointments.json";
// function loadProcessedAppointments() {
//   if (fs.existsSync(PROCESSED_FILE)) {
//     return new Set(JSON.parse(fs.readFileSync(PROCESSED_FILE)));
//   }
//   return new Set();
// }
// function saveProcessedAppointments(processed) {
//   fs.writeFileSync(PROCESSED_FILE, JSON.stringify([...processed], null, 2));
// }

// // Endpoint to handle GHL webhook for new appointments
// app.post("/ghl-appointment-sync", async (req, res) => {
//   const processedAppointments = loadProcessedAppointments();
//   const { appointment } = req.body; // Expected: { id, contactId, startTime, endTime, title, location }

//   if (!appointment || !appointment.id || !appointment.contactId || !appointment.startTime) {
//     console.error("Invalid webhook payload:", req.body);
//     return res.status(400).json({ error: "Missing appointment data" });
//   }

//   const appointmentId = appointment.id;
//   if (processedAppointments.has(appointmentId)) {
//     console.log(`Appointment ${appointmentId} already processed, skipping.`);
//     return res.status(200).json({ message: "Appointment already synced" });
//   }

//   try {
//     // Fetch contact details from GHL for address and name
//     let contact;
//     try {
//       const contactResponse = await ghlApi.get(`/contacts/${appointment.contactId}`);
//       contact = contactResponse.data.contact;
//       console.log(`Fetched GHL contact: ${contact.id}, email: ${contact.email}`);
//     } catch (error) {
//       console.error(`Error fetching GHL contact ${appointment.contactId}:`, error.response?.data || error.message);
//       return res.status(500).json({ error: "Failed to fetch contact details" });
//     }

//     // Map GHL appointment to ServiceM8 job activity
//     const activityData = {
//       staff_uuid: SERVICE_M8_STAFF_UUID,
//       start_date: moment(appointment.startTime).tz("Australia/Brisbane").format("YYYY-MM-DD HH:mm:ss"),
//       end_date: moment(appointment.endTime).tz("Australia/Brisbane").format("YYYY-MM-DD HH:mm:ss"),
//       activity_description: appointment.title || "GHL Appointment",
//       activity_type: "Appointment",
//       job_address: appointment.location || contact.address1 || "No address provided",
//       related_contact_uuid: null, // Set below if contact synced
//     };

//     // Find or create ServiceM8 contact
//     let companyUuid;
//     const contactName = `${contact.firstName || ""} ${contact.lastName || ""}`.trim().toLowerCase();
//     const contactEmail = (contact.email || "").toLowerCase().trim();

//     try {
//       const companiesResponse = await serviceM8Api.get("/company.json");
//       const companies = companiesResponse.data;
//       const matchingCompany = companies.find(
//         (c) => c.email.toLowerCase().trim() === contactEmail || c.name.toLowerCase().trim() === contactName
//       );

//       if (matchingCompany) {
//         companyUuid = matchingCompany.uuid;
//         console.log(`Found existing ServiceM8 company: ${companyUuid}`);
//       } else {
//         const newCompanyResponse = await serviceM8Api.post("/company.json", { name: contactName || contact.email });
//         companyUuid = newCompanyResponse.headers["x-record-uuid"];
//         console.log(`Created new ServiceM8 company: ${companyUuid}`);

//         const companyContactData = {
//           company_uuid: companyUuid,
//           first: contact.firstName || "",
//           last: contact.lastName || "",
//           email: contact.email || "",
//           phone: contact.phone || contact.mobile || "",
//         };
//         await serviceM8Api.post("/companycontact.json", companyContactData);
//         console.log(`Created ServiceM8 contact for company: ${companyUuid}`);
//       }

//       // Fetch contact UUID for job activity
//       const contactsResponse = await serviceM8Api.get(`/companycontact.json?$filter=company_uuid eq '${companyUuid}'`);
//       const matchingContact = contactsResponse.data.find(
//         (c) => c.email.toLowerCase().trim() === contactEmail
//       );
//       if (matchingContact) {
//         activityData.related_contact_uuid = matchingContact.uuid;
//       }
//     } catch (error) {
//       console.error("Error syncing ServiceM8 contact:", error.response?.data || error.message);
//     }

//     // Create ServiceM8 job activity
//     try {
//       const response = await serviceM8Api.post("/jobactivity.json", activityData);
//       const activityUuid = response.headers["x-record-uuid"];
//       console.log(`Created ServiceM8 job activity: ${activityUuid} for appointment ${appointmentId}`);

//       // Mark as processed
//       processedAppointments.add(appointmentId);
//       saveProcessedAppointments(processedAppointments);

//       res.status(200).json({ message: "Appointment synced", activityUuid });
//     } catch (error) {
//       console.error("Error creating ServiceM8 job activity:", error.response?.data || error.message);
//       res.status(500).json({ error: "Failed to sync appointment" });
//     }
//   } catch (error) {
//     console.error("Webhook error:", error.response?.data || error.message);
//     res.status(500).json({ error: "Failed to process webhook" });
//   }
// });

// // Example: Protected API route (unchanged)
// app.get("/me", async (req, res) => {
//   try {
//     const accessToken = await getAccessToken();
//     const response = await axios.get("https://services.leadconnectorhq.com/users/me", {
//       headers: { Authorization: `Bearer ${accessToken}` },
//     });
//     res.json(response.data);
//   } catch (err) {
//     console.error("API call error:", err.response?.data || err.message);
//     res.status(500).send("Failed to fetch data from GHL.");
//   }
// });

// app.listen(PORT, () => {
//   console.log(`🚀 Server running on http://localhost:${PORT}`);
//   console.log(`👉 Visit http://localhost:${PORT}/auth to start OAuth flow`);
// });