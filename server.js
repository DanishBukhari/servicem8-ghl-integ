const express = require('express');
const axios = require('axios');
const cron = require('node-cron');
const dotenv = require('dotenv');
const fs = require('fs');
const fsPromises = require('fs').promises;
const moment = require('moment-timezone');
const multer = require('multer');
const FormData = require('form-data');
const path = require('path');

dotenv.config();

const app = express();
app.use(express.json());

// Configure multer for file uploads
const upload = multer({ dest: 'uploads/' });

const PORT = process.env.PORT || 3000;

// GHL credentials
const CLIENT_ID = process.env.GHL_CLIENT_ID;
const CLIENT_SECRET = process.env.GHL_CLIENT_SECRET;
const REDIRECT_URI = process.env.GHL_REDIRECT_URI || `http://localhost:${PORT}/callback`;

// ServiceM8 credentials
const SERVICE_M8_API_KEY = process.env.SERVICE_M8_API_KEY;
const SERVICE_M8_STAFF_UUID = process.env.SERVICE_M8_STAFF_UUID;
const GHL_WEBHOOK_URL = process.env.GHL_WEBHOOK_URL;

// Ensure uploads directory exists
const UPLOADS_DIR = path.join(__dirname, 'Uploads');
fsPromises.mkdir(UPLOADS_DIR, { recursive: true }).catch((error) => {
  console.error('Error creating uploads directory:', error.message);
});

// File-based token storage
const TOKEN_FILE = './tokens.json';

// Utility: Save tokens to file
function saveTokens(tokens) {
  try {
    fs.writeFileSync(TOKEN_FILE, JSON.stringify(tokens, null, 2));
    console.log('Tokens saved to tokens.json:', {
      access_token: tokens.access_token,
      refresh_token: tokens.refresh_token,
      created_at: tokens.created_at,
      expires_in: tokens.expires_in,
    });
  } catch (error) {
    console.error('Error saving tokens to file:', error.message);
  }
}

// Utility: Load tokens from file or config vars
function loadTokens() {
  // Prefer config vars if available
  if (process.env.GHL_ACCESS_TOKEN && process.env.GHL_REFRESH_TOKEN) {
    console.log('Loading tokens from Heroku config vars');
    return {
      access_token: process.env.GHL_ACCESS_TOKEN,
      refresh_token: process.env.GHL_REFRESH_TOKEN,
      created_at: parseInt(process.env.GHL_TOKEN_CREATED_AT) || Math.floor(Date.now() / 1000),
      expires_in: parseInt(process.env.GHL_TOKEN_EXPIRES_IN) || 3600,
    };
  }
  // Fallback to file
  if (fs.existsSync(TOKEN_FILE)) {
    console.log('Loading tokens from tokens.json');
    return JSON.parse(fs.readFileSync(TOKEN_FILE));
  }
  return null;
}

// OAuth: Redirect to GHL auth page
app.get('/auth', (req, res) => {
  const url = `https://marketplace.gohighlevel.com/oauth/chooselocation?response_type=code&client_id=${CLIENT_ID}&redirect_uri=${encodeURIComponent(
    REDIRECT_URI
  )}&scope=calendars.readonly+calendars.write+calendars%2Fevents.write+calendars%2Fevents.readonly+users.readonly+contacts.readonly`;
  res.redirect(url);
});

// OAuth: Handle callback and exchange code for tokens
app.get('/callback', async (req, res) => {
  const code = req.query.code;
  if (!code) return res.status(400).send('No code provided!');

  try {
    const params = new URLSearchParams({
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
      grant_type: 'authorization_code',
      code,
      redirect_uri: REDIRECT_URI,
    });

    const response = await axios.post(
      'https://services.leadconnectorhq.com/oauth/token',
      params,
      {
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      }
    );

    const tokens = {
      ...response.data,
      created_at: Math.floor(Date.now() / 1000),
    };

    saveTokens(tokens);

    res.send('✅ Tokens saved! Copy the tokens from logs and set as Heroku config vars using: heroku config:set GHL_ACCESS_TOKEN=... GHL_REFRESH_TOKEN=... GHL_TOKEN_CREATED_AT=... GHL_TOKEN_EXPIRES_IN=... --app assurefixinteg-8a33dda2d6df');
  } catch (err) {
    console.error('Token exchange error:', err.response?.data || err.message);
    res.status(500).send('Failed to exchange code for tokens.');
  }
});

// OAuth: Auto-refresh token if expired
async function getAccessToken() {
  let tokens = loadTokens();

  if (!tokens) {
    throw new Error('No tokens found. Visit /auth first.');
  }

  const now = Math.floor(Date.now() / 1000);
  const expiry = tokens.created_at ? tokens.created_at + tokens.expires_in : 0;

  if (now >= expiry) {
    console.log('🔄 Refreshing access token...');

    const params = new URLSearchParams({
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
      grant_type: 'refresh_token',
      refresh_token: tokens.refresh_token,
    });

    const response = await axios.post(
      'https://services.leadconnectorhq.com/oauth/token',
      params,
      {
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      }
    );

    tokens = {
      ...response.data,
      refresh_token: tokens.refresh_token,
      created_at: Math.floor(Date.now() / 1000),
    };

    saveTokens(tokens);
  }

  return tokens.access_token;
}

// Axios instance for ServiceM8
const serviceM8Api = axios.create({
  baseURL: 'https://api.servicem8.com/api_1.0',
  headers: { Accept: 'application/json', 'X-API-Key': SERVICE_M8_API_KEY },
});

// Axios instance for GHL (OAuth 2.0)
const ghlApi = axios.create({
  baseURL: 'https://services.leadconnectorhq.com',
  headers: { Accept: 'application/json' },
  version: '2021-04-15'
});

// Middleware to add GHL auth header dynamically
ghlApi.interceptors.request.use(async (config) => {
  const accessToken = await getAccessToken();
  console.log('Using GHL access token:', accessToken.substring(0, 20) + '...');
  config.headers.Authorization = `Bearer ${accessToken}`;
  return config;
});

// Store processed UUIDs and queue UUID
let processedJobs = new Set();
let processedContacts = new Set();
let quotesNewQueueUuid = '6bced9d5-c84a-4d47-84bf-22dff884744b';
const STATE_FILE = './state.json';
const PROCESSED_APPT_FILE = './processed_appointments.json';
const processedGhlContactIds = new Map();

// Load polling state
async function loadState() {
  try {
    const data = await fsPromises.readFile(STATE_FILE, 'utf8');
    const state = JSON.parse(data);
    processedJobs = new Set(state.processedJobs || []);
    processedContacts = new Set(state.processedContacts || []);
    console.log('Loaded state: processedJobs=', processedJobs.size, 'processedContacts=', processedContacts.size);
    return state.lastPollTimestamp || 0;
  } catch (error) {
    console.error('Error loading state:', error.message);
    return 0;
  }
}

// Save polling state
async function saveState(lastPollTimestamp) {
  try {
    await fsPromises.writeFile(
      STATE_FILE,
      JSON.stringify({
        lastPollTimestamp,
        processedJobs: Array.from(processedJobs),
        processedContacts: Array.from(processedContacts),
      })
    );
    console.log('Saved state: lastPollTimestamp=', lastPollTimestamp);
  } catch (error) {
    console.error('Error saving state:', error.message);
  }
}

// Load processed appointment IDs
function loadProcessedAppointments() {
  if (fs.existsSync(PROCESSED_APPT_FILE)) {
    return new Set(JSON.parse(fs.readFileSync(PROCESSED_APPT_FILE)));
  }
  return new Set();
}

// Save processed appointment IDs
function saveProcessedAppointments(processed) {
  fs.writeFileSync(PROCESSED_APPT_FILE, JSON.stringify([...processed], null, 2));
}

// Check new ServiceM8 contacts and sync to GHL
const checkNewContacts = async () => {
  try {
    console.log('Starting contact polling...');
    const lastPollTimestamp = await loadState();
    const currentTimestamp = Date.now();

    const accountTimezone = 'Australia/Brisbane';
    const now = moment().tz(accountTimezone);
    const twentyMinutesAgo = now.clone().subtract(20, 'minutes').format('YYYY-MM-DD HH:mm:ss');
    const filter = `$filter=edit_date gt '${twentyMinutesAgo}'`;

    const contactsResponse = await serviceM8Api.get(`/companycontact.json?${filter}`);
    const contacts = contactsResponse.data;
    console.log(`Fetched ${contacts.length} new or updated contacts from ServiceM8`);

    for (const contact of contacts) {
      const contactUuid = contact.uuid;
      if (processedContacts.has(contactUuid)) {
        console.log(`Contact ${contactUuid} already processed, skipping.`);
        continue;
      }

      const { first, last, email, phone, mobile, company_uuid } = contact;
      const contactName = `${first || ''} ${last || ''}`.trim();
      console.log(
        `Processing new contact - UUID: ${contactUuid}, Name: ${contactName}, Email: ${email}, Phone: ${
          phone || mobile
        }, Company UUID: ${company_uuid}`
      );

      if (!email && !contactName) {
        console.log(`No email or name for contact ${contactUuid}, skipping GHL creation.`);
        processedContacts.add(contactUuid);
        continue;
      }

      let ghlContactId = null;
      try {
        if (email) {
          const searchResponse = await ghlApi.get('/contacts/', {
            params: { query: email },
          });

          const existingContact = searchResponse.data.contacts.find(
            (c) => (c.email || '').toLowerCase().trim() === (email || '').toLowerCase().trim()
          );
          if (existingContact) {
            ghlContactId = existingContact.id;
            console.log(`Contact already exists in GHL: ${ghlContactId} for email ${email}`);
            processedContacts.add(contactUuid);
            continue;
          }
        }
      } catch (error) {
        console.error(
          `Error checking GHL contact for email ${email}:`,
          error.response ? error.response.data : error.message
        );
      }

      let addressDetails = {};
      try {
        const companyResponse = await serviceM8Api.get('/company.json', {
          params: { '$filter': `uuid eq '${company_uuid}'` },
        });

        const company = companyResponse.data[0] || {};
        addressDetails = {
          address1: company.billing_address || '',
          city: company.billing_city || '',
          state: company.billing_state || '',
          postalCode: company.billing_postcode || '',
        };
        console.log(`Fetched company address for ${company_uuid}:`, addressDetails);
      } catch (error) {
        console.error(
          `Error fetching company details for ${company_uuid}:`,
          error.response ? error.response.data : error.message
        );
      }

      try {
        const ghlContactResponse = await ghlApi.post('/contacts/', {
          firstName: first || '',
          lastName: last || '',
          name: contactName,
          email: email || '',
          phone: phone || mobile || '',
          address1: addressDetails.address1,
          city: addressDetails.city,
          state: addressDetails.state,
          postalCode: addressDetails.postalCode,
          source: 'ServiceM8 Integration',
        });

        ghlContactId = ghlContactResponse.data.contact.id;
        console.log(`Created GHL contact: ${ghlContactId} for email ${email}`);
        processedContacts.add(contactUuid);
      } catch (error) {
        console.error(
          'Error creating GHL contact:',
          error.response ? error.response.data : error.message
        );
      }
    }

    await saveState(currentTimestamp);
    console.log('Contact polling completed.');
  } catch (error) {
    console.error('Error polling contacts:', error.response ? error.response.data : error.message);
  }
};

// Check payment status and trigger GHL webhook
const checkPaymentStatus = async () => {
  try {
    console.log('Starting payment status check...');
    const accountTimezone = 'Australia/Brisbane';
    const now = moment().tz(accountTimezone);
    const twentyMinutesAgo = now.clone().subtract(20, 'minutes').format('YYYY-MM-DD HH:mm:ss');
    const targetDate = moment('2025-08-20').tz(accountTimezone).startOf('day').format('YYYY-MM-DDTHH:mm:ss');
    console.log(`Checking payments edited after ${twentyMinutesAgo} for jobs completed on or after ${targetDate}`);

    const paymentFilter = `$filter=edit_date gt '${twentyMinutesAgo}'`;
    const paymentsResponse = await serviceM8Api.get(`/jobpayment.json?${paymentFilter}`);
    const payments = paymentsResponse.data;
    console.log(`Fetched ${payments.length} payments edited in the last 20 minutes`);

    for (const payment of payments) {
      const paymentUuid = payment.uuid;
      const jobUuid = payment.job_uuid;
      console.log(`Processing payment ${paymentUuid} for job ${jobUuid}`);

      if (processedJobs.has(paymentUuid)) {
        console.log(`Payment ${paymentUuid} already processed, skipping.`);
        continue;
      }

      let job;
      try {
        const jobResponse = await serviceM8Api.get(`/job.json?$filter=uuid eq '${jobUuid}'`);
        job = jobResponse.data[0];
        console.log(`Fetched job ${jobUuid}: status=${job?.status}, company_uuid=${job?.company_uuid}, edit_date=${job?.edit_date}`);
      } catch (error) {
        console.error(`Error fetching job ${jobUuid}:`, error.response ? error.response.data : error.message);
        continue;
      }
      if (!job) {
        console.log(`No job found for job_uuid ${jobUuid}, skipping payment ${paymentUuid}`);
        continue;
      }

      if (job.status.toLowerCase() !== 'completed') {
        console.log(`Job ${jobUuid} is not completed (status: ${job.status}), skipping payment ${paymentUuid}`);
        continue;
      }

      let completionDate = null;
      try {
        const jobActivitiesResponse = await serviceM8Api.get(`/jobactivity.json?$filter=job_uuid eq '${jobUuid}'`);
        const jobActivities = jobActivitiesResponse.data;
        console.log(`Fetched ${jobActivities.length} activities for job ${jobUuid}`);
        let maxEndDate = null;
        for (const activity of jobActivities) {
          if (activity.end_date) {
            console.log(`Activity for job ${jobUuid}: end_date=${activity.end_date}`);
            if (!maxEndDate || moment(activity.end_date).tz(accountTimezone).isAfter(moment(maxEndDate).tz(accountTimezone))) {
              maxEndDate = activity.end_date;
            }
          }
        }
        if (maxEndDate) {
          completionDate = moment(maxEndDate).tz(accountTimezone);
          console.log(`Using job activity end_date as completion date: ${completionDate.format('YYYY-MM-DD HH:mm:ss')}`);
        } else {
          completionDate = moment(job.edit_date).tz(accountTimezone);
          console.log(`No activities found, using job edit_date as completion date: ${completionDate.format('YYYY-MM-DD HH:mm:ss')}`);
        }
      } catch (error) {
        console.error(`Error fetching job activities for job ${jobUuid}:`, error.response ? error.response.data : error.message);
        completionDate = moment(job.edit_date).tz(accountTimezone);
        console.log(`Failed to fetch activities, using job edit_date as completion date: ${completionDate.format('YYYY-MM-DD HH:mm:ss')}`);
      }

      if (!completionDate) {
        console.log(`No completion date available for job ${jobUuid}, skipping payment ${paymentUuid}`);
        continue;
      }
      const targetMoment = moment(targetDate).tz(accountTimezone);
      console.log(`Job ${jobUuid} completion date: ${completionDate.format('YYYY-MM-DD HH:mm:ss')}, target: ${targetMoment.format('YYYY-MM-DD HH:mm:ss')}`);
      if (!completionDate.isSameOrAfter(targetMoment)) {
        console.log(`Payment ${paymentUuid} belongs to job ${jobUuid} not completed on or after May 24, 2025, skipping.`);
        continue;
      }

      let categoryName = '';
      if (job.category_uuid) {
        try {
          const categoryResponse = await serviceM8Api.get(`/category.json?$filter=uuid eq '${job.category_uuid}'`);
          const category = categoryResponse.data[0];
          if (category) {
            categoryName = (category.name || '').trim().toLowerCase();
            console.log(`Fetched category for job ${jobUuid}: ${categoryName}`);
          }
        } catch (error) {
          console.error(`Error fetching category for job ${jobUuid}:`, error.response ? error.response.data : error.message);
        }
      }

      if (categoryName === 'real estate agents') {
        console.log(`Skipping webhook for job ${jobUuid} as category is Real Estate Agents`);
        processedJobs.add(paymentUuid);
        continue;
      }

      let ghlContactId = '';
      if (job.job_description) {
        const ghlContactIdMatch = job.job_description.match(/GHL Contact ID: ([a-zA-Z0-9]+)/);
        ghlContactId = ghlContactIdMatch ? ghlContactIdMatch[1] : '';
        console.log(`Extracted GHL Contact ID: ${ghlContactId} for job ${jobUuid}`);
      }
      const companyUuid = job.company_uuid;
      if (!companyUuid) {
        console.log(`No company_uuid for job ${jobUuid}, skipping payment ${paymentUuid}`);
        continue;
      }

      let clientEmail = '';
      try {
        const companyResponse = await serviceM8Api.get('/companycontact.json', {
          params: { '$filter': `company_uuid eq '${companyUuid}'` },
        });
        const company = companyResponse.data;
        const primaryContact = company.find(c => c.email) || {};
        clientEmail = (primaryContact.email || '').trim().toLowerCase();
        console.log(`Extracted client email: ${clientEmail} for company ${companyUuid}`);
      } catch (error) {
        console.error(`Error fetching contact for company ${companyUuid}:`, error.response ? error.response.data : error.message);
      }

      const contactKey = ghlContactId || clientEmail;
      if (contactKey && processedContacts.has(contactKey)) {
        console.log(`Contact ${contactKey} already triggered, skipping payment ${paymentUuid}`);
        continue;
      }

      if (
        payment.active === 1 &&
        payment.amount > 0 &&
        moment(payment.edit_date).tz(accountTimezone).isAfter(twentyMinutesAgo)
      ) {
        console.log(`Recent paid payment found: UUID ${paymentUuid}, Amount ${payment.amount}, Job UUID ${jobUuid}, Edit Date ${payment.edit_date}`);
        const webhookPayload = {
          paymentUuid: paymentUuid,
          jobUuid: jobUuid,
          clientEmail: clientEmail || '',
          ghlContactId: ghlContactId,
          status: 'Invoice Paid',
        };
        try {
          const webhookResponse = await axios.post(GHL_WEBHOOK_URL, webhookPayload, {
            headers: {
              Authorization: `Bearer ${await getAccessToken()}`,
              'Content-Type': 'application/json',
            },
          });
          console.log(
            `GHL webhook triggered for payment ${paymentUuid}: status=${webhookResponse.status}, response=${JSON.stringify(webhookResponse.data)}`
          );
          processedJobs.add(paymentUuid);
          if (contactKey) processedContacts.add(contactKey);
        } catch (webhookError) {
          console.error(
            `Failed to trigger GHL webhook for payment ${paymentUuid}:`,
            webhookError.response ? webhookError.response.data : webhookError.message
          );
        }
      } else {
        console.log(`Payment ${paymentUuid} is not paid or not recent, skipping. Details:`, {
          active: payment.active,
          amount: payment.amount,
          edit_date: payment.edit_date,
        });
      }
    }

    await saveState(Date.now());
    console.log('Payment status check completed.');
  } catch (error) {
    console.error('Error checking payment status:', error.response ? error.response.data : error.message);
  }
};

// Helper function to get file extension from MIME type
function getFileExtensionFromMime(mime) {
  const mimeToExt = {
    'image/png': '.png',
    'image/jpeg': '.jpg',
    'image/gif': '.gif',
    'application/pdf': '.pdf',
  };
  return mimeToExt[mime.toLowerCase()] || '.dat';
}

// Helper function to assign phone or mobile based on prefix
function assignPhoneFields(data, phoneNumber) {
  if (phoneNumber.startsWith('07')) {
    data.phone = phoneNumber;
    data.mobile = '';
  } else if (phoneNumber.startsWith('04')) {
    data.mobile = phoneNumber;
    data.phone = '';
  } else {
    data.phone = phoneNumber;
    data.mobile = '';
  }
}

// Endpoint for GHL to create a job in ServiceM8
app.post('/ghl-create-job', upload.array('photos'), async (req, res) => {
  try {
    console.log('Starting job creation from GHL...');
    const { firstName, lastName, email, phone, address, jobDescription, ghlContactId } = req.body;

    if (!firstName || !lastName || !email || !ghlContactId) {
      console.log('Missing required fields:', { firstName, lastName, email, ghlContactId });
      return res.status(400).json({ error: 'Missing required fields' });
    }

    const now = Date.now();
    const lastProcessed = processedGhlContactIds.get(ghlContactId);
    if (lastProcessed && now - lastProcessed < 5000) {
      console.log(`Duplicate job creation attempt for ghlContactId ${ghlContactId}, skipping`);
      return res.status(200).json({ message: 'Job creation skipped (duplicate request)' });
    }
    processedGhlContactIds.set(ghlContactId, now);
    console.log(`Processing job creation for ghlContactId ${ghlContactId}`);

    const queueUuid = '6bced9d5-c84a-4d47-84bf-22dff884744b';
    if (!queueUuid) {
      console.log('Failed to fetch "To Be Quoted (Desk Quote)" queue UUID');
      return res.status(500).json({ error: 'Failed to fetch "To Be Quoted (Desk Quote)" queue UUID' });
    }

    const companiesResponse = await serviceM8Api.get('/company.json');
    const companies = companiesResponse.data;
    console.log(`Fetched ${companies.length} companies from ServiceM8`);

    let companyUuid;
    const fullName = `${firstName} ${lastName}`.trim().toLowerCase();
    const inputEmail = (email || '').toLowerCase().trim();
    const matchingCompany = companies.find((company) => {
      const companyEmail = (company.email || '').toLowerCase().trim();
      const companyName = (company.name || '').toLowerCase().trim();
      console.log(
        `Comparing company: email=${companyEmail} vs ${inputEmail}, name=${companyName} vs ${fullName}`
      );
      return companyEmail === inputEmail || companyName === fullName;
    });

    if (matchingCompany) {
      companyUuid = matchingCompany.uuid;
      console.log(
        `Client already exists: ${companyUuid} for email ${email}, phone: ${matchingCompany.phone}`
      );
    } else {
      console.log(`Creating new client with name ${fullName}, email ${email}, phone ${phone}`);
      const newCompanyResponse = await serviceM8Api.post('/company.json', { name: fullName });
      companyUuid = newCompanyResponse.headers['x-record-uuid'];
      console.log(`Client created: ${companyUuid} for email ${email} with phone ${phone}`);
    }

    const contactsResponse = await serviceM8Api.get(`/companycontact.json?$filter=company_uuid eq '${companyUuid}'`);
    const existingContacts = contactsResponse.data;
    const matchingContact = existingContacts.find((contact) => {
      const contactEmail = (contact.email || '').toLowerCase().trim();
      const contactName = `${contact.first || ''} ${contact.last || ''}`.trim().toLowerCase();
      return contactEmail === inputEmail || contactName === fullName;
    });

    if (!matchingContact) {
      const companyContactData = {
        company_uuid: companyUuid,
        first: firstName,
        last: lastName,
        email: email,
      };
      if (phone) {
        assignPhoneFields(companyContactData, phone);
      }
      await serviceM8Api.post('/companycontact.json', companyContactData);
      console.log(`Contact added for client: ${companyUuid}`);
    } else {
      console.log(`Contact already exists for company ${companyUuid}, skipping creation.`);
    }

    let message = '';
    try {
      const contactResponse = await ghlApi.get(`/contacts/${ghlContactId}`, {
        params: { include: 'customFields' },
      });
      const contact = contactResponse.data.contact;
      console.log(`Fetched GHL contact data for ${ghlContactId}:`, JSON.stringify(contact, null, 2));

      let customFields = contact.customFields || contact.custom_fields || contact.customField || contact.custom_field_values || contact.fields || [];
      if (!Array.isArray(customFields)) {
        console.log(`Custom fields not an array, attempting to convert object:`, customFields);
        customFields = Object.values(customFields).filter(f => f && typeof f === 'object');
      }

      if (customFields.length > 0) {
        const messageField = customFields.find(field => 
          (field.name && (field.name === 'Message' || field.name.toLowerCase() === 'message')) ||
          (field.label && (field.label === 'Message' || field.label.toLowerCase() === 'message')) ||
          (field.id && (field.id === 'zNzhT7M36keauEw2TCtf' || field.id === 'VvhUQGlzD80PnB9aYdL4'))
        );
        if (messageField && (messageField.value || messageField.values)) {
          message = messageField.value || (messageField.values && messageField.values[0]) || '';
          console.log(`Message retrieved for contact ${ghlContactId}: ${message}`);
        } else {
          console.log(`No Message field found in customFields for contact ${ghlContactId}:`, customFields);
        }
      } else {
        console.log(`No customFields available or accessible for contact ${ghlContactId}`);
      }
    } catch (error) {
      console.error('Failed to fetch contact message from GHL (proceeding without message):', error.response ? error.response.data : error.message);
    }

    const jobDescriptionWithMessage = message
      ? `Enquiry details: ${message}\nGHL Contact ID: ${ghlContactId}\n${jobDescription || ''}`
      : `GHL Contact ID: ${ghlContactId}\n${jobDescription || ''}`;
    const jobData = {
      company_uuid: companyUuid,
      status: 'Quote',
      queue_uuid: queueUuid,
      job_address: address,
      job_description: jobDescriptionWithMessage,
    };

    const jobResponse = await serviceM8Api.post('/job.json', jobData);
    const jobUuid = jobResponse.headers['x-record-uuid'];
    console.log(`Job created: ${jobUuid} in queue ${queueUuid}`);

    const jobContactData = {
      job_uuid: jobUuid,
      type: 'Job Contact',
      first: firstName,
      last: lastName,
      email: email,
    };
    if (phone) {
      assignPhoneFields(jobContactData, phone);
    }
    await serviceM8Api.post('/jobcontact.json', jobContactData);
    console.log(`Job contact added for job: ${jobUuid}`);

    let photoData = [];
    try {
      const contactResponse = await ghlApi.get(`/contacts/${ghlContactId}`);
      const contact = contactResponse.data.contact;
      console.log(`Fetched GHL contact data for ${ghlContactId}`);

      if (contact.customField) {
        for (const field of contact.customField) {
          if (field.value && typeof field.value === 'object' && !Array.isArray(field.value)) {
            for (const [uuid, entry] of Object.entries(field.value)) {
              if (
                entry.url &&
                entry.meta &&
                entry.meta.mimetype &&
                entry.meta.mimetype.match(/image\/(png|jpeg|jpg)/i)
              ) {
                const fileExtension = getFileExtensionFromMime(entry.meta.mimetype);
                photoData.push({
                  url: entry.url,
                  documentId: entry.documentId,
                  filename: entry.meta.originalname || `photo-${uuid}-${Date.now()}${fileExtension}`,
                  mimetype: entry.meta.mimetype,
                });
              }
            }
          }
        }
      } else {
        console.log(`No customField found in GHL contact ${ghlContactId}. Available properties: ${Object.keys(contact)}`);
      }

      if (photoData.length === 0) {
        try {
          const attachmentsResponse = await ghlApi.get(`/contacts/${ghlContactId}/attachments`);
          const attachments = attachmentsResponse.data.attachments || [];
          console.log(`Fetched ${attachments.length} attachments from GHL contact ${ghlContactId}`);

          for (const attachment of attachments) {
            if (
              attachment.url &&
              attachment.mimetype &&
              attachment.mimetype.match(/image\/(png|jpeg|jpg)/i)
            ) {
              const fileExtension = getFileExtensionFromMime(attachment.mimetype);
              photoData.push({
                url: attachment.url,
                documentId: attachment.documentId || attachment.url.split('/').pop(),
                filename: attachment.filename || `attachment-${Date.now()}${fileExtension}`,
                mimetype: attachment.mimetype,
              });
            }
          }
        } catch (attachmentError) {
          console.log('Attachments endpoint not available or failed:', attachmentError.response ? attachmentError.response.data : attachmentError.message);
        }
      }

      console.log(`Fetched ${photoData.length} photos for contact ${ghlContactId}:`, photoData.map(p => p.url));
    } catch (error) {
      console.error(
        'Error fetching GHL contact images:',
        error.response ? error.response.data : error.message
      );
    }

    for (const photo of photoData) {
      const { url: photoUrl, documentId, filename, mimetype } = photo;
      let tempPath;
      let attachmentUuid;

      try {
        tempPath = path.join(UPLOADS_DIR, filename);
        console.log(`Downloading image from ${photoUrl} to ${tempPath}`);
        let downloadResponse;

        try {
          downloadResponse = await axios.get(`https://services.leadconnectorhq.com/documents/download/${documentId}`, {
            headers: {
              Authorization: `Bearer ${await getAccessToken()}`,
            },
            responseType: 'stream',
          });
        } catch (primaryError) {
          console.log(`Primary download failed for ${photoUrl}:`, primaryError.response ? primaryError.response.data : primaryError.message);
          try {
            downloadResponse = await axios.get(photoUrl, {
              headers: {
                Authorization: `Bearer ${await getAccessToken()}`,
              },
              responseType: 'stream',
            });
          } catch (fallbackError) {
            console.error(`Fallback download failed for ${photoUrl}:`, fallbackError.response ? fallbackError.response.data : fallbackError.message);
            continue;
          }
        }

        const contentType = downloadResponse.headers['content-type'] || '';
        if (!contentType.match(/image\/(png|jpeg|jpg)/i)) {
          console.log(`Skipping non-image URL ${photoUrl}: Content-Type ${contentType}`);
          continue;
        }

        const writer = fs.createWriteStream(tempPath);
        downloadResponse.data.pipe(writer);

        await new Promise((resolve, reject) => {
          writer.on('finish', resolve);
          writer.on('error', reject);
        });

        const stats = await fsPromises.stat(tempPath);
        console.log(`Downloaded image to ${tempPath}, size: ${stats.size} bytes`);
        if (stats.size === 0) {
          console.error(`Downloaded file ${tempPath} is empty`);
          continue;
        }

        try {
          await fsPromises.access(tempPath, fs.constants.R_OK);
        } catch (error) {
          console.error(`File ${tempPath} is not accessible:`, error.message);
          continue;
        }

        const fileExtension = getFileExtensionFromMime(mimetype);
        const attachmentData = {
          related_object: 'job',
          related_object_uuid: jobUuid,
          attachment_name: filename,
          file_type: fileExtension,
        };
        const attachmentResponse = await serviceM8Api.post('/Attachment.json', attachmentData);
        attachmentUuid = attachmentResponse.headers['x-record-uuid'];
        console.log(`Created attachment record for job ${jobUuid}, UUID: ${attachmentUuid}, file_type: ${fileExtension}`);

        const fileData = await fsPromises.readFile(tempPath);
        await serviceM8Api.put(`/Attachment/${attachmentUuid}.file`, fileData, {
          headers: {
            'Content-Type': 'application/octet-stream',
          },
        });
        console.log(`Uploaded file for attachment ${attachmentUuid}`);
      } catch (error) {
        console.error(`Error processing image ${filename}:`, error.message);
      }
    }

    console.log(`Job creation completed for job ${jobUuid}`);
    res.status(200).json({ message: 'Job created successfully', jobUuid });
  } catch (error) {
    console.error('Error creating job:', error.response ? error.response.data : error.message);
    res.status(500).json({ error: 'Failed to create job' });
  }
});

// Endpoint to handle GHL webhook for new appointments
app.post('/ghl-appointment-sync', async (req, res) => {
  const processedAppointments = loadProcessedAppointments();
  const appointment = req.body;

  if (!appointment || !appointment.id || !appointment.contactId || !appointment.startTime) {
    console.error('Invalid webhook payload:', req.body);
    return res.status(400).json({ error: 'Missing appointment data' });
  }

  const appointmentId = appointment.id;
  if (processedAppointments.has(appointmentId)) {
    console.log(`Appointment ${appointmentId} already processed, skipping.`);
    return res.status(200).json({ message: 'Appointment already synced' });
  }

  try {
    const startTime = moment(appointment.startTime, 'dddd, MMMM D, YYYY h:mm A').tz('Australia/Brisbane');
    const endTime = moment(appointment.endTime, 'dddd, MMMM D, YYYY h:mm A').tz('Australia/Brisbane');
    if (!startTime.isValid() || !endTime.isValid()) {
      console.error('Invalid date format for startTime or endTime:', appointment.startTime, appointment.endTime);
      return res.status(400).json({ error: 'Invalid date format' });
    }

    let contact;
    try {
      const contactResponse = await ghlApi.get(`/contacts/${appointment.contactId}`);
      contact = contactResponse.data.contact;
      console.log(`Fetched GHL contact: ${contact.id}, email: ${contact.email}, locationId: ${contact.locationId}`);
    } catch (error) {
      console.error(`Error fetching GHL contact ${appointment.contactId}:`, error.response?.data || error.message);
      return res.status(500).json({ error: 'Failed to fetch contact details' });
    }

    const activityData = {
      staff_uuid: SERVICE_M8_STAFF_UUID,
      start_date: startTime.format('YYYY-MM-DD HH:mm:ss'),
      end_date: endTime.format('YYYY-MM-DD HH:mm:ss'),
      activity_description: appointment.title || 'GHL Appointment',
      activity_type: 'Appointment',
      job_address: appointment.location || contact.address1 || 'No address provided',
      related_contact_uuid: null,
    };

    let companyUuid;
    const contactName = `${contact.firstName || ''} ${contact.lastName || ''}`.trim().toLowerCase();
    const contactEmail = (contact.email || '').toLowerCase().trim();

    try {
      const companiesResponse = await serviceM8Api.get('/company.json');
      const companies = companiesResponse.data;
      const matchingCompany = companies.find(
        (c) => c.email.toLowerCase().trim() === contactEmail || c.name.toLowerCase().trim() === contactName
      );

      if (matchingCompany) {
        companyUuid = matchingCompany.uuid;
        console.log(`Found existing ServiceM8 company: ${companyUuid}`);
      } else {
        const newCompanyResponse = await serviceM8Api.post('/company.json', { name: contactName || contact.email });
        companyUuid = newCompanyResponse.headers['x-record-uuid'];
        console.log(`Created new ServiceM8 company: ${companyUuid}`);

        const companyContactData = {
          company_uuid: companyUuid,
          first: contact.firstName || '',
          last: contact.lastName || '',
          email: contact.email || '',
          phone: contact.phone || contact.mobile || '',
        };
        await serviceM8Api.post('/companycontact.json', companyContactData);
        console.log(`Created ServiceM8 contact for company: ${companyUuid}`);
      }

      const contactsResponse = await serviceM8Api.get(`/companycontact.json?$filter=company_uuid eq '${companyUuid}'`);
      const matchingContact = contactsResponse.data.find(
        (c) => c.email.toLowerCase().trim() === contactEmail
      );
      if (matchingContact) {
        activityData.related_contact_uuid = matchingContact.uuid;
      }
    } catch (error) {
      console.error('Error syncing ServiceM8 contact:', error.response?.data || error.message);
    }

    try {
      const response = await serviceM8Api.post('/jobactivity.json', activityData);
      const activityUuid = response.headers['x-record-uuid'];
      console.log(`Created ServiceM8 job activity: ${activityUuid} for appointment ${appointmentId}`);

      processedAppointments.add(appointmentId);
      saveProcessedAppointments(processedAppointments);

      res.status(200).json({ message: 'Appointment synced', activityUuid });
    } catch (error) {
      console.error('Error creating ServiceM8 job activity:', error.response?.data || error.message);
      res.status(500).json({ error: 'Failed to sync appointment' });
    }
  } catch (error) {
    console.error('Webhook error:', error.response?.data || error.message);
    res.status(500).json({ error: 'Failed to process webhook' });
  }
});
// Temporary endpoints for testing
app.get('/test-payment-check', async (req, res) => {
  console.log('Triggering test payment check...');
  await checkPaymentStatus();
  res.send('Payment check triggered');
});

app.get('/test-contact-check', async (req, res) => {
  console.log('Triggering test contact check...');
  await checkNewContacts();
  res.send('Contact check triggered');
});

app.get('/test-contact/:id', async (req, res) => {
  try {
    const contactId = req.params.id;
    const response = await ghlApi.get(`/contacts/${contactId}`);
    res.json(response.data);
  } catch (error) {
    console.error('Test contact fetch error:', error.response?.data || error.message);
    res.status(500).json({ error: error.response?.data || error.message });
  }
});

// Schedule polling
cron.schedule('*/2880 * * * *', () => {
  console.log('Scheduled polling for new contacts...');
  checkNewContacts();
});

cron.schedule('*/2880 * * * *', () => {
  console.log('Scheduled polling for payment status...');
  checkPaymentStatus();
});

app.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
  console.log(`👉 Visit http://localhost:${PORT}/auth to start OAuth flow`);
});