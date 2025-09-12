const express = require('express');
const axios = require('axios');
const cron = require('node-cron');
const dotenv = require('dotenv');
const fs = require('fs');
const fsPromises = require('fs').promises;
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
const GHL_API_KEY = process.env.GHL_API_KEY;
const HEROKU_API_KEY = process.env.HEROKU_API_KEY;
const APP_NAME = 'assurefixinteg-8a33dda2d6df';

// ServiceM8 credentials
const SERVICE_M8_API_KEY = process.env.SERVICE_M8_API_KEY;
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
      access_token: tokens.access_token.substring(0, 20) + '...',
      refresh_token: tokens.refresh_token.substring(0, 20) + '...',
      created_at: tokens.created_at,
      expires_in: tokens.expires_in,
      scope: tokens.scope,
    });
  } catch (error) {
    console.error('Error saving tokens to file:', error.message);
  }
}

// Utility: Load tokens from file or config vars
function loadTokens() {
  if (process.env.GHL_ACCESS_TOKEN && process.env.GHL_REFRESH_TOKEN) {
    console.log('Loading tokens from Heroku config vars');
    return {
      access_token: process.env.GHL_ACCESS_TOKEN,
      refresh_token: process.env.GHL_REFRESH_TOKEN,
      created_at: parseInt(process.env.GHL_TOKEN_CREATED_AT) || Math.floor(Date.now() / 1000),
      expires_in: parseInt(process.env.GHL_TOKEN_EXPIRES_IN) || 3600,
    };
  }
  if (fs.existsSync(TOKEN_FILE)) {
    console.log('Loading tokens from tokens.json');
    return JSON.parse(fs.readFileSync(TOKEN_FILE));
  }
  console.log('No tokens found');
  return null;
}

// OAuth: Redirect to GHL auth page
app.get('/auth', (req, res) => {
  const url = `https://marketplace.gohighlevel.com/oauth/chooselocation?response_type=code&client_id=${CLIENT_ID}&redirect_uri=${encodeURIComponent(
    REDIRECT_URI
  )}&scope=calendars.readonly+calendars.write+calendars%2Fevents.write+calendars%2Fevents.readonly+users.readonly+contacts.readonly`;
  console.log('Redirecting to GHL OAuth:', url);
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

    // Save to Heroku config vars
    await axios.patch(
      `https://api.heroku.com/apps/${APP_NAME}/config-vars`,
      {
        GHL_ACCESS_TOKEN: tokens.access_token,
        GHL_REFRESH_TOKEN: tokens.refresh_token,
        GHL_TOKEN_CREATED_AT: tokens.created_at.toString(),
        GHL_TOKEN_EXPIRES_IN: tokens.expires_in.toString(),
      },
      {
        headers: {
          Authorization: `Bearer ${HEROKU_API_KEY}`,
          Accept: 'application/vnd.heroku+json; version=3',
          'Content-Type': 'application/json',
        },
      }
    );
    console.log('Tokens saved to Heroku config vars.');

    saveTokens(tokens);

    res.send('✅ Tokens saved! Tokens have been set as Heroku config vars.');
  } catch (err) {
    console.error('Token exchange error:', err.response?.data || err.message);
    res.status(500).send('Failed to exchange code for tokens.');
  }
});

// OAuth: Auto-refresh token every 12 hours
async function refreshGHLTokens() {
  try {
    let tokens = loadTokens();
    if (!tokens) {
      throw new Error('No tokens found for refresh.');
    }

    console.log('🔄 Refreshing GHL tokens...');
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

    const newTokens = {
      ...response.data,
      refresh_token: tokens.refresh_token,
      created_at: Math.floor(Date.now() / 1000),
    };

    // Save to Heroku config vars
    await axios.patch(
      `https://api.heroku.com/apps/${APP_NAME}/config-vars`,
      {
        GHL_ACCESS_TOKEN: newTokens.access_token,
        GHL_REFRESH_TOKEN: newTokens.refresh_token,
        GHL_TOKEN_CREATED_AT: newTokens.created_at.toString(),
        GHL_TOKEN_EXPIRES_IN: newTokens.expires_in.toString(),
      },
      {
        headers: {
          Authorization: `Bearer ${HEROKU_API_KEY}`,
          Accept: 'application/vnd.heroku+json; version=3',
          'Content-Type': 'application/json',
        },
      }
    );
    console.log('Tokens refreshed and saved to Heroku config vars.');

    saveTokens(newTokens);
  } catch (error) {
    console.error('Error refreshing tokens:', error.response?.data || error.message);
  }
}

// OAuth: Auto-refresh token if expired
async function getAccessToken() {
  let tokens = loadTokens();

  if (!tokens) {
    throw new Error('No tokens found. Visit /auth first.');
  }

  const now = Math.floor(Date.now() / 1000);
  const expiry = tokens.created_at ? tokens.created_at + tokens.expires_in : 0;

  if (now >= expiry) {
    await refreshGHLTokens();
    tokens = loadTokens();
  }

  return tokens.access_token;
}

// Axios instance for ServiceM8
const serviceM8Api = axios.create({
  baseURL: 'https://api.servicem8.com/api_1.0',
  headers: { Accept: 'application/json', 'X-API-Key': SERVICE_M8_API_KEY },
});

// Axios instance for GHL v1 API (using API key)
const ghlApi = axios.create({
  baseURL: 'https://rest.gohighlevel.com/v1',
  headers: {
    Accept: 'application/json',
    Authorization: `Bearer ${GHL_API_KEY}`,
  },
});

// Axios instance for GHL v2 API (using OAuth)
const ghlApiV2 = axios.create({
  baseURL: 'https://services.leadconnectorhq.com',
  headers: { Accept: 'application/json', Version: '2021-04-15' },
});

// Middleware to add GHL v2 auth header dynamically
ghlApiV2.interceptors.request.use(async (config) => {
  const accessToken = await getAccessToken();
  console.log('Using GHL v2 access token:', accessToken.substring(0, 20) + '...');
  config.headers.Authorization = `Bearer ${accessToken}`;
  return config;
});

// Store processed UUIDs
let processedJobs = new Set();
let processedContacts = new Set();
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

// Helper: Format date to ServiceM8 format (YYYY-MM-DD HH:mm:ss)
function formatDate(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');
  const seconds = String(date.getSeconds()).padStart(2, '0');
  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

// Check new ServiceM8 contacts and sync to GHL
const checkNewContacts = async () => {
  try {
    console.log('Starting contact polling...');
    const lastPollTimestamp = await loadState();
    const currentTimestamp = Date.now();

    const now = new Date();
    const twentyMinutesAgo = new Date(now.getTime() - 20 * 60 * 1000).toISOString().slice(0, 19).replace('T', ' ');
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

// Check completed jobs and trigger review request
const checkCompletedJobs = async () => {
  try {
    console.log('Starting job completion check...');
    const currentTimestamp = Date.now();
    const now = new Date();
    const twentyFourHoursAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000).toISOString().slice(0, 19).replace('T', ' ');
    const jobFilter = `$filter=status eq 'Completed' and edit_date gt '${twentyFourHoursAgo}'`;
    const jobsResponse = await serviceM8Api.get(`/job.json?${jobFilter}`);
    const jobs = jobsResponse.data;
    console.log(`Fetched ${jobs.length} completed jobs in the last 24 hours`);

    for (const job of jobs) {
      const jobUuid = job.uuid;
      if (processedJobs.has(jobUuid)) {
        console.log(`Job ${jobUuid} already processed, skipping.`);
        continue;
      }

      console.log(`Processing completed job ${jobUuid}`);

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

      if (categoryName === 'real estate agents' || categoryName === 'property managers') {
        console.log(`Skipping webhook for job ${jobUuid} as category is ${categoryName}`);
        processedJobs.add(jobUuid);
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
        console.log(`No company_uuid for job ${jobUuid}, skipping.`);
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
        console.log(`Contact ${contactKey} already triggered, skipping job ${jobUuid}`);
        continue;
      }

      // Trigger review request webhook for completed job
      const webhookPayload = {
        jobUuid: jobUuid,
        clientEmail: clientEmail || '',
        ghlContactId: ghlContactId,
        status: 'Job Completed',
      };
      try {
        const webhookResponse = await axios.post(GHL_WEBHOOK_URL, webhookPayload, {
          headers: {
            Authorization: `Bearer ${await getAccessToken()}`,
            'Content-Type': 'application/json',
          },
        });
        console.log(
          `GHL webhook triggered for job ${jobUuid}: status=${webhookResponse.status}, response=${JSON.stringify(webhookResponse.data)}`
        );
        processedJobs.add(jobUuid);
        if (contactKey) processedContacts.add(contactKey);
      } catch (webhookError) {
        console.error(
          `Failed to trigger GHL webhook for job ${jobUuid}:`,
          webhookError.response ? webhookError.response.data : webhookError.message
        );
      }
    }

    await saveState(currentTimestamp);
    console.log('Job completion check completed.');
  } catch (error) {
    console.error('Error checking job completion:', error.response ? error.response.data : error.message);
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
  if (phoneNumber && phoneNumber.startsWith('04')) {
    data.mobile = phoneNumber;
    data.phone = '';
  } else if (phoneNumber) {
    data.phone = phoneNumber;
    data.mobile = '';
  }
}

// Endpoint for GHL to create a job in ServiceM8
app.post('/ghl-create-job', upload.array('photos'), async (req, res) => {
  try {
    console.log('Starting job creation from GHL...');
    const { firstName, lastName, email, phone, address, jobDescription, ghlContactId, source, urgency } = req.body;
    console.log('Received job creation data:', { firstName, lastName, email, phone, address, jobDescription, ghlContactId, source, urgency });

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
      console.log('Invalid queue UUID');
      return res.status(500).json({ error: 'Invalid queue UUID' });
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
      console.log(`Client already exists: ${companyUuid} for email ${email}, phone: ${matchingCompany.phone}`);
    } else {
      console.log(`Creating new client with name ${fullName}, email ${email}, phone ${phone}`);
      const newCompanyResponse = await serviceM8Api.post('/company.json', {
        name: fullName,
        email: email || '',
        phone: phone || '',
      });
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
    let fetchedSource = source || '';
    let fetchedUrgency = urgency || '';
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
          (field.name && (field.name.toLowerCase() === 'message')) ||
          (field.id && (field.id === 'zNzhT7M36keauEw2TCtf' || field.id === 'VvhUQGlzD80PnB9aYdL4'))
        );
        if (messageField && (messageField.value || messageField.values)) {
          message = messageField.value || (messageField.values && messageField.values[0]) || '';
          console.log(`Message retrieved for contact ${ghlContactId}: ${message}`);
        } else {
          console.log(`No Message field found in customFields for contact ${ghlContactId}:`, customFields);
        }

        // Fetch source and urgency from custom fields if not provided in payload
        if (!fetchedSource) {
          const sourceField = customFields.find(field =>
            (field.name && field.name.toLowerCase() === 'source') ||
            (field.id && field.id === '1EkwGnC9UtMNrTCwGnBk')
          );
          if (sourceField && (sourceField.value || sourceField.values)) {
            fetchedSource = sourceField.value || (sourceField.values && sourceField.values[0]) || '';
            console.log(`Source retrieved for contact ${ghlContactId}: ${fetchedSource}`);
          }
        }

        if (!fetchedUrgency) {
          const urgencyField = customFields.find(field =>
            field.name && field.name.toLowerCase() === 'urgency'
          );
          if (urgencyField && (urgencyField.value || urgencyField.values)) {
            fetchedUrgency = urgencyField.value || (urgencyField.values && urgencyField.values[0]) || '';
            console.log(`Urgency retrieved for contact ${ghlContactId}: ${fetchedUrgency}`);
          }
        }
      } else {
        console.log(`No customFields available or accessible for contact ${ghlContactId}`);
      }
    } catch (error) {
      console.error('Failed to fetch contact message from GHL:', error.response?.data || error.message);
    }

    const jobDescriptionWithMessage = message
      ? `Enquiry details: ${message}\nSource: ${fetchedSource || 'Not specified'}\nUrgency: ${fetchedUrgency || 'Not specified'}\nGHL Contact ID: ${ghlContactId}\n${jobDescription || ''}`
      : `Source: ${fetchedSource || 'Not specified'}\nUrgency: ${fetchedUrgency || 'Not specified'}\nGHL Contact ID: ${ghlContactId}\n${jobDescription || ''}`;

    const jobData = {
      company_uuid: companyUuid,
      status: 'Quote',
      queue_uuid: queueUuid,
      job_address: address || 'No address provided',
      job_description: jobDescriptionWithMessage,
    };
    console.log('Creating ServiceM8 job with data:', JSON.stringify(jobData, null, 2));

    let jobUuid;
    try {
      const jobResponse = await serviceM8Api.post('/job.json', jobData);
      jobUuid = jobResponse.headers['x-record-uuid'];
      console.log(`Job created: ${jobUuid} in queue ${queueUuid}`);
    } catch (error) {
      console.error('Error creating ServiceM8 job:', error.response?.data || error.message);
      return res.status(500).json({ error: 'Failed to create job', details: error.response?.data || error.message });
    }

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
    try {
      await serviceM8Api.post('/jobcontact.json', jobContactData);
      console.log(`Job contact added for job: ${jobUuid}`);
    } catch (error) {
      console.error('Error adding job contact to ServiceM8:', error.response?.data || error.message);
      // Continue even if this fails, as it's non-critical
    }

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
    res.status(500).json({ error: 'Failed to create job', details: error.response?.data || error.message });
  }
});

// Endpoint to handle GHL webhook for new appointments
app.post('/ghl-appointment-sync', async (req, res) => {
  const processedAppointments = loadProcessedAppointments();
  const appointment = req.body;
  console.log('Received GHL appointment webhook payload:', JSON.stringify(appointment, null, 2));

  if (!appointment || !appointment.id || !appointment.contactId || !appointment.startTime) {
    console.error('Invalid webhook payload:', appointment);
    return res.status(400).json({ error: 'Missing appointment data' });
  }

  const appointmentId = appointment.id;
  if (processedAppointments.has(appointmentId)) {
    console.log(`Appointment ${appointmentId} already processed, skipping.`);
    return res.status(200).json({ message: 'Appointment already synced' });
  }

  try {
    // Log raw dates from GHL
    console.log(`Raw GHL startTime: ${appointment.startTime}, endTime: ${appointment.endTime}`);

    // Parse dates assuming they are in Brisbane time (AEST, UTC+10)
    const startTime = new Date(appointment.startTime);
    const endTime = new Date(appointment.endTime);

    // Validate dates
    if (isNaN(startTime.getTime()) || isNaN(endTime.getTime())) {
      console.error('Invalid date format for startTime or endTime:', appointment.startTime, appointment.endTime);
      return res.status(400).json({ error: 'Invalid date format' });
    }

    // Format dates for ServiceM8
    const formattedStartDate = formatDate(startTime);
    const formattedEndDate = formatDate(endTime);
    console.log(`Formatted dates for ServiceM8: start_date=${formattedStartDate}, end_date=${formattedEndDate}`);

    let contact;
    try {
      const contactResponse = await ghlApiV2.get(`/contacts/${appointment.contactId}`);
      contact = contactResponse.data.contact;
      console.log(`Fetched GHL contact: ${contact.id}, email: ${contact.email}, locationId: ${contact.locationId}`);
    } catch (error) {
      console.error(`Error fetching GHL contact ${appointment.contactId}:`, error.response?.data || error.message);
      return res.status(500).json({ error: 'Failed to fetch contact details' });
    }

    // Capitalize first letters of name
    const firstName = contact.firstName ? contact.firstName.charAt(0).toUpperCase() + contact.firstName.slice(1).toLowerCase() : '';
    const lastName = contact.lastName ? contact.lastName.charAt(0).toUpperCase() + contact.lastName.slice(1).toLowerCase() : '';
    const contactName = `${firstName} ${lastName}`.trim().toLowerCase();
    const contactEmail = (contact.email || '').toLowerCase().trim();

    let companyUuid;
    try {
      const companiesResponse = await serviceM8Api.get('/company.json');
      const companies = companiesResponse.data;
      const matchingCompany = companies.find(
        (c) =>
          (c.email ? c.email.toLowerCase().trim() : '') === contactEmail ||
          (c.name ? c.name.toLowerCase().trim() : '') === contactName
      );

      if (matchingCompany) {
        companyUuid = matchingCompany.uuid;
        console.log(`Found existing ServiceM8 company: ${companyUuid}`);
      } else {
        const newCompanyResponse = await serviceM8Api.post('/company.json', {
          name: `${firstName} ${lastName}`.trim() || contact.email || 'Unknown Contact',
          email: contact.email || '',
        });
        companyUuid = newCompanyResponse.headers['x-record-uuid'];
        console.log(`Created new ServiceM8 company: ${companyUuid}`);

        const companyContactData = {
          company_uuid: companyUuid,
          first: firstName,
          last: lastName,
          email: contact.email || '',
        };
        if (contact.phone || contact.mobile) {
          const phoneNumber = contact.phone || contact.mobile;
          assignPhoneFields(companyContactData, phoneNumber);
        }
        await serviceM8Api.post('/companycontact.json', companyContactData);
        console.log(`Created ServiceM8 contact for company: ${companyUuid}`);
      }
    } catch (error) {
      console.error('Error syncing ServiceM8 contact:', error.response?.data || error.message);
      return res.status(500).json({ error: 'Failed to sync contact' });
    }

    // Fetch source and urgency from appointment payload
    const fetchedSource = appointment.source || 'Not specified';
    const fetchedUrgency = appointment.urgency || 'Not specified';
    console.log(`Appointment source: ${fetchedSource}, urgency: ${fetchedUrgency}`);

    // Create a job in ServiceM8
    const jobData = {
      company_uuid: companyUuid,
      status: 'Work Order',
      queue_uuid: 'a67d1770-7a34-461a-91d9-23393bfa8d8b',
      job_address: appointment.location || contact.address1 || 'No address provided',
      job_description: `${appointment.title || 'GHL Appointment'}\nIssue: ${appointment.issue || 'Not specified'}\nSource: ${fetchedSource}\nUrgency: ${fetchedUrgency}`,
    };
    let jobUuid;
    try {
      console.log('Creating ServiceM8 job with data:', JSON.stringify(jobData, null, 2));
      const jobResponse = await serviceM8Api.post('/job.json', jobData);
      jobUuid = jobResponse.headers['x-record-uuid'];
      console.log(`Created ServiceM8 job: ${jobUuid}, status: ${jobData.status}`);
    } catch (error) {
      console.error('Error creating ServiceM8 job:', error.response?.data || error.message);
      return res.status(500).json({ error: 'Failed to create job', details: error.response?.data || error.message });
    }

    // Add job contact to ServiceM8
    const jobContactData = {
      job_uuid: jobUuid,
      type: 'Job Contact',
      first: firstName,
      last: lastName,
      email: contact.email || '',
    };
    if (contact.phone || contact.mobile) {
      const phoneNumber = contact.phone || contact.mobile;
      assignPhoneFields(jobContactData, phoneNumber);
    }
    try {
      await serviceM8Api.post('/jobcontact.json', jobContactData);
      console.log(`Added job contact to ServiceM8 job: ${jobUuid}`);
    } catch (error) {
      console.error('Error adding job contact to ServiceM8:', error.response?.data || error.message);
      // Continue even if this fails, as it's non-critical
    }

    // Hardcoded staff UUIDs
    const sebastianUuid = 'bdb4d3b3-3190-41ca-9e1b-2339396aa3eb';
    const tamsinUuid = '3899bd2d-65dc-416d-aa4d-21e1a7706aeb';

    // Function to check staff availability
    const isStaffAvailable = async (staffUuid) => {
      try {
        const filter = `$filter=staff_uuid eq '${staffUuid}'`;
        const activitiesResponse = await serviceM8Api.get(`/jobactivity.json?${filter}`);
        const activities = activitiesResponse.data;
        console.log(`Fetched ${activities.length} activities for staff ${staffUuid}`);

        const appointmentDate = new Date(startTime.getFullYear(), startTime.getMonth(), startTime.getDate());
        for (const activity of activities) {
          const activityStart = new Date(activity.start_date);
          const activityEnd = new Date(activity.end_date);
          const activityDate = new Date(activityStart.getFullYear(), activityStart.getMonth(), activityStart.getDate());
          if (activityDate.getTime() === appointmentDate.getTime()) {
            if (
              startTime >= activityStart && startTime < activityEnd ||
              endTime > activityStart && endTime <= activityEnd ||
              (startTime <= activityStart && endTime >= activityEnd)
            ) {
              console.log(`Overlap found for staff ${staffUuid}: ${activity.start_date} - ${activity.end_date}`);
              return false; // Overlap found, staff is unavailable
            }
          }
        }
        return true; // No overlaps on the appointment date, staff is available
      } catch (error) {
        console.error(`Error checking availability for staff ${staffUuid}:`, error.response?.data || error.message);
        return false; // Assume unavailable on error
      }
    };

    // Check availability
    let selectedStaffUuid = null;
    if (await isStaffAvailable(sebastianUuid)) {
      selectedStaffUuid = sebastianUuid;
      console.log('Sebastian is available, booking with him.');
    } else if (await isStaffAvailable(tamsinUuid)) {
      selectedStaffUuid = tamsinUuid;
      console.log('Sebastian is not available, but Tamsin is, booking with her.');
    } else {
      console.log('Neither Sebastian nor Tamsin is available.');
      return res.status(200).json({ message: 'No available slot in ServiceM8' });
    }

    // Create job activity with selected staff
    const activityData = {
      job_uuid: jobUuid,
      staff_uuid: selectedStaffUuid,
      start_date: formattedStartDate,
      end_date: formattedEndDate,
      activity_description: `${appointment.title || 'GHL Appointment'}\nIssue: ${appointment.issue || 'Not specified'}\nSource: ${fetchedSource}\nUrgency: ${fetchedUrgency}`,
      activity_type: 'Appointment',
      job_address: appointment.location || contact.address1 || 'No address provided',
      related_contact_uuid: null,
      activity_was_scheduled: true,
      active: 1,
    };

    try {
      console.log('Creating ServiceM8 job activity with data:', JSON.stringify(activityData, null, 2));
      const response = await serviceM8Api.post('/jobactivity.json', activityData);
      const activityUuid = response.headers['x-record-uuid'];
      console.log(`Created ServiceM8 job activity: ${activityUuid} for appointment ${appointmentId}, staff: ${selectedStaffUuid}`);
      console.log(`Activity details: start_date=${activityData.start_date}, end_date=${activityData.end_date}, job_uuid=${jobUuid}, scheduled=${activityData.activity_was_scheduled}, active=${activityData.active}`);

      processedAppointments.add(appointmentId);
      saveProcessedAppointments(processedAppointments);

      res.status(200).json({ message: 'Appointment synced', jobUuid, activityUuid });
    } catch (error) {
      console.error('Error creating ServiceM8 job activity:', error.response?.data || error.message);
      return res.status(500).json({ error: 'Failed to sync appointment', details: error.response?.data || error.message });
    }
  } catch (error) {
    console.error('Webhook error:', error.response?.data || error.message);
    return res.status(500).json({ error: 'Failed to process webhook', details: error.response?.data || error.message });
  }
});

// Temporary endpoints for testing
app.get('/test-job-completion', async (req, res) => {
  console.log('Triggering test job completion check...');
  await checkCompletedJobs();
  res.send('Job completion check triggered');
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
cron.schedule('0 0 * * *', () => {
  console.log('Scheduled polling for new contacts...');
  checkNewContacts();
});

cron.schedule('0 0 * * *', () => {
  console.log('Scheduled polling for job completion...');
  checkCompletedJobs();
});

cron.schedule('0 */12 * * *', () => {
  console.log('Scheduled GHL token refresh...');
  refreshGHLTokens();
});

app.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
  console.log(`👉 Visit http://localhost:${PORT}/auth to start OAuth flow`);
});