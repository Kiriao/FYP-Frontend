const fs = require('fs');

// Read your service account key
const serviceAccount = require('./serviceAccountKey.json');

// Convert to base64
const base64Key = Buffer.from(serviceAccount.private_key).toString('base64');

console.log('=== Copy this value to Vercel as FIREBASE_PRIVATE_KEY ===');
console.log(base64Key);
console.log('==========================================================');