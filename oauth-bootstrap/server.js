const express = require("express");
const dotenv = require("dotenv");

dotenv.config();

const app = express();

const {
  CLIENT_ID,
  CLIENT_SECRET,
  AUTH_URL,
  TOKEN_URL,
  REDIRECT_URI,
  SCOPE,
} = process.env;

// Step 1: endpoint bantu buat login URL
app.get("/login", (req, res) => {
  const url =
    `${AUTH_URL}?response_type=code` +
    `&client_id=${encodeURIComponent(CLIENT_ID)}` +
    `&redirect_uri=${encodeURIComponent(REDIRECT_URI)}` +
    `&scope=${encodeURIComponent(SCOPE)}`;

  res.send(`
    <h2>OAuth Login</h2>
    <a href="${url}">Login via OAuth Provider</a>
  `);
});

// Step 2: callback dari OAuth provider
app.get("/callback", async (req, res) => {
  const { code, error } = req.query;

  if (error) {
    return res.status(400).send(`OAuth Error: ${error}`);
  }

  if (!code) {
    return res.status(400).send("No authorization code received");
  }

  const params = new URLSearchParams({
    grant_type: 'authorization_code',
    code: code,
    redirect_uri: REDIRECT_URI
  });

  const credentials = Buffer.from(`${CLIENT_ID}:${CLIENT_SECRET}`).toString('base64');


  try {
    const tokenRes = await fetch(TOKEN_URL, {
      method: "POST",
      headers: { 
        "Content-Type": "application/x-www-form-urlencoded" 
        , "Authorization": `Basic ${credentials}`
      },
      body: params.toString(),
    });


    const token = await tokenRes.json();

    console.log("====================================");
    console.log("ACCESS TOKEN:", token.access_token);
    console.log("REFRESH TOKEN:", token.refresh_token);
    console.log("EXPIRES IN:", token.expires_in);
    console.log("====================================");

    res.send(`
      <h2>OAuth Success ✅</h2>
      <p>Refresh token sudah diterima.</p>
      <p><strong>SILAKAN TUTUP HALAMAN INI.</strong></p>
      <p>Cek terminal untuk refresh_token.</p>
    `);
  } catch (err) {
    console.error(err);
    res.status(500).send("Failed to exchange token");
  }
});

app.listen(3000, () => {
  console.log("OAuth bootstrap server running at http://localhost:3000");
  console.log("Open http://localhost:3000/login to start OAuth login");
});
