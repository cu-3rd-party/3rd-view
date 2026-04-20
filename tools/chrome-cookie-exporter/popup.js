const statusEl = document.getElementById("status");

const YANDEX_URLS = [
  "https://calendar.yandex.ru/",
  "https://yandex.ru/",
  "https://passport.yandex.ru/"
];

const TIME_URL = "https://time.cu.ru/";
const KTALK_URL = "https://centraluniversity.ktalk.ru/";

function setStatus(message) {
  statusEl.textContent = message;
}

function getAllCookies(details) {
  return new Promise((resolve, reject) => {
    chrome.cookies.getAll(details, (cookies) => {
      if (chrome.runtime.lastError) {
        reject(new Error(chrome.runtime.lastError.message));
        return;
      }
      resolve(cookies);
    });
  });
}

function getCookie(details) {
  return new Promise((resolve, reject) => {
    chrome.cookies.get(details, (cookie) => {
      if (chrome.runtime.lastError) {
        reject(new Error(chrome.runtime.lastError.message));
        return;
      }
      resolve(cookie);
    });
  });
}

async function copyText(text) {
  await navigator.clipboard.writeText(text);
}

function getStorageValue(key) {
  return new Promise((resolve, reject) => {
    chrome.storage.local.get([key], (result) => {
      if (chrome.runtime.lastError) {
        reject(new Error(chrome.runtime.lastError.message));
        return;
      }
      resolve(result[key] || null);
    });
  });
}

function joinCookieString(cookies) {
  return cookies.map((cookie) => `${cookie.name}=${cookie.value}`).join("; ");
}

async function loadYandexCookies() {
  const cookieMap = new Map();
  for (const url of YANDEX_URLS) {
    const cookies = await getAllCookies({ url });
    for (const cookie of cookies) {
      cookieMap.set(cookie.name, cookie);
    }
  }

  const cookies = [...cookieMap.values()].sort((left, right) => left.name.localeCompare(right.name));
  if (!cookies.length) {
    throw new Error("No Yandex cookies found. Open Yandex Calendar and sign in first.");
  }

  return joinCookieString(cookies);
}

function pickCsrfToken(cookies) {
  const exactNames = ["csrftoken", "csrf", "x-csrf-token", "XSRF-TOKEN", "xsrf-token"];
  for (const name of exactNames) {
    const match = cookies.find((cookie) => cookie.name === name);
    if (match) {
      return match.value;
    }
  }

  const fuzzyMatch = cookies.find((cookie) => /csrf|xsrf/i.test(cookie.name));
  return fuzzyMatch ? fuzzyMatch.value : null;
}

async function loadTimeEnvLines() {
  const cookies = await getAllCookies({ url: TIME_URL });
  if (!cookies.length) {
    throw new Error("No Time cookies found. Open time.cu.ru and sign in first.");
  }

  const teamId = await getStorageValue("timeTeamId");
  if (!teamId) {
    throw new Error(
      "No Time team id captured yet. Open a space in time.cu.ru so it sends a /api/v4/teams/.../channels request."
    );
  }

  const csrfValue = pickCsrfToken(cookies);
  if (!csrfValue) {
    throw new Error("Could not find a Time CSRF cookie. Open the app and refresh once.");
  }

  return `TIME_TEAM_ID=${teamId}\nTIME_COOKIE=${joinCookieString(cookies)}\nTIME_CSRF=${csrfValue}`;
}

async function loadKTalkCookies() {
  const cookies = await getAllCookies({ url: KTALK_URL });
  if (!cookies.length) {
    throw new Error("No KTalk cookies found. Open centraluniversity.ktalk.ru and sign in first.");
  }

  return joinCookieString(cookies);
}

async function runCopy(loader, successMessage) {
  setStatus("Reading browser cookies...");
  try {
    const text = await loader();
    await copyText(text);
    setStatus(`${successMessage}\n\n${text}`);
  } catch (error) {
    setStatus(error instanceof Error ? error.message : String(error));
  }
}

document.getElementById("copy-yandex").addEventListener("click", () => {
  runCopy(loadYandexCookies, "Copied Yandex cookie string for cookie.txt.");
});

document.getElementById("copy-time").addEventListener("click", () => {
  runCopy(loadTimeEnvLines, "Copied TIME_COOKIE and TIME_CSRF lines.");
});

document.getElementById("copy-ktalk").addEventListener("click", () => {
  runCopy(loadKTalkCookies, "Copied KTalk Cookie header for ktalk_auth.txt.");
});
