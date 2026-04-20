const TIME_API_URL_PATTERN = "https://time.cu.ru/api/v4/teams/*";

chrome.webRequest.onBeforeRequest.addListener(
  (details) => {
    const match = details.url.match(/\/api\/v4\/teams\/([^/]+)\/channels/i);
    if (!match) {
      return;
    }

    chrome.storage.local.set({ timeTeamId: match[1] });
  },
  { urls: [TIME_API_URL_PATTERN] }
);
