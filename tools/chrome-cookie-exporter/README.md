# Chrome Cookie Exporter

Minimal Chrome extension for copying the auth values this project needs.

## What it copies

1. `cookie.txt`: combined Yandex cookies for Calendar access
2. `.env` lines: `TIME_TEAM_ID`, `TIME_COOKIE`, and `TIME_CSRF`
3. `ktalk_auth.txt`: the full KTalk `Cookie` header value

## Load in Chrome

1. Open `chrome://extensions`
2. Enable Developer mode
3. Click Load unpacked
4. Select `tools/chrome-cookie-exporter`

## Use

1. Sign in to `calendar.yandex.ru`, `time.cu.ru`, and `centraluniversity.ktalk.ru`
2. Open the extension popup
3. For Time, open a space/channel page so the app sends a `/api/v4/teams/.../channels/...` request
4. Click the button you need
5. Paste the copied value into the matching local file or `.env`

## Notes

- The extension only reads cookies from the three target services.
- Time team id is taken from observed `time.cu.ru/api/v4/teams/.../channels/...` request URLs.
- KTalk auth is copied from browser cookies and written as a single `Cookie` header string.
- If Time CSRF is missing, refresh `time.cu.ru` after login and try again.
