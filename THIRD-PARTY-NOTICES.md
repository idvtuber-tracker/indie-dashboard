# Third-Party Notices

This file lists all third-party software, fonts, and data used by the
**idvtuber-tracker/tracker** and **idvtuber-tracker/dashboard** repositories,
together with their licenses and copyright holders.

---

## Python packages

Installed via `pip` / `requirements.txt`. These packages are **not bundled**
in this repository; they are downloaded at install time. License obligations
apply if you redistribute them.

| Package | Version | License | Copyright holder |
|---|---|---|---|
| `google-api-python-client` | 2.131.0 | Apache 2.0 | Google LLC |
| `psycopg2-binary` | 2.9.9 | LGPL 3.0 + OpenSSL exception | Daniele Varrazzo and contributors |
| `requests` | 2.32.3 | Apache 2.0 | Kenneth Reitz and contributors |
| `tzdata` | 2025.3 | Apache 2.0 (package) / Public Domain (IANA timezone data) | PSF (package); IANA (timezone data) |

### Notes on psycopg2-binary (LGPL 3.0)

`psycopg2-binary` is licensed under the GNU Lesser General Public License
v3.0, with an OpenSSL exception. LGPL requires that if you distribute the
project as a **compiled binary** that statically links psycopg2, users must
be able to relink against a modified version of the library. Because this
project is distributed as Python source and users install psycopg2 via pip,
this condition is automatically satisfied and no additional action is required.

Full license text: <https://www.gnu.org/licenses/lgpl-3.0.txt>

### Notes on Apache 2.0 packages

Apache 2.0 requires that you preserve copyright notices and NOTICE files when
**redistributing** the packages themselves. Because this project does not
redistribute the packages — users install them via pip — no additional action
is required.

Full license text: <https://www.apache.org/licenses/LICENSE-2.0>

---

## CDN-loaded JavaScript libraries

These libraries are **not bundled** in this repository. They are loaded at
runtime from a CDN in the generated HTML. MIT license requires preserving
the copyright notice if you bundle them; CDN usage requires no additional
action.

| Library | Version | License | Copyright holder | CDN |
|---|---|---|---|---|
| Chart.js | 4.4.0 | MIT | Chart.js contributors | jsDelivr |
| chartjs-plugin-zoom | 2.0.1 | MIT | Štěpán Roh and contributors | cdnjs |
| Hammer.js | 2.0.8 | MIT | Jorik Tangelder and contributors | cdnjs |

MIT License (Chart.js, chartjs-plugin-zoom, Hammer.js)

> Permission is hereby granted, free of charge, to any person obtaining a
> copy of this software and associated documentation files (the "Software"),
> to deal in the Software without restriction, including without limitation
> the rights to use, copy, modify, merge, publish, distribute, sublicense,
> and/or sell copies of the Software, and to permit persons to whom the
> Software is furnished to do so, subject to the following conditions:
> The above copyright notice and this permission notice shall be included in
> all copies or substantial portions of the Software.
> THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
> IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
> FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
> AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
> LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
> FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
> DEALINGS IN THE SOFTWARE.

---

## Web fonts

Loaded via Google Fonts CDN. Not bundled in this repository. Licensed under
the SIL Open Font License 1.1, which permits free use in any project
(commercial or non-commercial) but prohibits selling the fonts on their own.

| Font | License | Copyright holder |
|---|---|---|
| DM Mono | SIL Open Font License 1.1 | Colophon Foundry |
| Fraunces | SIL Open Font License 1.1 | Undercase Type (Phaedra Charles, Flavia Zimbardi) |

Full license text: <https://openfontlicense.org/open-font-license-official-text/>

---

## GitHub Actions

Used in CI/CD workflows. Licensed under the MIT License by GitHub, Inc.

| Action | Version |
|---|---|
| `actions/checkout` | v4 |
| `actions/setup-python` | v5 |
| `actions/upload-pages-artifact` | v3 |
| `actions/deploy-pages` | v4 |

---

## External data and media

### YouTube Data API v3

Data retrieved via the YouTube Data API v3 is used under the
[YouTube API Terms of Service](https://developers.google.com/youtube/terms/api-services-terms-of-service).
This project is non-commercial and uses the data solely as an analytics
reference.

### Google Analytics

This site uses Google Analytics (GA4) to collect aggregate, non-identifying
traffic statistics. Google Analytics sets first-party cookies. See Google's
Privacy Policy for details on how Google processes this data:
<https://policies.google.com/privacy>

### Channel avatars and stream thumbnails

Channel avatar images and stream thumbnails are served directly from
YouTube's CDN (`yt3.ggpht.com`, `i.ytimg.com`) and are the property of
their respective channel owners. No ownership or license over this media is
claimed by this project.
