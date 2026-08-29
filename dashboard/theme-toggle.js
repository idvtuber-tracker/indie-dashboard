/* theme-toggle.js — shared dark/light toggle behavior.
   Mirrors _THEME_JS in dashboard_core_private.py. Kept as its own file (not
   inlined) so standalone pages like privacy.html/terms.html don't need to
   duplicate this logic — link to it instead. */
(function () {
  var STORAGE_KEY = 'idvt-theme';
  var root  = document.documentElement;
  var btn   = null;
  var thumb = null;

  function getSystemTheme() {
    return window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
  }
  function getEffectiveTheme() {
    return localStorage.getItem(STORAGE_KEY) || getSystemTheme();
  }
  function applyTheme(theme) {
    root.setAttribute('data-theme', theme);
    if (thumb) {
      theme === 'light' ? thumb.classList.add('is-light') : thumb.classList.remove('is-light');
    }
    window.dispatchEvent(new Event('idvt-theme-change'));
  }
  function toggleTheme() {
    var next = getEffectiveTheme() === 'dark' ? 'light' : 'dark';
    localStorage.setItem(STORAGE_KEY, next);
    applyTheme(next);
  }
  applyTheme(getEffectiveTheme());
  document.addEventListener('DOMContentLoaded', function () {
    btn   = document.getElementById('theme-toggle');
    thumb = document.getElementById('toggle-thumb');
    applyTheme(getEffectiveTheme());
    if (btn) btn.addEventListener('click', toggleTheme);
  });
  window.matchMedia('(prefers-color-scheme: light)').addEventListener('change', function () {
    if (!localStorage.getItem(STORAGE_KEY)) applyTheme(getSystemTheme());
  });
})();
