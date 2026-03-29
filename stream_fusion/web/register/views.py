from fastapi import APIRouter, Depends
from fastapi.responses import HTMLResponse, JSONResponse

from stream_fusion.services.postgresql.dao.apikey_dao import APIKeyDAO
from stream_fusion.services.postgresql.schemas.apikey_schemas import APIKeyCreate
from stream_fusion.logging_config import logger
from stream_fusion.settings import settings

router = APIRouter()

# ---------------------------------------------------------------------------
# HTML templates – self-contained, no external template files required.
# ---------------------------------------------------------------------------

_DISABLED_HTML = """\
<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Stream-Fusion — Inscription désactivée</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&family=Sora:wght@400;600;700&display=swap" rel="stylesheet">
<style>
  :root {
    --accent: #6366f1; --accent-light: #818cf8; --accent-glow: rgba(99,102,241,0.3);
    --orange: #f97316; --cyan: #06b6d4;
    --surface: rgba(12,12,22,0.82); --border: rgba(255,255,255,0.07);
    --text: #f1f5f9; --text-dim: #94a3b8; --text-muted: #64748b; --warning: #facc15;
  }
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Outfit', sans-serif; background: #050510; color: var(--text); min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 2rem; }
  .bg-scene { position: fixed; inset: 0; z-index: 0; background-image: linear-gradient(175deg, rgba(5,5,16,0.88) 0%, rgba(5,5,16,0.75) 50%, rgba(5,5,16,0.92) 100%), url('https://images.wallpapersden.com/image/download/4k-artistic-landscape_bWpoZWiUmZqaraWkpJRobWllrWdma2U.jpg'); background-size: cover; background-position: center; }
  .bg-orbs { position: fixed; inset: 0; z-index: 0; pointer-events: none; background: radial-gradient(ellipse 500px 400px at 15% 15%, rgba(99,102,241,0.08), transparent), radial-gradient(ellipse 400px 350px at 85% 85%, rgba(249,115,22,0.06), transparent), radial-gradient(ellipse 300px 300px at 60% 40%, rgba(6,182,212,0.04), transparent); }
  .glass-form { position: relative; z-index: 1; background: var(--surface); border: 1px solid var(--border); border-radius: 24px; box-shadow: 0 40px 120px rgba(0,0,0,0.5), inset 0 1px 0 rgba(255,255,255,0.05); padding: 3rem 2.5rem; width: 100%; max-width: 460px; text-align: center; }
  .logo-area { margin-bottom: 1.5rem; }
  .logo-area img { width: 100px; height: 100px; filter: drop-shadow(0 0 30px rgba(99,102,241,0.25)); margin-bottom: 0.75rem; }
  .logo-area h1 { font-family: 'Sora', sans-serif; font-size: 1.6rem; font-weight: 700; background: linear-gradient(135deg, #e2e8f0, #818cf8, #06b6d4); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin-bottom: 0.25rem; }
  .logo-area p { color: var(--text-dim); font-size: 0.88rem; font-weight: 400; }
  .notice { padding: 0.85rem 1.2rem; border-radius: 12px; font-size: 0.92rem; font-weight: 500; background: linear-gradient(135deg, rgba(234,179,8,0.1), rgba(249,115,22,0.08)); border: 1px solid rgba(234,179,8,0.25); color: var(--warning); }
  @keyframes fadeUp { from { opacity: 0; transform: translateY(28px); } to { opacity: 1; transform: translateY(0); } }
  .glass-form { animation: fadeUp 0.65s ease-out both; }
</style>
</head>
<body>
<div class="bg-scene"></div>
<div class="bg-orbs"></div>
<div class="glass-form">
  <div class="logo-area">
    <img src="https://i.ibb.co/ZRZcVBLd/SF-modern.png" alt="Stream-Fusion">
    <h1>Stream-Fusion</h1>
    <p>Clé API personnelle</p>
  </div>
  <div class="notice">Les inscriptions sont fermées pour le moment.</div>
</div>
</body>
</html>
"""

_REGISTER_HTML = """\
<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Stream-Fusion — Obtenir une clé API</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500&family=Outfit:wght@300;400;500;600;700&family=Sora:wght@400;600;700&display=swap" rel="stylesheet">
<style>
  :root {
    --accent: #6366f1; --accent-light: #818cf8; --accent-dark: #4f46e5;
    --accent-glow: rgba(99,102,241,0.3); --orange: #f97316; --cyan: #06b6d4;
    --green: #22c55e; --red: #ef4444;
    --surface: rgba(12,12,22,0.82); --border: rgba(255,255,255,0.07);
    --border-hover: rgba(255,255,255,0.14);
    --text: #f1f5f9; --text-dim: #94a3b8; --text-muted: #64748b; --warning: #facc15;
  }
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Outfit', sans-serif; background: #050510; color: var(--text); min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 2rem; }
  .bg-scene { position: fixed; inset: 0; z-index: 0; background-image: linear-gradient(175deg, rgba(5,5,16,0.88) 0%, rgba(5,5,16,0.75) 50%, rgba(5,5,16,0.92) 100%), url('https://images.wallpapersden.com/image/download/4k-artistic-landscape_bWpoZWiUmZqaraWkpJRobWllrWdma2U.jpg'); background-size: cover; background-position: center; }
  .bg-orbs { position: fixed; inset: 0; z-index: 0; pointer-events: none; background: radial-gradient(ellipse 500px 400px at 15% 15%, rgba(99,102,241,0.08), transparent), radial-gradient(ellipse 400px 350px at 85% 85%, rgba(249,115,22,0.06), transparent), radial-gradient(ellipse 300px 300px at 60% 40%, rgba(6,182,212,0.04), transparent); }
  .glass-form { position: relative; z-index: 1; background: var(--surface); border: 1px solid var(--border); border-radius: 24px; box-shadow: 0 40px 120px rgba(0,0,0,0.5), inset 0 1px 0 rgba(255,255,255,0.05); padding: 3rem 2.5rem; width: 100%; max-width: 460px; text-align: center; }
  .logo-area { margin-bottom: 1.5rem; }
  .logo-area img { width: 100px; height: 100px; filter: drop-shadow(0 0 30px rgba(99,102,241,0.25)); margin-bottom: 0.75rem; }
  .logo-area h1 { font-family: 'Sora', sans-serif; font-size: 1.6rem; font-weight: 700; background: linear-gradient(135deg, #e2e8f0, #818cf8, #06b6d4); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin-bottom: 0.25rem; }
  .logo-area p { color: var(--text-dim); font-size: 0.88rem; font-weight: 400; }
  .btn { display: inline-flex; align-items: center; justify-content: center; gap: 8px; width: 100%; padding: 14px 32px; border-radius: 14px; font-family: 'Sora', sans-serif; font-weight: 600; font-size: 0.92rem; border: none; cursor: pointer; transition: all 0.35s; color: white; position: relative; overflow: hidden; }
  .btn::before { content: ''; position: absolute; inset: 0; background: linear-gradient(135deg, rgba(255,255,255,0.1), transparent); opacity: 0; transition: opacity 0.3s; }
  .btn:hover:not(:disabled)::before { opacity: 1; }
  .btn-primary { background: linear-gradient(135deg, var(--accent), #7c3aed); box-shadow: 0 4px 24px var(--accent-glow), 0 0 0 1px rgba(99,102,241,0.3); }
  .btn-primary:hover:not(:disabled) { transform: translateY(-3px); box-shadow: 0 8px 32px rgba(99,102,241,0.5), 0 0 0 1px rgba(99,102,241,0.5); }
  .btn-primary:active:not(:disabled) { transform: translateY(-1px); }
  .btn:disabled { opacity: 0.45; cursor: not-allowed; transform: none !important; }
  .message { margin-top: 1.25rem; padding: 0.85rem 1rem; border-radius: 12px; font-size: 0.88rem; line-height: 1.5; display: none; text-align: left; }
  .message.error { display: block; background: rgba(239,68,68,0.08); border: 1px solid rgba(239,68,68,0.2); color: #fca5a5; }
  .message.success { display: block; background: rgba(34,197,94,0.08); border: 1px solid rgba(34,197,94,0.2); color: #86efac; }
  .message.warning { display: block; background: linear-gradient(135deg, rgba(234,179,8,0.1), rgba(249,115,22,0.08)); border: 1px solid rgba(234,179,8,0.25); color: var(--warning); }
  .key-display { margin-top: 1.25rem; display: none; text-align: left; }
  .key-display.visible { display: block; }
  .key-display label { display: block; font-size: 0.75rem; font-weight: 600; color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 0.5rem; }
  .key-box { display: flex; align-items: stretch; border-radius: 11px; overflow: hidden; background: rgba(0,0,0,0.3); border: 1px solid var(--border); }
  .key-value { flex: 1; font-family: 'JetBrains Mono', monospace; font-size: 0.84rem; letter-spacing: -0.02em; padding: 0.75rem 1rem; background: transparent; color: var(--accent-light); word-break: break-all; user-select: all; }
  .btn-copy { padding: 0.75rem 1rem; background: rgba(255,255,255,0.06); border: none; border-left: 1px solid var(--border); color: var(--text-dim); cursor: pointer; font-family: 'Outfit', sans-serif; font-size: 0.8rem; font-weight: 500; transition: background 0.2s, color 0.2s; white-space: nowrap; }
  .btn-copy:hover { background: rgba(255,255,255,0.1); color: var(--text); }
  .hint { margin-top: 0.6rem; font-size: 0.73rem; color: var(--text-muted); font-weight: 400; }
  .spinner { display: inline-block; width: 1em; height: 1em; border: 2px solid transparent; border-top-color: white; border-radius: 50%; animation: spin 0.6s linear infinite; vertical-align: middle; margin-right: 0.4em; }
  @keyframes spin { to { transform: rotate(360deg); } }
  @keyframes fadeUp { from { opacity: 0; transform: translateY(28px); } to { opacity: 1; transform: translateY(0); } }
  .glass-form { animation: fadeUp 0.65s ease-out both; }
</style>
</head>
<body>
<div class="bg-scene"></div>
<div class="bg-orbs"></div>
<div class="glass-form">
  <div class="logo-area">
    <img src="https://i.ibb.co/ZRZcVBLd/SF-modern.png" alt="Stream-Fusion">
    <h1>Stream-Fusion</h1>
    <p>Créez votre clé API personnelle</p>
  </div>

  <button class="btn btn-primary" id="submitBtn" onclick="generate()">Générer une clé API</button>

  <div id="msg" class="message"></div>

  <div id="keyDisplay" class="key-display">
    <label>Votre clé API</label>
    <div class="key-box">
      <div class="key-value" id="keyValue"></div>
      <button class="btn-copy" id="copyBtn" onclick="copyKey()">Copier</button>
    </div>
    <p class="hint">Sauvegardez cette clé maintenant. Elle ne sera plus affichée.</p>
  </div>
</div>

<script>
const msg = document.getElementById('msg');
const keyDisplay = document.getElementById('keyDisplay');
const keyValue = document.getElementById('keyValue');
const submitBtn = document.getElementById('submitBtn');

async function generate() {
  msg.className = 'message';
  msg.style.display = '';
  keyDisplay.classList.remove('visible');
  submitBtn.disabled = true;
  submitBtn.innerHTML = '<span class="spinner"></span>Génération\\u2026';

  try {
    const res = await fetch(window.location.pathname, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });
    const data = await res.json();

    if (!res.ok) {
      const detail = data.detail || 'Une erreur est survenue';
      msg.textContent = detail;
      msg.className = 'message error';
      submitBtn.disabled = false;
      submitBtn.textContent = 'Générer une clé API';
      return;
    }

    msg.textContent = 'Clé API créée avec succès !';
    msg.className = 'message success';
    keyValue.textContent = data.api_key;
    keyDisplay.classList.add('visible');
    submitBtn.disabled = false;
    submitBtn.textContent = 'Générer une clé API';
  } catch (err) {
    msg.textContent = 'Erreur réseau. Veuillez réessayer.';
    msg.className = 'message error';
    submitBtn.disabled = false;
    submitBtn.textContent = 'Générer une clé API';
  }
}

function copyKey() {
  const key = keyValue.textContent;
  navigator.clipboard.writeText(key).then(() => {
    const btn = document.getElementById('copyBtn');
    btn.textContent = 'Copié !';
    setTimeout(() => { btn.textContent = 'Copier'; }, 2000);
  });
}
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/register", response_class=HTMLResponse, include_in_schema=False)
async def register_page():
    """Serve the public registration page.

    Returns the enabled form when ``allow_public_key_registration`` is
    ``True``, otherwise a notice that registrations are closed.
    """
    if settings.allow_public_key_registration:
        return HTMLResponse(content=_REGISTER_HTML)
    return HTMLResponse(content=_DISABLED_HTML)


@router.post("/register", include_in_schema=False)
async def register_create_key(apikey_dao: APIKeyDAO = Depends()):
    """Create a new API key via the public registration page.

    Guarded by the ``allow_public_key_registration`` setting – returns a
    ``403`` when registrations are disabled.
    """
    if not settings.allow_public_key_registration:
        return JSONResponse(
            status_code=403,
            content={"detail": "Les inscriptions sont fermées pour le moment."},
        )

    logger.info("Public registration: creating new API key")
    try:
        api_key = await apikey_dao.create_key(
            APIKeyCreate(name="public-registration", never_expire=True)
        )
        logger.info("Public registration: API key created successfully")
        return JSONResponse(content={"api_key": str(api_key.api_key)})
    except Exception as e:
        logger.error(f"Public registration: error creating API key – {e}")
        return JSONResponse(
            status_code=500,
            content={"detail": "Impossible de créer la clé API. Veuillez réessayer."},
        )
