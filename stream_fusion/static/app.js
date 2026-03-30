// JAVASCRIPT POUR PAGE PRINCIPALE
// app.js

// ═══ CARTES ═══
function toggleCard(headerEl) {
    var card = headerEl.closest('.card');
    var children = card.querySelectorAll(':scope > :not(.card-header)');
    var isCollapsing = !card.classList.contains('collapsed');

    if (isCollapsing) {
        // Set explicit height from current size so transition has a real start value
        children.forEach(function(el) {
            el.style.maxHeight = el.scrollHeight + 'px';
            el.offsetHeight; // force reflow
        });
        card.classList.add('collapsed');
        children.forEach(function(el) {
            el.style.maxHeight = '0';
        });
    } else {
        card.classList.remove('collapsed');
        children.forEach(function(el) {
            el.style.maxHeight = el.scrollHeight + 'px';
        });
        // After transition ends, remove inline max-height so content can reflow naturally
        var onDone = function() {
            children.forEach(function(el) {
                el.style.maxHeight = '';
            });
            children[0] && children[0].removeEventListener('transitionend', onDone);
        };
        if (children[0]) children[0].addEventListener('transitionend', onDone, { once: true });
    }
}

// ═══ VALIDATION CLÉ API EN LIVE ═══
function validateKeyLive(val) {
    var status = document.getElementById('apiKeyStatus');
    var msg = document.getElementById('apiKeyMsg');
    var msgText = document.getElementById('apiKeyMsgText');
    var errDiv = document.getElementById('apiKeyError');

    if (!val) {
        status.className = 'api-key-status';
        msg.className = 'api-key-msg';
        return;
    }

    if (val !== val.trim() || val.indexOf(' ') !== -1) {
        status.className = 'api-key-status invalid';
        msg.className = 'api-key-msg show invalid';
        msgText.textContent = 'Format invalide — une clé UUID v4 est requise';
        return;
    }

    var isValid = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(val);

    if (isValid) {
        status.className = 'api-key-status';
        msg.className = 'api-key-msg';
        if (errDiv) errDiv.classList.add('hidden');
    } else {
        status.className = 'api-key-status invalid';
        msg.className = 'api-key-msg show invalid';
        msgText.textContent = 'Format invalide — une clé UUID v4 est requise';
    }
}

function togglePw(btn) {
    var input = btn.parentElement.querySelector('input');
    if (input.type === 'password') {
        input.type = 'text';
        btn.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9.88 9.88a3 3 0 1 0 4.24 4.24"/><path d="M10.73 5.08A10.43 10.43 0 0 1 12 5c7 0 10 7 10 7a13.16 13.16 0 0 1-1.67 2.68"/><path d="M6.61 6.61A13.526 13.526 0 0 0 2 12s3 7 10 7a9.74 9.74 0 0 0 5.39-1.61"/><line x1="2" y1="2" x2="22" y2="22"/></svg>';
    } else {
        input.type = 'password';
        btn.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M2 12s3-7 10-7 10 7 10 7-3 7-10 7-10-7-10-7Z"/><circle cx="12" cy="12" r="3"/></svg>';
    }
}

// ═══ PLUS DE TOASTS ═══
var _iconSuccess = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 6 9 17l-5-5"/></svg>';
var _iconError = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>';
var _iconWarning = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>';

function showToast(msg, type, duration) {
    var t = document.getElementById('toast');
    var m = document.getElementById('toastMsg');
    var icon = document.getElementById('toastIcon');
    m.textContent = msg || 'Succès !';
    t.className = 'toast';
    if (type === 'error') {
        t.classList.add('error');
        icon.innerHTML = _iconError;
    } else if (type === 'warning') {
        t.classList.add('warning');
        icon.innerHTML = _iconWarning;
    } else {
        icon.innerHTML = _iconSuccess;
    }
    t.classList.add('show');
    clearTimeout(t._tid);
    t._tid = setTimeout(function() { t.classList.remove('show'); }, duration || 3500);
}

// ═══ GENERATEUR MANIFEST ═══
var _manifestUrl = '';

async function generateManifest() {
    if (typeof window.clearValidationErrors === 'function') window.clearValidationErrors();
    document.getElementById('manifestResult').classList.remove('show');
    _manifestUrl = '';

    // getLink est async — on l'appelle avec 'none' pour obtenir l'URL sans side effect
    var manifestUrl = await getLink('none');
    if (!manifestUrl) return;
    _manifestUrl = manifestUrl;

    // TEST MANIFEST - VÉRIFICATION CLÉ API
    var btn = document.getElementById('generateBtn');
    var btnOrigHTML = btn.innerHTML;
    btn.classList.add('loading');
    btn.innerHTML = '<div class="btn-spinner"></div> Vérification…';

    fetch(_manifestUrl, { method: 'GET' })
        .then(function(resp) {
            btn.classList.remove('loading');
            btn.innerHTML = btnOrigHTML;

            if (resp.status === 403) {
                // Pas bon
                showToast('Clé API rejetée par le serveur', 'error', 4500);
                if (typeof window.clearValidationErrors === 'function') {
                    window.clearValidationErrors();
                    window._errorSteps.add(0);
                    var dot = window._wizDots[0];
                    if (dot) { dot.classList.add('error'); dot.innerHTML = window._dotIcons[0]; }
                    window.showFieldError('ApiKey', 'Clé API invalide — le serveur a retourné une erreur 403', 0);
                    window.goToStep(0);
                }
                return;
            }

            if (!resp.ok && resp.status !== 200) {
                showToast('Manifest généré (le serveur n\'a pas pu être vérifié)', 'warning', 4000);
            }

            // SUCCÈS
            document.getElementById('manifestUrl').value = _manifestUrl;
            document.getElementById('manifestResult').classList.add('show');
            setTimeout(function() {
                document.getElementById('manifestResult').scrollIntoView({ behavior: 'smooth', block: 'center' });
            }, 100);
        })
        .catch(function(err) {
            // ERREUR RÉSEAU
            btn.classList.remove('loading');
            btn.innerHTML = btnOrigHTML;
            showToast('Impossible de vérifier la clé — manifest généré quand même', 'warning', 4000);
            document.getElementById('manifestUrl').value = _manifestUrl;
            document.getElementById('manifestResult').classList.add('show');
            setTimeout(function() {
                document.getElementById('manifestResult').scrollIntoView({ behavior: 'smooth', block: 'center' });
            }, 100);
        });
}

function copyManifestUrl() {
    var url = document.getElementById('manifestUrl').value;
    if (!url) return;
    navigator.clipboard.writeText(url).then(function() {
        showToast('Manifest copié dans le presse-papier !', 'success', 3000);
        var btn = document.querySelector('.manifest-url-copy');
        btn.style.background = 'rgba(34,197,94,0.2)';
        setTimeout(function() { btn.style.background = ''; }, 600);
    }, function() {
        showToast('Erreur lors de la copie', 'error', 3000);
    });
}

function installToStremio() {
    var url = document.getElementById('manifestUrl').value;
    if (!url) return;
    var stremioUrl = url.replace(/^https?:\/\//, 'stremio://');
    window.open(stremioUrl, '_blank');
}

// Fuck les alertes moi j'veux des toasts
(function() {
    var _origAlert = window.alert;
    window.alert = function(msg) {
        if (!msg) return;
        var s = String(msg).toLowerCase();

        if (s.indexOf('copied') !== -1 || s.indexOf('copié') !== -1 || s.indexOf('clipboard') !== -1) {
            showToast('Manifest copié dans le presse-papier !', 'success', 3000);
        }
        else if (s.indexOf('erreur') !== -1 || s.indexOf('error') !== -1 || s.indexOf('invalide') !== -1 || s.indexOf('invalid') !== -1) {
            showToast(msg, 'error', 4500);
            if (typeof window.highlightValidationErrors === 'function') {
                window.highlightValidationErrors(msg);
            }
        }
        else if (s.indexOf('please fill') !== -1 || s.indexOf('required') !== -1 || s.indexOf('veuillez') !== -1 || s.indexOf('manquant') !== -1) {
            showToast(msg, 'warning', 5000);
            if (typeof window.highlightValidationErrors === 'function') {
                window.highlightValidationErrors(msg);
            }
        }
        else if (s.indexOf('expiré') !== -1 || s.indexOf('expired') !== -1 || s.indexOf('réessayer') !== -1) {
            showToast(msg, 'warning', 5000);
        }
        else if (s.indexOf('passkey') !== -1 || s.indexOf('caractère') !== -1) {
            showToast(msg, 'error', 4500);
            if (typeof window.highlightValidationErrors === 'function') {
                window.highlightValidationErrors(msg);
            }
        }
        else {
            showToast(msg, 'warning', 4000);
        }
    };
})();

function syncCred(prefix) {
    var email = document.getElementById(prefix + '_email');
    var pass = document.getElementById(prefix + '_pass');
    var hidden = document.getElementById(prefix + '_credentials');
    if (email && pass && hidden) {
        var e = email.value.trim();
        var p = pass.value;
        hidden.value = (e || p) ? e + ':' + p : '';
    }
}

function loadCred(prefix) {
    var hidden = document.getElementById(prefix + '_credentials');
    var email = document.getElementById(prefix + '_email');
    var pass = document.getElementById(prefix + '_pass');
    if (hidden && email && pass && hidden.value && hidden.value.indexOf(':') !== -1) {
        var parts = hidden.value.split(':');
        email.value = parts[0] || '';
        pass.value = parts.slice(1).join(':') || '';
    }
}

document.addEventListener('DOMContentLoaded', function() {
    var ygg = document.getElementById('yggflix');
    if (ygg) {
        Object.defineProperty(ygg, 'checked', {
            get: function() { return true; },
            set: function() {},
            configurable: true
        });
        Object.defineProperty(ygg, 'disabled', {
            get: function() { return false; },
            set: function() {},
            configurable: true
        });
    }

    setTimeout(function() {
        loadCred('offcloud');
        loadCred('pikpak');
        var apiKey = document.getElementById('ApiKey');
        if (apiKey) {
            if (apiKey.value) validateKeyLive(apiKey.value);
            // Reinforce live validation via addEventListener (in case inline oninput is stale)
            apiKey.addEventListener('input', function() {
                validateKeyLive(this.value);
            });
        }
    }, 150);

    // NAV
    var _wizSteps = document.querySelectorAll('.wizard-step');
    var _wizDots = document.querySelectorAll('.wiz-dot');
    var _wizLines = document.querySelectorAll('.wiz-line');
    var _wizTitle = document.getElementById('wizTitle');
    var _wizSub = document.getElementById('wizSubtitle');
    var _wizProgress = document.getElementById('wizProgress');
    var _wizArrowPrev = document.getElementById('wizArrowPrev');
    var _wizArrowNext = document.getElementById('wizArrowNext');
    var _wizPrev = document.getElementById('wizPrev');
    var _wizNext = document.getElementById('wizNext');
    var _wizCounter = document.getElementById('wizCounter');
    var _currentStep = 0;
    var _prevStep = 0;
    var _totalSteps = _wizSteps.length;

    var _dotIcons = [];
    _wizDots.forEach(function(d) { _dotIcons.push(d.innerHTML); });
    var _checkSvg = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M20 6 9 17l-5-5"/></svg>';
    var _errorSvg = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>';

    var _stepMeta = [
        { title: "Authentification", sub: "Entrez votre clé d'accès pour commencer" },
        { title: "Services de Débridage", sub: "Choisissez et configurez vos débrideurs" },
        { title: "Sources de Torrents", sub: "Configurez vos fournisseurs de recherche" },
        { title: "Services & Contenu", sub: "Personnalisez les services, catalogues et métadonnées" },
        { title: "Tri & Exclusions", sub: "Filtrez les résultats selon vos préférences" },
        { title: "Langues & Limites", sub: "Définissez vos langues et limites de résultats" },
        { title: "Installation", sub: "Générez et installez votre addon" },
    ];

    var _fieldStepMap = {
        'API Key': { step: 0, field: 'ApiKey', label: 'Clé API Stream-Fusion manquante ou invalide' },
        'Real-Debrid Account Connection': { step: 1, field: 'rd_token_info_div', label: 'Connexion Real-Debrid requise' },
        'AllDebrid Account Connection': { step: 1, field: 'ad_token_info_div', label: 'Connexion AllDebrid requise' },
        'TorBox Account Connection': { step: 1, field: 'tb_token_info_div', label: 'Connexion TorBox requise' },
        'Premiumize Account Connection': { step: 1, field: 'pm_token_info_div', label: 'Connexion Premiumize requise' },
        'Debrid-Link API Key': { step: 1, field: 'debridlink_api_key', label: 'Clé API Debrid-Link manquante' },
        'EasyDebrid API Key': { step: 1, field: 'easydebrid_api_key', label: 'Clé API EasyDebrid manquante' },
        'Offcloud Credentials': { step: 1, field: 'offcloud_credentials_div', label: 'Identifiants Offcloud manquants' },
        'PikPak Credentials': { step: 1, field: 'pikpak_credentials_div', label: 'Identifiants PikPak manquants' },
        'Cache URL': { step: 3, field: 'cacheUrl', label: 'URL de cache manquante' },
        'C411 API Key': { step: 2, field: 'c411ApiKey', label: 'Clé API C411 manquante' },
        'Torr9 API Key': { step: 2, field: 'torr9ApiKey', label: 'Clé API Torr9 manquante' },
        'LaCale API Key': { step: 2, field: 'lacaleApiKey', label: 'Clé API La-Cale manquante' },
        'Generation Free API Key': { step: 2, field: 'generationfreeApiKey', label: 'Clé API Generation Free manquante' },
        'ABN API Key': { step: 2, field: 'abnApiKey', label: 'Clé API ABN manquante' },
        'G3MINI API Key': { step: 2, field: 'g3miniApiKey', label: 'Clé API G3MINI manquante' },
        'TheOldSchool API Key': { step: 2, field: 'theoldschoolApiKey', label: 'Clé API TheOldSchool manquante' },
        'Languages': { step: 5, field: 'languageCheckBoxes', label: 'Sélectionnez au moins une langue' },
        'StremThru URL': { step: 3, field: 'stremthru_url', label: 'URL StremThru manquante' },
    };
    var _errorSteps = new Set();

    function clearValidationErrors() {
        _errorSteps.clear();
        _wizDots.forEach(function(d) { d.classList.remove('error'); });
        document.querySelectorAll('.field-error-hint').forEach(function(el) { el.remove(); });
    }

    function showFieldError(fieldId, message, stepIdx) {
        var el = document.getElementById(fieldId);
        if (!el) return;
        var target = el.closest('.fi-group') || el.closest('.tog-row') || el.closest('.cred-pair') || el.parentElement;
        if (!target) return;
        if (target.querySelector('.field-error-hint')) return;
        var hint = document.createElement('div');
        hint.className = 'field-error-hint';
        hint.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>' + message;
        target.appendChild(hint);

        function removeHint() {
            if (hint.parentElement) hint.remove();
            el.removeEventListener('input', removeHint);
            el.removeEventListener('change', removeHint);
            if (typeof stepIdx === 'number') {
                var stepEl = document.querySelector('[data-wizard-step="' + stepIdx + '"]');
                if (stepEl && !stepEl.querySelector('.field-error-hint')) {
                    _errorSteps.delete(stepIdx);
                    updateWizard(true);
                }
            }
        }
        el.addEventListener('input', removeHint);
        el.addEventListener('change', removeHint);
        var siblings = target.querySelectorAll('input');
        siblings.forEach(function(sib) {
            if (sib !== el) {
                sib.addEventListener('input', removeHint, { once: true });
                sib.addEventListener('change', removeHint, { once: true });
            }
        });
    }

    function highlightValidationErrors(alertMsg) {
        clearValidationErrors();
        var stepsWithErrors = new Set();
        // Sort keys longest-first so "C411 API Key" is consumed before "API Key"
        var keys = Object.keys(_fieldStepMap).sort(function(a, b) { return b.length - a.length; });
        var remaining = alertMsg;
        keys.forEach(function(key) {
            if (remaining.indexOf(key) !== -1) {
                var info = _fieldStepMap[key];
                stepsWithErrors.add(info.step);
                _errorSteps.add(info.step);
                showFieldError(info.field, info.label, info.step);
                // Strip matched key so shorter substrings don't false-positive
                while (remaining.indexOf(key) !== -1) {
                    remaining = remaining.replace(key, '');
                }
            }
        });
        stepsWithErrors.forEach(function(stepIdx) {
            var dot = _wizDots[stepIdx];
            if (dot) {
                dot.classList.remove('active', 'done');
                dot.classList.add('error');
                dot.innerHTML = _dotIcons[stepIdx];
            }
        });
        if (stepsWithErrors.size > 0) {
            var first = Math.min.apply(null, Array.from(stepsWithErrors));
            goToStep(first);
        }
    }

    function updateWizard(skipAnim) {
        var direction = _currentStep > _prevStep ? 'slide-left' : 'slide-right';
        _wizSteps.forEach(function(s, i) {
            s.classList.remove('active', 'slide-left', 'slide-right');
            if (i === _currentStep) {
                s.classList.add('active', direction);
                if (!skipAnim) {
                    s.style.animation = 'none';
                    s.offsetHeight;
                    s.style.animation = '';
                }
            }
        });
        _wizDots.forEach(function(d, i) {
            d.classList.remove('active', 'done', 'error');
            if (i === _currentStep) {
                d.classList.add('active');
                d.innerHTML = _dotIcons[i];
            } else if (i < _currentStep) {
                d.classList.add('done');
                d.innerHTML = _dotIcons[i];
            } else {
                d.innerHTML = _dotIcons[i];
            }
        });
        _errorSteps.forEach(function(stepIdx) {
            if (stepIdx === _currentStep) return;
            var d = _wizDots[stepIdx];
            if (d) {
                d.classList.remove('active', 'done');
                d.classList.add('error');
                d.innerHTML = _dotIcons[stepIdx];
            }
        });
        _wizLines.forEach(function(l, i) {
            l.classList.toggle('done', i < _currentStep);
        });
        if (_stepMeta[_currentStep]) {
            _wizTitle.textContent = _stepMeta[_currentStep].title;
            _wizSub.textContent = _stepMeta[_currentStep].sub;
        }
        var pct = ((_currentStep + 1) / _totalSteps) * 100;
        _wizProgress.style.width = pct + '%';
        // FLÈCHES
        _wizArrowPrev.disabled = _currentStep === 0;
        _wizArrowNext.disabled = _currentStep === _totalSteps - 1;
        // BOUTONS
        _wizPrev.disabled = _currentStep === 0;
        _wizPrev.style.visibility = _currentStep === 0 ? 'hidden' : 'visible';
        _wizNext.style.display = _currentStep === _totalSteps - 1 ? 'none' : '';
        // COMPTEUR
        _wizCounter.textContent = (_currentStep + 1) + ' / ' + _totalSteps;
        window.scrollTo({ top: 0, behavior: 'smooth' });
    }

    window.goToStep = function(n) {
        if (n < 0 || n >= _totalSteps) return;
        _prevStep = _currentStep;
        _currentStep = n;
        updateWizard();
    };
    window.nextStep = function() { goToStep(_currentStep + 1); };
    window.prevStep = function() { goToStep(_currentStep - 1); };

    window.clearValidationErrors = clearValidationErrors;
    window.highlightValidationErrors = highlightValidationErrors;
    window.showFieldError = showFieldError;
    window._wizDots = _wizDots;
    window._errorSteps = _errorSteps;
    window._errorSvg = _errorSvg;
    window._dotIcons = _dotIcons;

    // Clear error hints when a provider toggle is unchecked
    var _toggleFieldMap = {
        'c411': 'c411ApiKey',
        'torr9': 'torr9ApiKey',
        'lacale': 'lacaleApiKey',
        'generationfree': 'generationfreeApiKey',
        'abn': 'abnApiKey',
        'g3mini': 'g3miniApiKey',
        'theoldschool': 'theoldschoolApiKey',
        'debrid_rd': 'rd_token_info_div',
        'debrid_ad': 'ad_token_info_div',
        'debrid_tb': 'tb_token_info_div',
        'debrid_pm': 'pm_token_info_div',
        'debrid_dl': 'debridlink_api_key',
        'debrid_ed': 'easydebrid_api_key',
        'debrid_oc': 'offcloud_credentials_div',
        'debrid_pk': 'pikpak_credentials_div',
        'stremthru_enabled': 'stremthru_url'
    };
    Object.keys(_toggleFieldMap).forEach(function(togId) {
        var tog = document.getElementById(togId);
        if (!tog) return;
        tog.addEventListener('change', function() {
            if (tog.checked) return;
            var fieldId = _toggleFieldMap[togId];
            var el = document.getElementById(fieldId);
            if (!el) return;
            var target = el.closest('.fi-group') || el.closest('.tog-row') || el.closest('.cred-pair') || el.parentElement;
            if (!target) return;
            var hints = target.querySelectorAll('.field-error-hint');
            hints.forEach(function(h) { h.remove(); });
            // Find which step this field belongs to and clear error if no more hints in that step
            var info = _fieldStepMap[Object.keys(_fieldStepMap).filter(function(k) { return _fieldStepMap[k].field === fieldId; })[0]];
            if (info) {
                var stepEl = document.querySelector('[data-wizard-step="' + info.step + '"]');
                if (stepEl && !stepEl.querySelector('.field-error-hint')) {
                    _errorSteps.delete(info.step);
                    updateWizard(true);
                }
            }
        });
    });

    // Show deprecation modal when Real-Debrid is toggled on
    var _rdCheckbox = document.getElementById('debrid_rd');
    var _rdModal = document.getElementById('rd-modal');
    var _rdModalClose = document.getElementById('rd-modal-close');
    if (_rdCheckbox && _rdModal) {
        _rdCheckbox.addEventListener('change', function() {
            if (_rdCheckbox.checked) {
                _rdModal.classList.add('show');
            }
        });
        if (_rdModalClose) {
            _rdModalClose.addEventListener('click', function() {
                _rdModal.classList.remove('show');
            });
        }
        _rdModal.addEventListener('click', function(e) {
            if (e.target === _rdModal) _rdModal.classList.remove('show');
        });
    }

    // Auto-uncheck "Personnaliser l'ordre" when no debrid services remain
    var _debridOpts = document.getElementById('debridDownloaderOptions');

    // Click anywhere on a radio row to select it
    if (_debridOpts) {
        _debridOpts.addEventListener('click', function(e) {
            var row = e.target.closest('#debridDownloaderOptions > div');
            if (!row) return;
            var radio = row.querySelector('input[type="radio"]');
            if (radio && !radio.checked) {
                radio.checked = true;
                radio.dispatchEvent(new Event('change', { bubbles: true }));
            }
        });
    }

    var _debridOrderCb = document.getElementById('debrid_order');
    var _debridOrderList = document.getElementById('debridOrderList');
    if (_debridOpts && _debridOrderCb) {
        function _enforceOrderState() {
            var hasRealOptions = _debridOpts.children.length > 0;
            if (!hasRealOptions) {
                _debridOrderCb.disabled = true;
                _debridOrderCb.checked = false;
                if (_debridOrderList) _debridOrderList.classList.add('hidden');
            }
        }

        // After ANY input change in the form, wait for config.js to finish then enforce
        var _form = _debridOrderCb.closest('form') || document;
        _form.addEventListener('change', function() {
            setTimeout(_enforceOrderState, 0);
        }, true);

        // Also block direct clicks when no options exist
        _debridOrderCb.addEventListener('click', function(e) {
            if (_debridOpts.children.length === 0) {
                e.preventDefault();
                _debridOrderCb.checked = false;
            }
        });

        // Initial enforcement
        setTimeout(_enforceOrderState, 200);
    }

    document.addEventListener('keydown', function(e) {
        var tag = document.activeElement.tagName;
        if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;
        if (e.key === 'ArrowRight' || e.key === 'ArrowDown') {
            e.preventDefault(); nextStep();
        } else if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') {
            e.preventDefault(); prevStep();
        }
    });

    updateWizard(true);

    // EXCLUSIONS MOTS CLÉS
    var hiddenInput = document.getElementById('exclusion-keywords');
    var wrap = document.getElementById('tagWrap');
    var ghost = document.getElementById('tagInput');
    var tags = [];

    function syncHidden() {
        hiddenInput.value = tags.join(', ');
    }

    function createChip(text) {
        var chip = document.createElement('span');
        chip.className = 'tag-chip';
        chip.setAttribute('data-tag', text);
        chip.innerHTML = '<span>' + text + '</span>'
            + '<span class="tag-chip-x" title="Supprimer">'
            + '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>'
            + '</span>';
        chip.querySelector('.tag-chip-x').addEventListener('click', function(e) {
            e.stopPropagation();
            removeTag(text);
        });
        return chip;
    }

    function addTag(raw) {
        var text = raw.trim();
        if (!text || tags.indexOf(text) !== -1) return;
        tags.push(text);
        wrap.insertBefore(createChip(text), ghost);
        syncHidden();
    }

    function removeTag(text) {
        var idx = tags.indexOf(text);
        if (idx === -1) return;
        tags.splice(idx, 1);
        var chip = wrap.querySelector('.tag-chip[data-tag="' + CSS.escape(text) + '"]');
        if (chip) {
            chip.style.animation = 'none';
            chip.style.transition = 'opacity 0.2s, transform 0.2s';
            chip.style.opacity = '0';
            chip.style.transform = 'scale(0.7)';
            setTimeout(function() { chip.remove(); }, 200);
        }
        syncHidden();
    }

    function removeLast() {
        if (tags.length === 0) return;
        removeTag(tags[tags.length - 1]);
    }

    ghost.addEventListener('keydown', function(e) {
        if (e.key === 'Enter' || e.key === ',') {
            e.preventDefault();
            var val = ghost.value.replace(/,/g, '');
            addTag(val);
            ghost.value = '';
        }
        if (e.key === 'Backspace' && ghost.value === '') {
            removeLast();
        }
    });

    ghost.addEventListener('paste', function(e) {
        e.preventDefault();
        var paste = (e.clipboardData || window.clipboardData).getData('text');
        var parts = paste.split(',');
        parts.forEach(function(p) { addTag(p); });
        ghost.value = '';
    });

    var _origValDesc = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value');
    var observer = new MutationObserver(function() { loadFromHidden(); });
    observer.observe(hiddenInput, { attributes: true, attributeFilter: ['value'] });

    function loadFromHidden() {
        var val = _origValDesc.get.call(hiddenInput);
        if (!val) return;
        var existing = val.split(',');
        existing.forEach(function(p) {
            var t = p.trim();
            if (t && tags.indexOf(t) === -1) addTag(t);
        });
    }

    setTimeout(loadFromHidden, 100);

    var sliders = [
        { range: 'range_maxSize', input: 'maxSize', fill: 'fill_maxSize', max: 500 },
        { range: 'range_resultsPerQuality', input: 'resultsPerQuality', fill: 'fill_resultsPerQuality', max: 50 },
        { range: 'range_maxResults', input: 'maxResults', fill: 'fill_maxResults', max: 100 },
        { range: 'range_minCachedResults', input: 'minCachedResults', fill: 'fill_minCachedResults', max: 50 }
    ];

    sliders.forEach(function(s) {
        var range = document.getElementById(s.range);
        var input = document.getElementById(s.input);
        var fill = document.getElementById(s.fill);
        if (!range || !input || !fill) return;

        function updateFill() {
            var min = parseFloat(range.min) || 0;
            var max = parseFloat(range.max) || s.max;
            var val = parseFloat(range.value) || 0;
            var pct = ((val - min) / (max - min)) * 100;
            fill.style.width = pct + '%';
        }

        range.addEventListener('input', function() {
            input.value = range.value;
            updateFill();
        });

        input.addEventListener('input', function() {
            var v = parseFloat(input.value);
            if (!isNaN(v)) {
                range.value = Math.min(Math.max(v, parseFloat(range.min) || 0), parseFloat(range.max) || s.max);
                updateFill();
            }
        });

        input.addEventListener('change', function() {
            var v = parseFloat(input.value);
            if (!isNaN(v)) {
                range.value = Math.min(Math.max(v, parseFloat(range.min) || 0), parseFloat(range.max) || s.max);
                updateFill();
            }
        });

        updateFill();
    });
});