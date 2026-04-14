// Cordyn Label Verification Tool — Frontend SPA
// Note: This is a LOCAL tool running on localhost only. All data comes from
// our own API backend. innerHTML is used for rendering trusted server data.

const FIELDS = [
  { key: 'hsr_number', label: 'HSR Number', type: 'scalar' },
  { key: 'signal_word', label: 'Signal Word', type: 'scalar' },
  { key: 'active_ingredients', label: 'Active Ingredients', type: 'list' },
  { key: 'container_sizes', label: 'Container Sizes', type: 'list' },
  { key: 'target_rates', label: 'Application Rates', type: 'rates' },
  { key: 'rainfastness_hours', label: 'Rainfastness', type: 'scalar', suffix: ' hours' },
  { key: 'max_applications_per_season', label: 'Max Applications/Season', type: 'scalar' },
  { key: 'growth_stage_earliest', label: 'Growth Stage Window', type: 'growth_stage' },
  { key: 'tank_mix_incompatible', label: 'Tank Mix (Incompatible)', type: 'list' },
  { key: 'tank_mix_required', label: 'Tank Mix (Required)', type: 'list' },
  { key: 'label_buffer_zone_m', label: 'Buffer Zone', type: 'scalar', suffix: ' m' },
  { key: 'ppe_requirements', label: 'PPE Requirements', type: 'list' },
  { key: 'environmental_cautions', label: 'Environmental Cautions', type: 'list' },
  { key: 'hsno_classifications', label: 'HSNO Classifications', type: 'list' },
  { key: 'shelf_life_years', label: 'Shelf Life', type: 'scalar', suffix: ' years' },
  { key: 'label_whp_raw', label: 'WHP (cross-val)', type: 'raw' },
  { key: 'label_rei_raw', label: 'REI (cross-val)', type: 'raw' },
];

const TAG_FIELDS = [
  'max_applications', 'rainfastness', 'target_rate', 'buffer_zone',
  'growth_stage', 'tank_mix', 'active_ingredients', 'container_sizes',
  'hsno_classifications', 'shelf_life', 'signal_word', 'hsr_number',
];

let currentProducts = [];
let currentFilter = 'has-label';
let currentSearch = '';

// ─── Labels-review state ────────────────────────────────────────────────
// Order of product ids matching the current filter+search — used for →/←
// product navigation. Rebuilt whenever the list view renders.
let productOrder = [];
// Current product's detail state (set in renderProductDetail, used by keyboard).
let currentDetail = null; // { id, fields: [{key,label,hasValue}], verifiedMap }
let currentFieldIndex = 0;
let keyboardMode = 'FIELD_NAV'; // 'FIELD_NAV' | 'CORRECTION_INPUT'
let undoState = null; // { productId, fields, timerId }

// ─── Utility: safe text escaping ────────────────────────────────────────

function esc(str) {
  const d = document.createElement('div');
  d.textContent = str || '';
  return d.textContent.replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

// ─── Utility: DOM builder ───────────────────────────────────────────────

function el(tag, attrs, ...children) {
  const e = document.createElement(tag);
  if (attrs) {
    for (const [k, v] of Object.entries(attrs)) {
      if (k === 'onclick' || k.startsWith('on')) e[k] = v;
      else if (k === 'className') e.className = v;
      else if (k === 'style' && typeof v === 'object') Object.assign(e.style, v);
      else e.setAttribute(k, v);
    }
  }
  for (const c of children) {
    if (typeof c === 'string') e.appendChild(document.createTextNode(c));
    else if (c) e.appendChild(c);
  }
  return e;
}

function setContent(container, ...nodes) {
  container.replaceChildren(...nodes);
}

// ─── Router ─────────────────────────────────────────────────────────────

function route() {
  const hash = location.hash || '#/';
  const app = document.getElementById('app');
  const goingToDetail = hash.startsWith('#/product/');
  if (!goingToDetail) {
    uninstallKeyboardHandler();
    currentDetail = null;
  }
  if (goingToDetail) {
    renderProductDetail(app, hash.replace('#/product/', ''));
  } else if (hash === '#/coverage') {
    renderCoverage(app);
  } else if (hash === '#/patterns') {
    renderPatterns(app);
  } else if (hash === '#/acvm') {
    renderAcvmReview(app);
  } else if (hash === '#/products') {
    renderProductList(app);
  } else {
    renderDashboard(app);
  }
  document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
  const r = hash === '#/coverage' ? 'coverage'
    : hash === '#/patterns' ? 'patterns'
    : hash === '#/acvm' ? 'acvm'
    : hash === '#/products' ? 'list'
    : 'dashboard';
  document.querySelector(`[data-route="${r}"]`)?.classList.add('active');
}

window.addEventListener('hashchange', route);
window.addEventListener('load', () => { loadCoverage(); checkBootstrap(); route(); });

// ─── Bootstrap banner (first-run / re-extract) ──────────────────────────

let bootstrapPoller = null;

async function checkBootstrap() {
  try {
    const status = await api('/bootstrap/status');
    if (status.running || status.needs_bootstrap) {
      showBootstrapBanner(status);
      if (status.running) startBootstrapPolling();
    } else {
      hideBootstrapBanner();
    }
  } catch (e) { /* ignore on startup */ }
}

function showBootstrapBanner(status) {
  let banner = document.getElementById('bootstrap-banner');
  if (!banner) {
    banner = el('div', { id: 'bootstrap-banner', className: 'bootstrap-banner' });
    document.body.insertBefore(banner, document.getElementById('app'));
  }
  banner.replaceChildren();

  if (status.running) {
    const phaseLabels = {
      extracting_text: 'Extracting text from label PDFs',
      extracting_fields: 'Running field extractors',
      saving: 'Saving cache',
    };
    const label = phaseLabels[status.phase] || status.phase;
    const pct = status.total > 0 ? Math.round(100 * status.current / status.total) : 0;
    banner.appendChild(el('div', { className: 'bootstrap-title' }, `${label}…`));
    banner.appendChild(el('div', { className: 'bootstrap-sub' },
      `${status.current} / ${status.total} (${pct}%)`));
    const bar = el('div', { className: 'bootstrap-bar' });
    bar.appendChild(el('div', { className: 'bootstrap-fill', style: { width: pct + '%' } }));
    banner.appendChild(bar);
  } else if (status.phase === 'error') {
    banner.appendChild(el('div', { className: 'bootstrap-title bootstrap-error' }, 'Extraction failed'));
    banner.appendChild(el('div', { className: 'bootstrap-sub' }, status.error || 'Unknown error'));
    const btn = el('button', { className: 'btn btn-primary' }, 'Retry');
    btn.onclick = runBootstrap;
    banner.appendChild(btn);
  } else {
    // needs_bootstrap
    banner.appendChild(el('div', { className: 'bootstrap-title' },
      'Label data not yet extracted'));
    banner.appendChild(el('div', { className: 'bootstrap-sub' },
      `${status.texts_extracted} / ${status.total_labels} labels have text extracted. ` +
      `Run the extractor to populate the tool (first run takes a few minutes).`));
    const btn = el('button', { className: 'btn btn-primary' },
      `Extract ${status.total_labels} labels`);
    btn.onclick = runBootstrap;
    banner.appendChild(btn);
  }
}

function hideBootstrapBanner() {
  const banner = document.getElementById('bootstrap-banner');
  if (banner) banner.remove();
  if (bootstrapPoller) { clearInterval(bootstrapPoller); bootstrapPoller = null; }
}

async function runBootstrap() {
  try {
    await api('/bootstrap/run', { method: 'POST', body: { force: false } });
    startBootstrapPolling();
  } catch (e) { alert('Failed to start: ' + e.message); }
}

function startBootstrapPolling() {
  if (bootstrapPoller) return;
  bootstrapPoller = setInterval(async () => {
    try {
      const status = await api('/bootstrap/status');
      showBootstrapBanner(status);
      if (!status.running) {
        clearInterval(bootstrapPoller);
        bootstrapPoller = null;
        if (status.phase === 'done') {
          // Reload products so the list reflects new extractions.
          setTimeout(() => { hideBootstrapBanner(); route(); loadCoverage(); }, 1200);
        }
      }
    } catch (e) { /* retry next tick */ }
  }, 1000);
}

// ─── API ────────────────────────────────────────────────────────────────

async function api(path, opts = {}) {
  const res = await fetch(`/api${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...opts,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  if (!res.ok) throw new Error(`API ${res.status}`);
  return res.json();
}

async function loadCoverage() {
  try {
    const cov = await api('/coverage');
    const bar = document.querySelector('.progress-fill');
    const text = document.querySelector('.progress-text');
    const reviewed = cov.reviewed ?? cov.verified ?? 0;
    const pct = cov.total > 0 ? Math.round(100 * reviewed / cov.total) : 0;
    bar.style.width = pct + '%';
    text.textContent = `${reviewed}/${cov.total} reviewed`;
  } catch (e) { /* startup timing */ }
}

// ─── Product List ───────────────────────────────────────────────────────

async function renderProductList(container) {
  setContent(container, el('p', {}, 'Loading products...'));
  try { currentProducts = await api('/products'); }
  catch (e) { setContent(container, el('p', { style: { color: 'var(--red)' } }, 'Error: ' + e.message)); return; }

  const wrapper = el('div', {});

  // Header
  const header = el('div', { className: 'product-list-header' });
  header.appendChild(el('h2', {}, `Products (${currentProducts.length})`));
  const searchInput = el('input', { className: 'search-input', placeholder: 'Search by name...', value: currentSearch });
  searchInput.oninput = () => { currentSearch = searchInput.value; renderProductRows(); };
  header.appendChild(searchInput);

  const chips = el('div', { className: 'filter-chips' });
  const filterLabels = {'has-label': 'Has label', 'all': 'All', 'no-label': 'No label', 'low': 'Low conf.', 'unverified': 'Unverified'};
  for (const f of ['has-label', 'all', 'no-label', 'low', 'unverified']) {
    const chip = el('button', { className: 'filter-chip' + (currentFilter === f ? ' active' : '') }, filterLabels[f]);
    chip.onclick = () => { currentFilter = f; renderProductList(container); };
    chips.appendChild(chip);
  }
  header.appendChild(chips);
  wrapper.appendChild(header);

  // Table
  const table = el('table', {});
  const thead = el('thead', {});
  const headRow = el('tr', {});
  for (const h of ['Product', 'Section', 'Confidence', 'Fields', 'Verified']) {
    headRow.appendChild(el('th', {}, h));
  }
  thead.appendChild(headRow);
  table.appendChild(thead);
  const tbody = el('tbody', { id: 'product-tbody' });
  table.appendChild(tbody);
  wrapper.appendChild(table);
  setContent(container, wrapper);
  renderProductRows();
}

function renderProductRows() {
  const tbody = document.getElementById('product-tbody');
  if (!tbody) return;
  let filtered = currentProducts;
  if (currentFilter === 'has-label') filtered = filtered.filter(p => p.has_label);
  if (currentFilter === 'no-label') filtered = filtered.filter(p => !p.has_label);
  if (currentFilter === 'low') filtered = filtered.filter(p => p.has_label && p.confidence === 'low');
  if (currentFilter === 'unverified') filtered = filtered.filter(p => p.has_label && p.verified_count === 0);
  if (currentSearch) {
    const q = currentSearch.toLowerCase();
    filtered = filtered.filter(p => p.name.toLowerCase().includes(q));
  }
  productOrder = filtered.map(p => p.id);
  tbody.replaceChildren();
  for (const p of filtered) {
    const row = el('tr', {});
    row.onclick = () => { location.hash = `#/product/${p.id}`; };
    row.appendChild(el('td', {}, el('strong', {}, p.name)));
    row.appendChild(el('td', {}, p.section));
    const badge = el('span', { className: `badge badge-${p.confidence}` }, p.confidence);
    row.appendChild(el('td', {}, badge));
    row.appendChild(el('td', {}, `${p.extracted_count}/${p.total_fields}`));
    row.appendChild(el('td', {}, p.verified_count > 0 ? p.verified_count + ' fields' : '-'));
    tbody.appendChild(row);
  }
}

// ─── Product Detail ─────────────────────────────────────────────────────

async function renderProductDetail(container, productId) {
  uninstallKeyboardHandler(); // re-installed after render
  setContent(container, el('p', {}, 'Loading...'));
  let data;
  try { data = await api(`/products/${productId}`); }
  catch (e) { setContent(container, el('p', { style: { color: 'var(--red)' } }, 'Error: ' + e.message)); return; }

  const { product, extraction, verified: ver, has_label } = data;
  const wrapper = el('div', {});

  // Back link
  const backDiv = el('div', { className: 'detail-back' });
  const backLink = el('a', { href: '#/' }, '\u2190 Back to products');
  backDiv.appendChild(backLink);
  wrapper.appendChild(backDiv);

  // Title
  const conf = extraction.extraction_confidence || 'none';
  const title = el('h2', { style: { marginBottom: '12px' } });
  title.appendChild(document.createTextNode((product?.name || productId) + ' '));
  title.appendChild(el('span', { className: `badge badge-${conf}` }, conf));
  wrapper.appendChild(title);

  if (!has_label) {
    wrapper.appendChild(el('p', { style: { color: 'var(--grey-500)', margin: '24px 0' } },
      'No label PDF available for this product. It may not have an ACVM registration match.'));
    setContent(container, wrapper);
    return;
  }

  // Split panel
  const detailContainer = el('div', { className: 'detail-container' });

  // PDF panel
  const pdfPanel = el('div', { className: 'pdf-panel' });
  const iframe = el('iframe', { src: `/api/products/${productId}/label`, id: 'pdf-iframe' });
  pdfPanel.appendChild(iframe);
  detailContainer.appendChild(pdfPanel);

  // Extraction panel
  const extPanel = el('div', { className: 'extraction-panel', id: 'extraction-panel' });
  detailContainer.appendChild(extPanel);

  wrapper.appendChild(detailContainer);
  setContent(container, wrapper);

  // Seed current-detail state for keyboard handler.
  const isSameProduct = currentDetail?.id === productId;
  currentDetail = {
    id: productId,
    extraction,
    verified: ver || {},
  };
  if (!isSameProduct) {
    currentFieldIndex = 0;
  }
  keyboardMode = 'FIELD_NAV';

  renderExtractionFields(productId, extraction, ver || {});
  installKeyboardHandler();
}

function renderExtractionFields(productId, ext, ver) {
  const panel = document.getElementById('extraction-panel');
  if (!panel) return;
  panel.replaceChildren();

  FIELDS.forEach((field, idx) => {
    const val = ext[field.key];
    const rawKey = field.key + '_raw';
    const raw = ext[rawKey];
    const verified = ver[field.key];
    const hasValue = val !== null && val !== undefined && (!Array.isArray(val) || val.length > 0);
    const cardClasses = ['field-card'];
    if (verified?.status === 'correct') cardClasses.push('field-verified');
    else if (verified?.status === 'wrong') cardClasses.push('field-corrected');
    else if (verified?.status === 'absent') cardClasses.push('field-absent');
    if (idx === currentFieldIndex) cardClasses.push('field-focused');
    const card = el('div', {
      className: cardClasses.join(' '),
      'data-field-index': String(idx),
      'data-field-key': field.key,
    });
    card.onclick = () => { setCurrentField(idx); };

    // Header
    const header = el('div', { className: 'field-card-header' });
    header.appendChild(el('span', { className: 'field-name' }, field.label));
    if (verified) {
      const badgeType = verified.status === 'correct' ? 'high' : verified.status === 'wrong' ? 'medium' : 'low';
      header.appendChild(el('span', { className: `badge badge-${badgeType}` }, verified.status));
    }
    card.appendChild(header);

    // Value
    if (hasValue) {
      const valDiv = el('div', { className: 'field-value' });
      if (field.type === 'list' && Array.isArray(val)) {
        valDiv.textContent = val.map(v => typeof v === 'object' ? (v.name ? `${v.name} ${v.concentration_value || ''} ${v.concentration_unit || ''}` : JSON.stringify(v)) : String(v)).join(', ');
      } else if (field.type === 'rates' && Array.isArray(val)) {
        valDiv.textContent = val.map(r => `${r.target}: ${r.rate_value}`).join('; ');
      } else if (field.type === 'growth_stage') {
        valDiv.textContent = `${ext.growth_stage_earliest || '?'} to ${ext.growth_stage_latest || '?'}`;
      } else {
        valDiv.textContent = String(val) + (field.suffix || '');
      }
      card.appendChild(valDiv);
      if (raw) {
        const rawDiv = el('div', { className: 'field-raw' });
        rawDiv.textContent = String(raw).substring(0, 150);
        card.appendChild(rawDiv);
      }
    } else {
      card.appendChild(el('div', { className: 'field-not-found' }, 'Not found'));
    }

    // Correction input container (hidden until mode === CORRECTION_INPUT for this field)
    const correctionWrap = el('div', { className: 'correction-wrap hidden', 'data-correction-for': field.key });
    card.appendChild(correctionWrap);

    // Actions
    const actions = el('div', { className: 'field-actions' });
    const btnCorrect = el('button', { className: 'btn btn-correct', title: 'Mark correct (1)' }, 'Correct');
    btnCorrect.onclick = (e) => { e.stopPropagation(); verifyField(productId, field.key, 'correct', idx); };
    const btnWrong = el('button', { className: 'btn btn-wrong', title: 'Enter correction (2)' }, 'Wrong');
    btnWrong.onclick = (e) => { e.stopPropagation(); openCorrectionInput(idx); };
    const btnAbsent = el('button', { className: 'btn btn-absent', title: 'Not on label (3)' }, 'Not on label');
    btnAbsent.onclick = (e) => { e.stopPropagation(); verifyField(productId, field.key, 'absent', idx); };
    actions.append(btnCorrect, btnWrong, btnAbsent);
    card.appendChild(actions);

    panel.appendChild(card);
  });

  // Manual annotation section
  const annoCard = el('div', { className: 'field-card', style: { background: 'var(--grey-100)' } });
  annoCard.appendChild(el('div', { className: 'field-name' }, 'Manual Annotation'));
  annoCard.appendChild(el('p', { style: { fontSize: '12px', margin: '8px 0' } }, 'Paste label text here, then select the field type:'));
  const textarea = el('textarea', {
    id: 'manual-annotation-text',
    placeholder: 'Paste label text here...',
    style: { width: '100%', height: '60px', fontFamily: 'var(--mono)', fontSize: '12px', border: '1px solid var(--grey-300)', padding: '8px' },
  });
  annoCard.appendChild(textarea);
  const tagBtns = el('div', { style: { display: 'flex', gap: '4px', marginTop: '8px', flexWrap: 'wrap' } });
  for (const f of TAG_FIELDS) {
    const btn = el('button', { className: 'tag-btn' }, f.replace(/_/g, ' '));
    btn.onclick = () => annotateManual(productId, f);
    tagBtns.appendChild(btn);
  }
  annoCard.appendChild(tagBtns);
  panel.appendChild(annoCard);
}

async function verifyField(productId, field, status, fieldIdx, { advance = true } = {}) {
  await api(`/products/${productId}/verify`, { method: 'POST', body: { field, status } });
  if (advance && typeof fieldIdx === 'number') {
    currentFieldIndex = Math.min(fieldIdx + 1, FIELDS.length - 1);
  }
  route();
  loadCoverage();
}

// ─── Keyboard state machine (Labels review mode) ────────────────────────
//
//   ┌──────────────────────── FIELD_NAV (default) ──────────────────────┐
//   │  1         → mark current field Correct, advance                  │
//   │  2         → enter CORRECTION_INPUT for current field             │
//   │  3         → mark current field Not-on-label, advance             │
//   │  Tab/↓     → next field                                           │
//   │  Shift+Tab/↑ → previous field                                     │
//   │  →         → save + open next product (in filtered list order)    │
//   │  ←         → previous product                                     │
//   │  Shift+V   → bulk-verify all unreviewed fields Correct            │
//   │              (toast + 3s Undo)                                    │
//   │  T         → toggle Review/Train mode (placeholder; future)       │
//   │  /         → focus product search (navigate to list if needed)    │
//   │  ?         → show shortcut cheatsheet                             │
//   │  Escape    → close modals / clear toast                           │
//   └─────────────────────────────────────┬─────────────────────────────┘
//                                         │
//                                         │ press 2 / click Wrong
//                                         ▼
//   ┌──────────────────── CORRECTION_INPUT (input focused) ─────────────┐
//   │  Escape    → cancel, field stays unverified, → FIELD_NAV          │
//   │  Enter     → save correction, mark 'wrong', advance, → FIELD_NAV  │
//   │  Tab       → save-and-advance (same as Enter). Chosen so the      │
//   │              reviewer has one "I'm done" key inside inputs.       │
//   │  1/2/3     → swallowed (treated as typed characters)              │
//   └────────────────────────────────────────────────────────────────────┘

function installKeyboardHandler() {
  document.addEventListener('keydown', handleKeyDown);
}

function uninstallKeyboardHandler() {
  document.removeEventListener('keydown', handleKeyDown);
}

function handleKeyDown(e) {
  // Never intercept typing in non-correction inputs (product search, pattern
  // editor, annotation textarea). The correction input handles its own keys
  // via inline listeners, so we exit early if any input is focused.
  const active = document.activeElement;
  const inInput = active && (active.tagName === 'INPUT' || active.tagName === 'TEXTAREA');
  if (inInput) return;
  if (keyboardMode === 'CORRECTION_INPUT') return; // handled by input listeners

  // Global shortcuts first (work even outside detail view)
  if (e.key === '?') { e.preventDefault(); showCheatsheet(); return; }
  if (e.key === '/') {
    e.preventDefault();
    // If not on list view, navigate there; then focus search.
    if (!location.hash.startsWith('#/product/')) {
      const input = document.querySelector('.search-input');
      if (input) { input.focus(); input.select(); return; }
    }
    location.hash = '#/';
    setTimeout(() => document.querySelector('.search-input')?.focus(), 0);
    return;
  }
  if (e.key === 'Escape') {
    if (undoState) { clearUndoState(); hideToast(); }
    const modal = document.getElementById('pattern-modal');
    if (modal && !modal.classList.contains('hidden')) { closePatternModal(); return; }
    const sheet = document.getElementById('cheatsheet');
    if (sheet && !sheet.classList.contains('hidden')) { sheet.classList.add('hidden'); return; }
    return;
  }

  // Product-detail-only shortcuts
  if (!currentDetail) return;

  if (e.key === '1') { e.preventDefault(); verifyCurrentField('correct'); return; }
  if (e.key === '2') { e.preventDefault(); openCorrectionInput(currentFieldIndex); return; }
  if (e.key === '3') { e.preventDefault(); verifyCurrentField('absent'); return; }

  if (e.key === 'Tab' || e.key === 'ArrowDown') {
    e.preventDefault();
    const dir = e.shiftKey && e.key === 'Tab' ? -1 : 1;
    setCurrentField(currentFieldIndex + dir);
    return;
  }
  if (e.key === 'ArrowUp') { e.preventDefault(); setCurrentField(currentFieldIndex - 1); return; }

  if (e.key === 'ArrowRight') { e.preventDefault(); gotoProduct(1); return; }
  if (e.key === 'ArrowLeft')  { e.preventDefault(); gotoProduct(-1); return; }

  if (e.key === 'V' && e.shiftKey) { e.preventDefault(); bulkVerifyUnreviewed(); return; }
  // 'T' (Train mode toggle) — placeholder until Train mode lands.
  if (e.key === 't' || e.key === 'T') {
    if (e.shiftKey) return; // reserve Shift+T for future
    // no-op for now; keep keybinding reserved
    return;
  }
}

function verifyCurrentField(status) {
  if (!currentDetail) return;
  const field = FIELDS[currentFieldIndex];
  if (!field) return;
  verifyField(currentDetail.id, field.key, status, currentFieldIndex);
}

function setCurrentField(idx) {
  if (idx < 0) idx = 0;
  if (idx >= FIELDS.length) idx = FIELDS.length - 1;
  currentFieldIndex = idx;
  // Update highlight without re-rendering everything.
  document.querySelectorAll('.field-card').forEach(card => {
    const cardIdx = Number(card.getAttribute('data-field-index'));
    card.classList.toggle('field-focused', cardIdx === idx);
    if (cardIdx === idx) card.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
  });
}

function gotoProduct(direction) {
  if (!currentDetail || productOrder.length === 0) return;
  const currentIdx = productOrder.indexOf(currentDetail.id);
  if (currentIdx === -1) return;
  const newIdx = currentIdx + direction;
  if (newIdx < 0 || newIdx >= productOrder.length) return;
  location.hash = `#/product/${productOrder[newIdx]}`;
}

// ─── Inline correction input ────────────────────────────────────────────

function openCorrectionInput(fieldIdx) {
  if (!currentDetail) return;
  setCurrentField(fieldIdx);
  const field = FIELDS[fieldIdx];
  const card = document.querySelector(`.field-card[data-field-index="${fieldIdx}"]`);
  if (!card) return;
  const wrap = card.querySelector('.correction-wrap');
  if (!wrap) return;

  wrap.classList.remove('hidden');
  wrap.replaceChildren();
  const input = el('input', {
    type: 'text',
    className: 'correction-input',
    placeholder: `Correct value for "${field.label}" — Enter to save, Esc to cancel`,
  });
  wrap.appendChild(input);

  keyboardMode = 'CORRECTION_INPUT';

  const commit = async () => {
    const value = input.value.trim();
    if (!value) return cancel();
    // Flip mode first so the impending blur (from re-render) doesn't re-cancel.
    keyboardMode = 'FIELD_NAV';
    await api(`/products/${currentDetail.id}/correct`, {
      method: 'POST',
      body: { field: field.key, correct_value: value },
    });
    await verifyField(currentDetail.id, field.key, 'wrong', fieldIdx);
  };

  const cancel = () => {
    wrap.classList.add('hidden');
    wrap.replaceChildren();
    keyboardMode = 'FIELD_NAV';
  };

  input.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') { e.preventDefault(); cancel(); }
    else if (e.key === 'Enter' || e.key === 'Tab') { e.preventDefault(); commit(); }
  });
  input.addEventListener('blur', () => {
    // If user clicks elsewhere without Enter/Escape, treat as cancel.
    if (keyboardMode === 'CORRECTION_INPUT') cancel();
  });

  setTimeout(() => input.focus(), 0);
}

// ─── Bulk-verify with Undo ──────────────────────────────────────────────

async function bulkVerifyUnreviewed() {
  if (!currentDetail) return;
  const unreviewed = FIELDS
    .map(f => f.key)
    .filter(k => !currentDetail.verified[k]);
  if (unreviewed.length === 0) {
    showToast('All fields already reviewed', null);
    return;
  }
  const res = await api(`/products/${currentDetail.id}/verify/bulk`, {
    method: 'POST',
    body: { fields: unreviewed, status: 'correct' },
  });
  const applied = res.applied || unreviewed;
  undoState = {
    productId: currentDetail.id,
    fields: applied,
    timerId: setTimeout(clearUndoState, 3000),
  };
  showToast(`Marked ${applied.length} fields Correct`, () => undoBulkVerify());
  route();
  loadCoverage();
}

async function undoBulkVerify() {
  if (!undoState) return;
  const { productId, fields } = undoState;
  clearUndoState();
  await api(`/products/${productId}/verify/unverify`, {
    method: 'POST',
    body: { fields },
  });
  hideToast();
  route();
  loadCoverage();
}

function clearUndoState() {
  if (undoState?.timerId) clearTimeout(undoState.timerId);
  undoState = null;
}

// ─── Toast ──────────────────────────────────────────────────────────────

function showToast(msg, undoCallback) {
  let toast = document.getElementById('toast');
  if (!toast) {
    toast = el('div', { id: 'toast', className: 'toast hidden' });
    document.body.appendChild(toast);
  }
  toast.replaceChildren();
  toast.appendChild(el('span', {}, msg));
  if (undoCallback) {
    const btn = el('button', { className: 'toast-undo' }, 'Undo');
    btn.onclick = undoCallback;
    toast.appendChild(btn);
  }
  toast.classList.remove('hidden');
  clearTimeout(toast._hideTimer);
  toast._hideTimer = setTimeout(() => toast.classList.add('hidden'), 3000);
}

function hideToast() {
  const toast = document.getElementById('toast');
  if (toast) toast.classList.add('hidden');
}

// ─── Cheatsheet ─────────────────────────────────────────────────────────

function showCheatsheet() {
  let sheet = document.getElementById('cheatsheet');
  if (!sheet) {
    sheet = el('div', { id: 'cheatsheet', className: 'modal' });
    const content = el('div', { className: 'modal-content' });
    const header = el('div', { className: 'modal-header' });
    header.appendChild(el('h3', {}, 'Keyboard shortcuts'));
    const close = el('button', { className: 'modal-close' }, '\u00d7');
    close.onclick = () => sheet.classList.add('hidden');
    header.appendChild(close);
    content.appendChild(header);
    const body = el('div', {});
    const rows = [
      ['1', 'Mark current field Correct'],
      ['2', 'Enter correction for current field'],
      ['3', 'Mark current field Not on label'],
      ['Tab / \u2193', 'Next field'],
      ['Shift+Tab / \u2191', 'Previous field'],
      ['\u2192', 'Next product'],
      ['\u2190', 'Previous product'],
      ['Shift+V', 'Mark all unreviewed fields Correct (3s Undo)'],
      ['/', 'Focus product search'],
      ['?', 'Show this cheatsheet'],
      ['Esc', 'Close modal / cancel correction'],
    ];
    const table = el('table', { className: 'cheatsheet-table' });
    for (const [k, desc] of rows) {
      const tr = el('tr', {});
      tr.appendChild(el('td', {}, el('kbd', {}, k)));
      tr.appendChild(el('td', {}, desc));
      table.appendChild(tr);
    }
    body.appendChild(table);
    content.appendChild(body);
    sheet.appendChild(content);
    document.body.appendChild(sheet);
  }
  sheet.classList.remove('hidden');
}

async function annotateManual(productId, field) {
  const textarea = document.getElementById('manual-annotation-text');
  const text = textarea?.value?.trim();
  if (!text) { alert('Please paste or type the label text first'); return; }
  const value = prompt(`Enter the structured value for "${field}":\n\nText: "${text.substring(0, 80)}..."`);

  try {
    const result = await api(`/products/${productId}/annotate`, {
      method: 'POST',
      body: { field, selected_text: text, structured_value: value },
    });
    if (result.candidates && result.candidates.length > 0) {
      showPatternModal(result.candidates, field, productId);
    } else {
      alert('Annotation saved. No patterns generated (text may be too short).');
    }
  } catch (e) { alert('Error: ' + e.message); }
}

// ─── Pattern Learning Modal ─────────────────────────────────────────────

function showPatternModal(candidates, field, sourceProduct) {
  const modal = document.getElementById('pattern-modal');
  const body = document.getElementById('pattern-modal-body');
  body.replaceChildren();

  body.appendChild(el('p', { style: { marginBottom: '12px' } },
    `Generated ${candidates.length} candidate pattern(s) for ${field}:`));

  for (const c of candidates) {
    const block = el('div', { style: { marginBottom: '16px', padding: '12px', background: 'var(--grey-100)' } });
    block.appendChild(el('div', { style: { fontSize: '11px', color: 'var(--grey-500)', marginBottom: '4px' } }, c.strategy));
    const patternDiv = el('div', { className: 'pattern-text' });
    patternDiv.textContent = c.pattern;
    block.appendChild(patternDiv);
    block.appendChild(el('div', { className: 'pattern-result' },
      `${c.new_match_count} new matches / ${c.total_matches} total`));
    if (c.new_match_count > 0) {
      block.appendChild(el('div', { className: 'match-list' },
        'New: ' + c.new_matches.slice(0, 8).join(', ')));
    }
    const btns = el('div', { style: { marginTop: '8px', display: 'flex', gap: '8px' } });
    const approveBtn = el('button', { className: 'btn btn-primary' }, 'Approve');
    approveBtn.onclick = () => approvePattern(c.pattern, field, sourceProduct);
    const rejectBtn = el('button', { className: 'btn' }, 'Reject');
    rejectBtn.onclick = () => closePatternModal();
    btns.append(approveBtn, rejectBtn);
    block.appendChild(btns);
    body.appendChild(block);
  }

  // Custom pattern editor
  const editorSection = el('div', { style: { marginTop: '16px', borderTop: '1px solid var(--grey-300)', paddingTop: '12px' } });
  editorSection.appendChild(el('div', { className: 'field-name' }, 'Write custom pattern'));
  const patternInput = el('input', { className: 'pattern-input', id: 'custom-pattern', placeholder: 'Enter regex pattern...' });
  editorSection.appendChild(patternInput);
  const testBtn = el('button', { className: 'btn' }, 'Test');
  testBtn.onclick = () => testCustomPattern(field);
  editorSection.appendChild(testBtn);
  editorSection.appendChild(el('div', { id: 'custom-pattern-result' }));
  body.appendChild(editorSection);

  modal.classList.remove('hidden');
}

function closePatternModal() {
  document.getElementById('pattern-modal').classList.add('hidden');
}

async function approvePattern(pattern, field, sourceProduct) {
  try {
    const result = await api('/patterns/approve', {
      method: 'POST',
      body: { pattern, field, source_product: sourceProduct },
    });
    closePatternModal();
    alert(`Pattern approved! Re-extracted ${result.re_extracted} products.`);
    loadCoverage();
    route();
  } catch (e) { alert('Error: ' + e.message); }
}

async function testCustomPattern(field) {
  const pattern = document.getElementById('custom-pattern').value;
  if (!pattern) return;
  try {
    const result = await api('/patterns/test', { method: 'POST', body: { pattern, field } });
    const resultDiv = document.getElementById('custom-pattern-result');
    resultDiv.replaceChildren();
    const info = el('div', { className: 'pattern-result', style: { marginTop: '8px' } });
    if (!result.is_valid) {
      info.appendChild(el('span', { style: { color: 'var(--red)' } }, 'Invalid regex!'));
    } else {
      info.textContent = `${result.new_match_count} new / ${result.total_matches} total`;
      if (result.new_match_count > 0) {
        info.appendChild(el('br', {}));
        info.appendChild(el('span', { className: 'match-list' }, 'New: ' + result.new_matches.slice(0, 8).join(', ')));
        const approveBtn = el('button', { className: 'btn btn-primary', style: { marginTop: '8px', display: 'block' } }, 'Approve');
        approveBtn.onclick = () => approvePattern(pattern, field, 'manual');
        info.appendChild(approveBtn);
      }
    }
    resultDiv.appendChild(info);
  } catch (e) {
    const resultDiv = document.getElementById('custom-pattern-result');
    resultDiv.replaceChildren(el('span', { style: { color: 'var(--red)' } }, e.message));
  }
}

// ─── Coverage Dashboard ─────────────────────────────────────────────────

async function renderCoverage(container) {
  setContent(container, el('p', {}, 'Loading coverage...'));
  try {
    const cov = await api('/coverage');
    const wrapper = el('div', {});
    wrapper.appendChild(el('h2', { style: { marginBottom: '16px' } }, `Extraction Coverage (${cov.total} labels)`));
    wrapper.appendChild(el('p', { style: { marginBottom: '16px', color: 'var(--charcoal-light)' } },
      `${cov.verified} verified \u00b7 ${cov.learned_patterns} learned patterns`));

    const grid = el('div', { className: 'coverage-grid' });
    const sorted = Object.entries(cov.fields).sort((a, b) => b[1].pct - a[1].pct);
    for (const [field, data] of sorted) {
      grid.appendChild(el('div', { className: 'coverage-label' }, field.replace(/_/g, ' ')));
      const bar = el('div', { className: 'coverage-bar' });
      bar.appendChild(el('div', { className: 'coverage-fill', style: { width: data.pct + '%' } }));
      grid.appendChild(bar);
      grid.appendChild(el('div', { className: 'coverage-pct' }, `${data.count}/${data.total} (${data.pct}%)`));
    }
    wrapper.appendChild(grid);
    setContent(container, wrapper);
  } catch (e) {
    setContent(container, el('p', { style: { color: 'var(--red)' } }, 'Error: ' + e.message));
  }
}

// ─── Patterns View ──────────────────────────────────────────────────────

async function renderPatterns(container) {
  try {
    const patterns = await api('/patterns');
    const wrapper = el('div', {});
    wrapper.appendChild(el('h2', { style: { marginBottom: '16px' } }, 'Learned Patterns'));
    const fields = Object.keys(patterns);
    if (fields.length === 0) {
      wrapper.appendChild(el('p', { style: { color: 'var(--grey-500)' } },
        'No patterns learned yet. Review products and annotate missed data to teach the system.'));
    }
    for (const field of fields) {
      wrapper.appendChild(el('h3', { style: { marginTop: '16px', marginBottom: '8px' } }, field.replace(/_/g, ' ')));
      for (const p of patterns[field]) {
        const patDiv = el('div', { className: 'pattern-text' });
        patDiv.textContent = p.pattern;
        wrapper.appendChild(patDiv);
        wrapper.appendChild(el('div', { style: { fontSize: '11px', color: 'var(--grey-500)' } },
          `Added: ${(p.added_at || '?').substring(0, 10)} \u00b7 Matches: ${p.test_results?.total_matches || '?'} \u00b7 Status: ${p.status}`));
      }
    }
    setContent(container, wrapper);
  } catch (e) {
    setContent(container, el('p', { style: { color: 'var(--red)' } }, 'Error: ' + e.message));
  }
}

// ─── ACVM Match Review ──────────────────────────────────────────────────
// Lists catalogue products without a matched P-number (plus any currently
// under an override) and lets the user force a specific P-number or block
// the product from fuzzy matching. Writes to acvm_overrides.json — takes
// effect on the next run of the `acvm` pipeline stage.

let acvmData = null;

async function renderAcvmReview(container) {
  setContent(container, el('p', {}, 'Loading ACVM data...'));
  let data;
  try { data = await api('/acvm/unmatched'); }
  catch (e) { setContent(container, el('p', { style: { color: 'var(--red)' } }, 'Error: ' + e.message)); return; }
  acvmData = data;

  if (!data.available) {
    setContent(container, el('div', {},
      el('h2', {}, 'ACVM Match Review'),
      el('p', { style: { marginTop: '12px' } },
        'ACVM register not loaded. Run the ACVM pipeline stage to fetch it:'),
      el('pre', { style: { background: 'var(--grey-100)', padding: '12px', marginTop: '8px' } },
        'python scripts/run_pipeline.py --stages acvm')));
    return;
  }

  const wrapper = el('div', {});
  const header = el('div', { className: 'product-list-header' });
  header.appendChild(el('h2', {}, 'ACVM Match Review'));
  header.appendChild(el('div', { className: 'acvm-sub' },
    `${data.total_unmatched} unmatched of ${data.total_products} products. ` +
    `Click Rebuild to apply any saved overrides.`));
  const rebuildBtn = el('button', { className: 'btn btn-primary' }, 'Rebuild catalogue');
  rebuildBtn.onclick = () => triggerRebuild();
  header.appendChild(rebuildBtn);
  wrapper.appendChild(header);

  // Split into three groups for clarity.
  const groups = { unmatched: [], forced: [], blocked: [] };
  for (const u of data.unmatched) {
    if (u.override?.type === 'force') groups.forced.push(u);
    else if (u.override?.type === 'block') groups.blocked.push(u);
    else groups.unmatched.push(u);
  }

  if (groups.unmatched.length) {
    wrapper.appendChild(el('h3', { className: 'acvm-group-title' },
      `Needs review (${groups.unmatched.length})`));
    for (const u of groups.unmatched) wrapper.appendChild(renderAcvmRow(u));
  }
  if (groups.forced.length) {
    wrapper.appendChild(el('h3', { className: 'acvm-group-title' },
      `Forced matches (${groups.forced.length})`));
    for (const u of groups.forced) wrapper.appendChild(renderAcvmRow(u));
  }
  if (groups.blocked.length) {
    wrapper.appendChild(el('h3', { className: 'acvm-group-title' },
      `Blocked (${groups.blocked.length})`));
    for (const u of groups.blocked) wrapper.appendChild(renderAcvmRow(u));
  }
  if (data.unmatched.length === 0) {
    wrapper.appendChild(el('p', { style: { marginTop: '24px', color: 'var(--grey-500)' } },
      'All products matched. Nothing to review.'));
  }

  setContent(container, wrapper);
}

function renderAcvmRow(u) {
  const row = el('div', { className: 'acvm-row', 'data-slug': u.slug });

  // Header
  const head = el('div', { className: 'acvm-row-head' });
  head.appendChild(el('div', { className: 'acvm-name' }, u.name));
  head.appendChild(el('div', { className: 'acvm-section' }, u.section));
  if (u.override) {
    const badge = u.override.type === 'force'
      ? el('span', { className: 'badge badge-high' }, `Forced: ${u.override.p_number}`)
      : el('span', { className: 'badge badge-medium' }, 'Blocked');
    head.appendChild(badge);
  }
  row.appendChild(head);

  // Current override detail + clear button
  if (u.override) {
    const ovDiv = el('div', { className: 'acvm-override-info' });
    if (u.override.type === 'force') {
      ovDiv.appendChild(el('span', {}, `Forced to match: ${u.override.trade_name || '(unknown)'} (${u.override.p_number})`));
    } else {
      ovDiv.appendChild(el('span', {}, `Reason: ${u.override.reason}`));
    }
    const clearBtn = el('button', { className: 'btn' }, 'Clear override');
    clearBtn.onclick = () => applyOverride(u.slug, { action: 'clear' });
    ovDiv.appendChild(clearBtn);
    row.appendChild(ovDiv);
  }

  // Suggestions table
  if (u.suggestions.length) {
    const sug = el('div', { className: 'acvm-suggestions' });
    sug.appendChild(el('div', { className: 'acvm-suggestions-title' }, 'Fuzzy-match suggestions:'));
    for (const s of u.suggestions) {
      const srow = el('div', { className: 'acvm-suggestion' });
      const info = el('div', { className: 'acvm-suggestion-info' });
      info.appendChild(el('span', { className: 'acvm-score' }, `${s.score}%`));
      info.appendChild(el('span', { className: 'acvm-suggestion-name' }, s.trade_name));
      info.appendChild(el('span', { className: 'acvm-meta' },
        `${s.p_number} · ${s.product_type} · ${s.registrant}`));
      srow.appendChild(info);
      const forceBtn = el('button', { className: 'btn btn-primary' }, 'Force match');
      forceBtn.onclick = () => applyOverride(u.slug, { action: 'force', p_number: s.p_number });
      srow.appendChild(forceBtn);
      sug.appendChild(srow);
    }
    row.appendChild(sug);
  }

  // Manual P-number entry
  const manual = el('div', { className: 'acvm-manual' });
  manual.appendChild(el('span', { className: 'acvm-manual-label' }, 'Or enter a P-number manually:'));
  const pInput = el('input', { type: 'text', className: 'acvm-pnum-input', placeholder: 'P0xxxxx', maxlength: '8' });
  const applyBtn = el('button', { className: 'btn' }, 'Apply');
  applyBtn.onclick = async () => {
    const p = pInput.value.trim().toUpperCase();
    if (!p) return;
    try {
      await api(`/acvm/product/${p}`);
      await applyOverride(u.slug, { action: 'force', p_number: p });
    } catch (e) {
      alert(`${p} is not in the ACVM register`);
    }
  };
  manual.append(pInput, applyBtn);
  row.appendChild(manual);

  // Block + Split actions row
  const actionsRow = el('div', { className: 'acvm-row-actions' });
  if (u.override?.type !== 'block') {
    const blockBtn = el('button', { className: 'btn btn-wrong' }, 'Block this product');
    blockBtn.onclick = async () => {
      const reason = prompt(`Why block "${u.name}" from ACVM matching?\n(e.g. "Withdrawn from register" — saved to acvm_overrides.json)`);
      if (reason === null || reason.trim() === '') return;
      await applyOverride(u.slug, { action: 'block', reason: reason.trim() });
    };
    actionsRow.appendChild(blockBtn);
  }
  const splitBtn = el('button', { className: 'btn' }, 'Split into separate products');
  splitBtn.onclick = () => openSplitDialog(u);
  actionsRow.appendChild(splitBtn);
  row.appendChild(actionsRow);

  return row;
}

// ─── Split-product dialog ──────────────────────────────────────────────
// When the PDF parser merges two products into one cell (two text lines
// concatenated with a space instead of a semicolon), the user tells the
// assembler how to split them. Writes to data/corrections/product_splits.json
// and takes effect next time the assemble stage runs.

function openSplitDialog(product) {
  // Best-guess initial split: break at `] ` boundaries which usually separate
  // `Name [Registrant] Name [Registrant]` patterns.
  const parts = product.name.split(/\]\s+(?=[A-Z])/);
  const guess = parts.map((part, i) => i < parts.length - 1 ? part + ']' : part);

  let modal = document.getElementById('split-modal');
  if (!modal) {
    modal = el('div', { id: 'split-modal', className: 'modal' });
    document.body.appendChild(modal);
  }
  modal.classList.remove('hidden');
  modal.replaceChildren();

  const content = el('div', { className: 'modal-content' });
  const header = el('div', { className: 'modal-header' });
  header.appendChild(el('h3', {}, 'Split product into multiple'));
  const close = el('button', { className: 'modal-close' }, '\u00d7');
  close.onclick = () => modal.classList.add('hidden');
  header.appendChild(close);
  content.appendChild(header);

  const body = el('div', { style: { padding: '16px' } });
  body.appendChild(el('p', { className: 'split-note' },
    `The schedule PDF had "${product.name}" as a single entry, but it\u2019s actually multiple products. ` +
    `Enter each on its own line below. Takes effect next time the assemble pipeline stage runs.`));

  const textarea = el('textarea', {
    className: 'split-textarea',
    rows: '6',
    placeholder: 'One product name per line\ne.g.\nKnock out Extra [AgStar]\nLion 490DST [Nufarm]',
  });
  textarea.value = guess.filter(Boolean).join('\n');
  body.appendChild(textarea);

  const btnRow = el('div', { className: 'split-btn-row' });
  const saveBtn = el('button', { className: 'btn btn-primary' }, 'Save & rebuild');
  saveBtn.onclick = async () => {
    const names = textarea.value.split('\n').map(s => s.trim()).filter(Boolean);
    if (names.length < 2) { alert('Enter at least 2 product names'); return; }
    try {
      await api('/product-splits', { method: 'POST', body: { slug: product.slug, names } });
      modal.classList.add('hidden');
      triggerRebuild();
    } catch (e) { alert('Failed: ' + e.message); }
  };
  const cancelBtn = el('button', { className: 'btn' }, 'Cancel');
  cancelBtn.onclick = () => modal.classList.add('hidden');
  btnRow.append(saveBtn, cancelBtn);
  body.appendChild(btnRow);
  content.appendChild(body);
  modal.appendChild(content);

  setTimeout(() => textarea.focus(), 0);
}

async function applyOverride(slug, body) {
  try {
    await api('/acvm/override', { method: 'POST', body: { slug, ...body } });
    showToast(`Override saved for ${slug}`, null);
    renderAcvmReview(document.getElementById('app'));
  } catch (e) {
    alert('Failed: ' + e.message);
  }
}

// ─── Catalogue rebuild (apply overrides) ────────────────────────────────
// Runs the assemble + ACVM-match stages in-process. Uses the cached ACVM
// register — no network. Completes in ~5s. Used after saving a product
// split or ACVM override to make the change take effect without CLI.

let rebuildPoller = null;

async function triggerRebuild() {
  try {
    const res = await api('/catalogue/rebuild', { method: 'POST' });
    if (!res.ok) {
      alert(`Rebuild not started: ${res.reason}`);
      return;
    }
    startRebuildPolling();
  } catch (e) { alert('Rebuild failed to start: ' + e.message); }
}

function startRebuildPolling() {
  if (rebuildPoller) return;
  showRebuildBanner({ running: true, phase: 'assembling', message: 'Starting rebuild...' });
  rebuildPoller = setInterval(async () => {
    try {
      const status = await api('/catalogue/rebuild/status');
      showRebuildBanner(status);
      if (!status.running) {
        clearInterval(rebuildPoller);
        rebuildPoller = null;
        if (status.phase === 'done') {
          setTimeout(() => {
            hideRebuildBanner();
            // Refresh whichever page is open — the catalogue is new.
            route();
            loadCoverage();
            showToast(status.message || 'Catalogue rebuilt', null);
          }, 800);
        }
      }
    } catch (e) { /* retry */ }
  }, 600);
}

function showRebuildBanner(status) {
  let banner = document.getElementById('rebuild-banner');
  if (!banner) {
    banner = el('div', { id: 'rebuild-banner', className: 'bootstrap-banner' });
    document.body.insertBefore(banner, document.getElementById('app'));
  }
  banner.replaceChildren();
  if (status.running) {
    banner.appendChild(el('div', { className: 'bootstrap-title' },
      status.message || 'Rebuilding catalogue...'));
    banner.appendChild(el('div', { className: 'bootstrap-sub' },
      `Phase: ${status.phase}`));
    const bar = el('div', { className: 'bootstrap-bar' });
    bar.appendChild(el('div', { className: 'bootstrap-fill', style: { width: '100%', animation: 'pulse 1.2s ease-in-out infinite' } }));
    banner.appendChild(bar);
  } else if (status.phase === 'error') {
    banner.appendChild(el('div', { className: 'bootstrap-title bootstrap-error' }, 'Rebuild failed'));
    banner.appendChild(el('div', { className: 'bootstrap-sub' }, status.error || 'Unknown error'));
    const btn = el('button', { className: 'btn btn-primary' }, 'Dismiss');
    btn.onclick = hideRebuildBanner;
    banner.appendChild(btn);
  }
}

function hideRebuildBanner() {
  const banner = document.getElementById('rebuild-banner');
  if (banner) banner.remove();
}

// ─── Dashboard ──────────────────────────────────────────────────────────
// Landing page. Pulls together stat cards, the pipeline runner, the label
// freshness table, and a validation button. Everything that's "run stuff"
// or "see top-line numbers" lives here.

const PIPELINE_STAGES = [
  { key: 'parse',    label: 'Parse',    desc: 'Re-parse the schedule PDF' },
  { key: 'assemble', label: 'Assemble', desc: 'Rebuild catalogue from staging' },
  { key: 'acvm',     label: 'ACVM',     desc: 'Match (and optionally download new labels)' },
  { key: 'labels',   label: 'Labels',   desc: 'Re-extract text + fields from label PDFs' },
  { key: 'diff',     label: 'Diff',     desc: 'Compare to a previous season' },
];

let pipelineSse = null;

async function renderDashboard(container) {
  setContent(container, el('p', {}, 'Loading...'));
  let stats;
  try { stats = await api('/dashboard'); }
  catch (e) { setContent(container, el('p', { style: { color: 'var(--red)' } }, 'Error: ' + e.message)); return; }

  const wrapper = el('div', {});
  wrapper.appendChild(el('h2', { style: { marginBottom: '12px' } }, 'Dashboard'));

  const grid = el('div', { className: 'stat-grid' });
  const cards = [
    { label: 'Products', value: stats.products_total, sub: `${stats.products_unmatched_acvm} unmatched` },
    { label: 'Labels', value: stats.labels_total, sub: `${stats.labels_with_text} extracted` },
    { label: 'Extraction coverage', value: stats.extraction_coverage_pct + '%', sub: `${stats.reviewed} products reviewed` },
    { label: 'Outdated labels', value: stats.labels_outdated, sub: '>180 days' },
    { label: 'Learned patterns', value: stats.learned_patterns, sub: 'approved regexes' },
    { label: 'Last pipeline', value: stats.last_pipeline_status || 'never', sub: stats.last_pipeline_run ? stats.last_pipeline_run.substring(0, 16).replace('T', ' ') : '—' },
  ];
  for (const c of cards) {
    const card = el('div', { className: 'stat-card' });
    card.appendChild(el('div', { className: 'stat-label' }, c.label));
    card.appendChild(el('div', { className: 'stat-value' }, String(c.value)));
    card.appendChild(el('div', { className: 'stat-sub' }, c.sub));
    grid.appendChild(card);
  }
  wrapper.appendChild(grid);

  wrapper.appendChild(renderPipelineRunner());

  const bottomRow = el('div', { className: 'dashboard-bottom' });
  bottomRow.appendChild(renderFreshnessSection());
  bottomRow.appendChild(renderValidationSection());
  wrapper.appendChild(bottomRow);

  setContent(container, wrapper);
}

function renderPipelineRunner() {
  const section = el('div', { className: 'dash-section' });
  section.appendChild(el('h3', {}, 'Pipeline'));

  const controls = el('div', { className: 'pipeline-controls' });

  const stagesRow = el('div', { className: 'stage-row' });
  for (const s of PIPELINE_STAGES) {
    const label = el('label', { className: 'stage-chip' });
    const cb = el('input', { type: 'checkbox', value: s.key, id: `stage-${s.key}` });
    if (s.key !== 'diff') cb.checked = true;
    label.append(cb, document.createTextNode(' ' + s.label));
    label.title = s.desc;
    stagesRow.appendChild(label);
  }
  controls.appendChild(stagesRow);

  const options = el('div', { className: 'pipeline-options' });
  const forceLabel = el('label', {});
  const forceCb = el('input', { type: 'checkbox', id: 'opt-force' });
  forceLabel.append(forceCb, document.createTextNode(' Force re-run (ignore source-hash idempotency)'));
  options.appendChild(forceLabel);
  const dlLabel = el('label', {});
  const dlCb = el('input', { type: 'checkbox', id: 'opt-download' });
  dlLabel.append(dlCb, document.createTextNode(' Download new label PDFs (acvm stage, network)'));
  options.appendChild(dlLabel);
  controls.appendChild(options);

  const runBtn = el('button', { className: 'btn btn-primary', id: 'pipeline-run-btn' }, 'Run pipeline');
  runBtn.onclick = () => startPipelineRun();
  controls.appendChild(runBtn);

  section.appendChild(controls);

  const statusStrip = el('div', { className: 'pipeline-status', id: 'pipeline-status' });
  section.appendChild(statusStrip);
  const logPane = el('pre', { className: 'pipeline-log', id: 'pipeline-log' });
  section.appendChild(logPane);
  api('/pipeline/status').then(st => updatePipelineStatusUi(st));

  return section;
}

async function startPipelineRun() {
  const stages = PIPELINE_STAGES
    .map(s => s.key)
    .filter(k => document.getElementById(`stage-${k}`)?.checked);
  if (stages.length === 0) { alert('Select at least one stage'); return; }
  const force = !!document.getElementById('opt-force')?.checked;
  const download_labels = !!document.getElementById('opt-download')?.checked;

  const log = document.getElementById('pipeline-log');
  if (log) log.textContent = '';

  try {
    const res = await api('/pipeline/run', { method: 'POST', body: { stages, force, download_labels } });
    if (!res.ok) { alert(`Cannot start: ${res.reason}`); return; }
    subscribePipelineStream();
  } catch (e) { alert('Failed to start: ' + e.message); }
}

function subscribePipelineStream() {
  if (pipelineSse) { try { pipelineSse.close(); } catch (e) {} }
  pipelineSse = new EventSource('/api/pipeline/stream');
  pipelineSse.onmessage = (ev) => {
    const log = document.getElementById('pipeline-log');
    if (!log) return;
    log.textContent += ev.data.replaceAll('\\n', '\n') + '\n';
    log.scrollTop = log.scrollHeight;
  };
  pipelineSse.addEventListener('end', async () => {
    pipelineSse.close();
    pipelineSse = null;
    const st = await api('/pipeline/status').catch(() => null);
    if (st) updatePipelineStatusUi(st);
    setTimeout(() => renderDashboard(document.getElementById('app')), 400);
  });
  pipelineSse.onerror = () => {
    pipelineSse?.close();
    pipelineSse = null;
  };
  const statusPoller = setInterval(async () => {
    if (!pipelineSse) { clearInterval(statusPoller); return; }
    const st = await api('/pipeline/status').catch(() => null);
    if (st) updatePipelineStatusUi(st);
  }, 2000);
}

function updatePipelineStatusUi(st) {
  const strip = document.getElementById('pipeline-status');
  const btn = document.getElementById('pipeline-run-btn');
  if (!strip) return;
  strip.replaceChildren();
  const requested = st.stages_requested || [];
  const done = new Set(st.stages_completed || []);
  const errored = new Set(st.stages_errored || []);
  const stages = requested.length ? requested : PIPELINE_STAGES.map(x => x.key);
  for (const s of stages) {
    let cls = 'stage-dot';
    if (errored.has(s)) cls += ' dot-error';
    else if (done.has(s)) cls += ' dot-done';
    else if (st.phase === s && st.running) cls += ' dot-running';
    strip.appendChild(el('span', { className: cls, title: s }, s));
  }
  if (btn) {
    btn.disabled = !!st.running;
    btn.textContent = st.running ? 'Running...' : 'Run pipeline';
  }
}

function renderFreshnessSection() {
  const section = el('div', { className: 'dash-section' });
  section.appendChild(el('h3', {}, 'Label freshness'));
  const tableWrap = el('div', { id: 'freshness-table' });
  tableWrap.textContent = 'Loading...';
  section.appendChild(tableWrap);
  api('/labels/freshness').then(data => {
    tableWrap.replaceChildren();
    const sub = el('p', { className: 'dash-sub' },
      `${data.outdated} of ${data.total} labels last checked more than ${data.threshold_days} days ago.`);
    tableWrap.appendChild(sub);
    const table = el('table', { className: 'freshness-table' });
    const thead = el('thead', {});
    const hr = el('tr', {});
    for (const h of ['P-number', 'Trade name', 'Last checked', 'Age (days)']) hr.appendChild(el('th', {}, h));
    thead.appendChild(hr);
    table.appendChild(thead);
    const tbody = el('tbody', {});
    const rows = data.labels.filter(r => r.is_outdated);
    if (rows.length === 0) {
      const tr = el('tr', {});
      const td = el('td', { colspan: '4', style: { color: 'var(--grey-500)', padding: '16px' } }, 'No outdated labels.');
      tr.appendChild(td);
      tbody.appendChild(tr);
    } else {
      for (const r of rows) {
        const tr = el('tr', {});
        tr.appendChild(el('td', { className: 'mono' }, r.p_number));
        tr.appendChild(el('td', {}, r.trade_name || ''));
        tr.appendChild(el('td', {}, r.last_checked ? r.last_checked.substring(0, 10) : '—'));
        tr.appendChild(el('td', {}, r.age_days === null ? '—' : String(r.age_days)));
        tbody.appendChild(tr);
      }
    }
    table.appendChild(tbody);
    tableWrap.appendChild(table);
  }).catch(e => { tableWrap.textContent = 'Error: ' + e.message; });
  return section;
}

function renderValidationSection() {
  const section = el('div', { className: 'dash-section' });
  section.appendChild(el('h3', {}, 'Validation'));
  section.appendChild(el('p', { className: 'dash-sub' }, 'Cross-check referential integrity of the current catalogue.'));
  const runBtn = el('button', { className: 'btn btn-primary' }, 'Run validation');
  const results = el('div', { id: 'validation-results', style: { marginTop: '12px' } });
  runBtn.onclick = async () => {
    runBtn.disabled = true; runBtn.textContent = 'Running...';
    try {
      const res = await api('/validate', { method: 'POST' });
      results.replaceChildren();
      const summary = el('div', { style: { marginBottom: '8px' } },
        `${res.errors.length} error(s), ${res.warnings.length} warning(s) — ` +
        `${res.summary.total_products} products, ${res.summary.total_ais} AIs, ${res.summary.total_rm_rules} RM rules`);
      results.appendChild(summary);
      if (res.warnings.length) {
        const list = el('ul', { className: 'validation-list' });
        for (const w of res.warnings.slice(0, 50)) list.appendChild(el('li', {}, w));
        if (res.warnings.length > 50) list.appendChild(el('li', { style: { color: 'var(--grey-500)' } }, `(${res.warnings.length - 50} more...)`));
        results.appendChild(list);
      }
    } catch (e) { results.textContent = 'Error: ' + e.message; }
    finally { runBtn.disabled = false; runBtn.textContent = 'Run validation'; }
  };
  section.appendChild(runBtn);
  section.appendChild(results);
  return section;
}
