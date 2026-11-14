package main

// admin_reports_page.go — the page where reports are read and decided:
// /admin/reports, behind the admin sign-in (moderation.go says what each
// decision does).
//
// Everything on it was typed by somebody who is not the admin: the
// comment, the note with the report, the username. So, like the dashboard,
// the page shows all of it as plain text and runs only its own script.

import (
	"fmt"
	"net/http"
)

// AdminReportsPageHandler serves the review page.
func AdminReportsPageHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Security-Policy", adminReportsRules)
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Cache-Control", "no-store")
	fmt.Fprint(w, adminReportsHTML)
}

// adminReportsRules: the page's own script only. Thumbnails come from
// wherever the videos are kept, so pictures may come from any https
// address — pictures cannot run anything.
var adminReportsRules = "default-src 'none'; " +
	"script-src " + scriptFingerprint(adminReportsHTML) + "; " +
	"style-src 'unsafe-inline'; connect-src 'self'; img-src https:; " +
	"base-uri 'none'; form-action 'none'; frame-ancestors 'none'"

const adminReportsHTML = `<!doctype html>
<html><head><meta charset="utf-8"><title>Flexete reports</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
body{font-family:system-ui,sans-serif;background:#0b0b0e;color:#eee;margin:0 auto;padding:24px 16px;max-width:900px}
h1{font-size:20px;color:#8ab4ff;margin:0 0 4px}
h2{margin-top:32px;font-size:15px;color:#ffb}
a{color:#8ab4ff}
.tiny{color:#8a8c95;font-size:12px}
.card{background:#15161b;border:1px solid #2a2b33;border-radius:10px;padding:12px 14px;margin:12px 0}
.card.urgent{border-color:#ff6b6b}
.head{display:flex;gap:8px;flex-wrap:wrap;align-items:baseline}
.kind{font-weight:700}
.tag{font-size:11px;padding:1px 7px;border-radius:9px;background:#2a2b33;color:#ccd}
.tag.red{background:#5a1d1d;color:#ffb3b3}
.text{white-space:pre-wrap;word-break:break-word;background:#0f1014;border-radius:6px;padding:8px;margin:8px 0}
.notes{margin:6px 0 0 18px;padding:0;color:#ccd;font-size:13px}
.thumb{max-width:120px;max-height:160px;border-radius:6px;display:block;margin:8px 0}
.buttons{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}
button{background:#22242c;color:#eee;border:1px solid #3a3c46;border-radius:8px;padding:7px 12px;font-size:13px;cursor:pointer}
button.bad{border-color:#7a2a2a;color:#ffb3b3}
button:disabled{opacity:.5;cursor:default}
table{border-collapse:collapse;width:100%;font-size:13px}
th,td{border:1px solid #2a2b33;padding:6px 8px;text-align:left;vertical-align:top}
</style></head><body>
<h1>Flexete — reports</h1>
<div class="tiny"><a href="/admin">Dashboard</a> · Oldest first; anything that may break the law within a day is marked and comes first. One decision closes every report on the same thing.</div>
<div id="summary" class="tiny"></div>
<div id="open">loading…</div>
<h2>Suspended accounts</h2>
<div id="suspended">loading…</div>
<script>
// Everything shown here was typed by somebody else, so it only ever goes on
// the page as plain text (append / textContent), never as HTML.
const KINDS = {challenge:'Video', response:'Answer video', comment:'Comment', user:'Account'};
const REASONS = {sexual_content:'Nudity or sexual content', child_safety:'A child at risk', violence:'Violence or threats',
  hate:'Hate', harassment:'Bullying or harassment', self_harm:'Suicide or self-harm', spam:'Spam or scams',
  impersonation:'Pretending to be someone', ip:"Uses someone else's work", other:'Something else',
  abuse:'Abuse', threats:'Threats', block:'Blocked'};
// India's rules give 24 hours for nudity and impersonation; a child at risk,
// self-harm and violence should not wait either.
const URGENT = ['child_safety','sexual_content','impersonation','self_harm','violence'];
function el(tag, cls, ...kids){
  const n = document.createElement(tag);
  if(cls) n.className = cls;
  for(const k of kids){ if(k !== null && k !== undefined) n.append(k instanceof Node ? k : String(k)); }
  return n;
}
function ago(iso){
  const h = (Date.now() - new Date(iso).getTime()) / 3600000;
  if(h < 1) return Math.max(1, Math.round(h*60)) + ' min';
  if(h < 48) return Math.round(h) + ' h';
  return Math.round(h/24) + ' days';
}
function httpsOnly(u){ return typeof u === 'string' && u.startsWith('https://') ? u : ''; }
async function send(path, body){
  const res = await fetch(path, {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)});
  if(!res.ok){ alert('That did not work: ' + (await res.text())); }
  load();
}
function decide(item, action){
  const who = item.owner ? '@' + item.owner : 'the account behind it';
  const ask = {
    dismiss: 'Close these reports and do nothing?',
    remove: 'Delete this ' + KINDS[item.targetType].toLowerCase() + ' for good?',
    suspend: 'Suspend ' + who + '? They are signed out, cannot sign in, and are hidden everywhere until you unsuspend them.',
    remove_suspend: 'Delete this for good AND suspend ' + who + '?',
  }[action];
  if(!confirm(ask)) return;
  let note = '';
  if(action === 'suspend' || action === 'remove_suspend'){ note = prompt('Why? (only you will see this)', '') || ''; }
  send('/api/v1/admin/reports/resolve', {targetType:item.targetType, targetId:item.targetId, action, note, confirm:'yes'});
}
function card(item){
  const urgent = (item.reasons||[]).some(r => URGENT.includes(r));
  const c = el('div', 'card' + (urgent ? ' urgent' : ''));
  const head = el('div', 'head', el('span', 'kind', KINDS[item.targetType] || item.targetType), el('span', 'tiny', '#' + item.targetId));
  if(item.owner) head.append(el('span', null, (item.targetType === 'user' ? '@' : 'by @') + item.owner));
  if(item.ownerSuspended) head.append(el('span', 'tag red', 'suspended'));
  if(urgent) head.append(el('span', 'tag red', 'urgent'));
  if(item.gone) head.append(el('span', 'tag', 'already deleted'));
  c.append(head);
  c.append(el('div', 'tiny', item.reports + (item.reports === 1 ? ' report' : ' reports') + ' · waiting ' + ago(item.firstAt) +
    ' · ' + (item.reasons||[]).map(r => REASONS[r] || r).join(', ')));
  if(item.text) c.append(el('div', 'text', item.text));
  const thumb = httpsOnly(item.thumbnailUrl);
  if(thumb){ const i = el('img', 'thumb'); i.src = thumb; i.alt = ''; c.append(i); }
  const video = httpsOnly(item.videoUrl);
  if(video){ const a = el('a', null, 'Open the video'); a.href = video; a.target = '_blank'; a.rel = 'noopener noreferrer'; c.append(a); }
  if((item.notes||[]).length){ const ul = el('ul', 'notes'); for(const n of item.notes) ul.append(el('li', null, n)); c.append(ul); }
  const b = el('div', 'buttons');
  const button = (label, action, cls) => { const x = el('button', cls, label); x.addEventListener('click', () => decide(item, action)); return x; };
  b.append(button('Nothing wrong', 'dismiss'));
  if(item.targetType !== 'user' && !item.gone) b.append(button('Remove', 'remove', 'bad'));
  if(item.ownerId && !item.ownerSuspended) b.append(button(item.targetType === 'user' ? 'Suspend' : 'Suspend @' + item.owner, 'suspend', 'bad'));
  if(item.targetType !== 'user' && !item.gone && item.ownerId && !item.ownerSuspended) b.append(button('Remove + suspend', 'remove_suspend', 'bad'));
  c.append(b);
  return c;
}
async function load(){
  let data;
  try{ data = await fetch('/api/v1/admin/reports').then(r => { if(!r.ok) throw new Error(r.status); return r.json(); }); }
  catch(e){ document.getElementById('open').textContent = 'The reports could not be loaded: ' + e; return; }
  const open = (data.open || []).slice().sort((a, b) => {
    const ua = (a.reasons||[]).some(r => URGENT.includes(r)), ub = (b.reasons||[]).some(r => URGENT.includes(r));
    if(ua !== ub) return ua ? -1 : 1;
    return new Date(a.firstAt) - new Date(b.firstAt);
  });
  const summary = document.getElementById('summary');
  summary.textContent = open.length ? open.length + ' waiting · the oldest for ' + ago(open.reduce((m, x) => x.firstAt < m ? x.firstAt : m, open[0].firstAt)) : 'Nothing waiting.';
  document.getElementById('open').replaceChildren(...open.map(card));
  const sus = data.suspended || [];
  if(!sus.length){ document.getElementById('suspended').replaceChildren(el('div', 'tiny', 'Nobody is suspended.')); return; }
  const t = el('table');
  t.append(el('tr', null, el('th', null, 'account'), el('th', null, 'since'), el('th', null, 'why'), el('th', null, '')));
  for(const s of sus){
    const undo = el('button', null, 'Unsuspend');
    undo.addEventListener('click', () => { if(confirm('Let @' + s.username + ' sign in again and show them again?')) send('/api/v1/admin/unsuspend', {userId: s.id}); });
    t.append(el('tr', null, el('td', null, '@' + s.username), el('td', null, ago(s.suspendedAt) + ' ago'), el('td', null, s.reason), el('td', null, undo)));
  }
  document.getElementById('suspended').replaceChildren(t);
}
load();
</script>
</body></html>`
