'use strict';
let token = sessionStorage.getItem('taskflow-token');
const streams = new Map();
const cards = new Map();
const $ = id => document.getElementById(id);
const terminal = task => ['COMPLETED', 'FAILED'].includes(task.status);
function message(text) { $('message').textContent = text; }
async function api(path, options = {}) {
  const response = await fetch(path, { ...options, headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: `Bearer ${token}` } : {}), ...options.headers } });
  const data = await response.json();
  if (!response.ok) {
    if (response.status === 401 && !path.startsWith('/api/auth/')) logout();
    throw new Error(data.error || 'Ошибка запроса');
  }
  return data;
}
function logout() {
  token = null; sessionStorage.removeItem('taskflow-token');
  streams.forEach(controller => controller.abort()); streams.clear(); cards.clear(); $('tasks').replaceChildren();
  $('auth').hidden = false; $('workspace').hidden = true; $('logout').hidden = true;
}
async function authenticate(register) {
  if (!$('auth-form').reportValidity()) return;
  const buttons = $('auth-form').querySelectorAll('button'); buttons.forEach(b => b.disabled = true);
  try {
    const data = await api(`/api/auth/${register ? 'register' : 'login'}`, { method: 'POST', body: JSON.stringify({ login: $('login').value, password: $('password').value }) });
    token = data.token; sessionStorage.setItem('taskflow-token', token); $('password').value = ''; message(''); await load();
  } catch (error) { message(error.message); }
  finally { buttons.forEach(b => b.disabled = false); }
}
function render(task) {
  let card = cards.get(task.id);
  if (!card) {
    card = document.createElement('article');
    const top = document.createElement('div'); top.className = 'task-top';
    const title = document.createElement('strong'); const status = document.createElement('span'); status.className = 'status';
    top.append(title, status);
    const progress = document.createElement('progress'); progress.max = 100; progress.setAttribute('aria-label', 'Прогресс задачи');
    const meta = document.createElement('p'); meta.className = 'meta';
    const error = document.createElement('p'); error.className = 'error';
    card.append(top, progress, meta, error); cards.set(task.id, card); $('tasks').prepend(card);
  }
  card.querySelector('strong').textContent = task.type;
  card.querySelector('.status').textContent = `${task.status} · ${task.progress}%`;
  card.querySelector('progress').value = task.progress;
  card.querySelector('.meta').textContent = `${task.id} · ${new Date(task.createdAt).toLocaleString('ru-RU')}`;
  card.querySelector('.error').textContent = task.error || '';
  $('empty').hidden = cards.size > 0;
}
async function subscribe(id) {
  if (streams.has(id) || !token) return;
  const subscribedToken = token;
  const controller = new AbortController(); streams.set(id, controller);
  let finished = false;
  try {
    const response = await fetch(`/api/tasks/${id}/events`, { headers: { Authorization: `Bearer ${token}` }, signal: controller.signal });
    if (!response.ok) { if (response.status === 401) logout(); throw new Error('Не удалось подключиться к прогрессу'); }
    const reader = response.body.getReader(); const decoder = new TextDecoder(); let buffer = '';
    while (true) {
      const { value, done } = await reader.read(); if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let boundary;
      while ((boundary = buffer.indexOf('\n\n')) >= 0) {
        const frame = buffer.slice(0, boundary); buffer = buffer.slice(boundary + 2);
        const line = frame.split('\n').find(line => line.startsWith('data: '));
        if (line && token === subscribedToken && !controller.signal.aborted) { const task = JSON.parse(line.slice(6)); render(task); finished = terminal(task); }
      }
    }
  } catch (error) { if (error.name !== 'AbortError') message('Связь прервана. Повторное подключение…'); }
  finally {
    if (streams.get(id) === controller) streams.delete(id);
    if (!finished && token && !controller.signal.aborted) setTimeout(() => subscribe(id), 2000);
  }
}
async function load() {
  const requestedToken = token;
  const tasks = await api('/api/tasks');
  if (!token || token !== requestedToken) return;
  $('auth').hidden = true; $('workspace').hidden = false; $('logout').hidden = false;
  tasks.reverse().forEach(task => { render(task); if (!terminal(task)) subscribe(task.id); });
}
$('auth-form').addEventListener('submit', event => { event.preventDefault(); authenticate(false); });
$('register').addEventListener('click', () => authenticate(true));
$('logout').addEventListener('click', logout);
$('refresh').addEventListener('click', () => load().catch(error => message(error.message)));
document.querySelectorAll('[data-type]').forEach(button => button.addEventListener('click', async () => {
  button.disabled = true;
  try { const task = await api('/api/tasks', { method: 'POST', body: JSON.stringify({ type: button.dataset.type }) }); render(task); subscribe(task.id); message(''); }
  catch (error) { message(error.message); } finally { button.disabled = false; }
}));
if (token) load().catch(error => message(error.message));
