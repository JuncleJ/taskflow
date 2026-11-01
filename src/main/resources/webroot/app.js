'use strict';
let token = sessionStorage.getItem('taskflow-token');
const streams = new Map();
const cards = new Map();
const busyTasks = new Set();
const $ = id => document.getElementById(id);
const terminal = task => ['COMPLETED', 'FAILED', 'CANCELLED'].includes(task.status);

function message(text) {
    $('message').textContent = text;
}

async function api(path, options = {}) {
    const response = await fetch(path, {
        ...options,
        headers: {'Content-Type': 'application/json', ...(token ? {Authorization: `Bearer ${token}`} : {}), ...options.headers}
    });
    const data = response.status === 204 ? null : await response.json();
    if (!response.ok) {
        if (response.status === 401 && !['/api/auth/login', '/api/auth/register'].includes(path)) logout();
        const error = new Error(data.error || 'Ошибка запроса');
        error.status = response.status;
        throw error;
    }
    return data;
}

function logout() {
    token = null;
    sessionStorage.removeItem('taskflow-token');
    streams.forEach(controller => controller.abort());
    streams.clear();
    cards.clear();
    busyTasks.clear();
    $('tasks').replaceChildren();
    $('auth').hidden = false;
    $('workspace').hidden = true;
    $('account').hidden = true;
    $('current-user').textContent = '';
}

async function authenticate(register) {
    if (!$('auth-form').reportValidity()) return;
    const buttons = $('auth-form').querySelectorAll('button');
    buttons.forEach(b => b.disabled = true);
    try {
        const data = await api(`/api/auth/${register ? 'register' : 'login'}`, {
            method: 'POST',
            body: JSON.stringify({login: $('login').value, password: $('password').value})
        });
        token = data.token;
        sessionStorage.setItem('taskflow-token', token);
        $('password').value = '';
        message('');
        await load();
    } catch (error) {
        message(error.message);
    } finally {
        buttons.forEach(b => b.disabled = false);
    }
}

function render(task) {
    let card = cards.get(task.id);
    if (!card) {
        card = document.createElement('article');
        const top = document.createElement('div');
        top.className = 'task-top';
        const title = document.createElement('strong');
        const status = document.createElement('span');
        status.className = 'status';
        top.append(title, status);
        const progress = document.createElement('progress');
        progress.max = 100;
        progress.setAttribute('aria-label', 'Прогресс задачи');
        const meta = document.createElement('p');
        meta.className = 'meta';
        const error = document.createElement('p');
        error.className = 'error';
        const actions = document.createElement('div');
        actions.className = 'actions task-actions';
        for (const [operation, label] of [['cancel', 'Отменить'], ['delete', 'Удалить']]) {
            const button = document.createElement('button');
            button.dataset.operation = operation;
            button.textContent = label;
            button.addEventListener('click', () => taskAction(task.id, operation));
            actions.append(button);
        }
        card.append(top, progress, meta, error, actions);
        cards.set(task.id, card);
        $('tasks').prepend(card);
    }
    card.querySelector('strong').textContent = task.type;
    card.querySelector('.status').textContent = `${task.status} · ${task.progress}%`;
    card.querySelector('progress').value = task.progress;
    const details = task.type === 'DEMO' ? ` · ${task.parameters?.durationSeconds ?? 5} с`
        : task.type === 'CPU_TASK' ? ` · limit: ${task.parameters?.limit?.toLocaleString('ru-RU')}` : '';
    card.querySelector('.meta').textContent = `${task.id} · ${new Date(task.createdAt).toLocaleString('ru-RU')}${details}`;
    card.querySelector('.error').textContent = task.error || '';
    card.querySelector('[data-operation="cancel"]').hidden = terminal(task);
    card.querySelector('[data-operation="delete"]').hidden = !terminal(task);
    card.querySelectorAll('button').forEach(button => button.disabled = busyTasks.has(task.id));
    $('empty').hidden = cards.size > 0;
}

function removeTask(id) {
    streams.get(id)?.abort();
    streams.delete(id);
    cards.get(id)?.remove();
    cards.delete(id);
    $('empty').hidden = cards.size > 0;
}

async function taskAction(id, operation) {
    if (busyTasks.has(id)) return;
    const actionToken = token;
    busyTasks.add(id);
    cards.get(id)?.querySelectorAll('button').forEach(button => button.disabled = true);
    try {
        const result = await api(`/api/tasks/${id}${operation === 'cancel' ? '/cancel' : ''}`,
            {method: operation === 'cancel' ? 'POST' : 'DELETE'});
        if (!token || token !== actionToken) return;
        if (operation === 'delete') removeTask(id); else render(result);
        message('');
    } catch (error) {
        if (token === actionToken) {
            if (error.status === 404) removeTask(id);
            message(error.message);
        }
    } finally {
        busyTasks.delete(id);
        cards.get(id)?.querySelectorAll('button').forEach(button => button.disabled = false);
    }
}

async function subscribe(id) {
    if (streams.has(id) || !token) return;
    const subscribedToken = token;
    const controller = new AbortController();
    streams.set(id, controller);
    let finished = false;
    try {
        const response = await fetch(`/api/tasks/${id}/events`, {
            headers: {Authorization: `Bearer ${token}`},
            signal: controller.signal
        });
        if (!response.ok) {
            if (response.status === 401) logout();
            if (response.status === 404) {
                finished = true;
                removeTask(id);
                return;
            }
            throw new Error('Не удалось подключиться к прогрессу');
        }
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        while (true) {
            const {value, done} = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, {stream: true});
            let boundary;
            while ((boundary = buffer.indexOf('\n\n')) >= 0) {
                const frame = buffer.slice(0, boundary);
                buffer = buffer.slice(boundary + 2);
                const line = frame.split('\n').find(line => line.startsWith('data: '));
                if (line && token === subscribedToken && !controller.signal.aborted) {
                    const task = JSON.parse(line.slice(6));
                    if (frame.split('\n').includes('event: deleted')) {
                        finished = true;
                        removeTask(task.id);
                    } else {
                        render(task);
                        finished = terminal(task);
                    }
                }
            }
        }
    } catch (error) {
        if (error.name !== 'AbortError') message('Связь прервана. Повторное подключение…');
    } finally {
        if (streams.get(id) === controller) streams.delete(id);
        if (!finished && token && !controller.signal.aborted) setTimeout(() => subscribe(id), 2000);
    }
}

async function load() {
    const requestedToken = token;
    const [user, tasks] = await Promise.all([api('/api/auth/me'), api('/api/tasks')]);
    if (!token || token !== requestedToken) return;
    $('auth').hidden = true;
    $('workspace').hidden = false;
    $('current-user').textContent = user.login;
    $('account').hidden = false;
    const visible = new Set(tasks.map(task => task.id));
    for (const id of cards.keys()) if (!visible.has(id)) removeTask(id);
    tasks.reverse().forEach(task => {
        render(task);
        if (!terminal(task)) subscribe(task.id);
    });
}

$('auth-form').addEventListener('submit', event => {
    event.preventDefault();
    authenticate(false);
});
$('register').addEventListener('click', () => authenticate(true));
$('logout').addEventListener('click', logout);
$('refresh').addEventListener('click', () => load().catch(error => message(error.message)));
document.querySelectorAll('[data-type]').forEach(button => button.addEventListener('click', async () => {
    const type = button.dataset.type;
    const input = type === 'DEMO' ? $('duration-seconds') : type === 'CPU_TASK' ? $('cpu-limit') : null;
    if (input && !input.reportValidity()) return;
    const request = {type};
    if (type === 'DEMO') request.durationSeconds = input.valueAsNumber;
    if (type === 'CPU_TASK') request.limit = input.valueAsNumber;
    button.disabled = true;
    try {
        const task = await api('/api/tasks', {method: 'POST', body: JSON.stringify(request)});
        render(task);
        subscribe(task.id);
        message('');
    } catch (error) {
        message(error.message);
    } finally {
        button.disabled = false;
    }
}));
if (token) load().catch(error => message(error.message));
