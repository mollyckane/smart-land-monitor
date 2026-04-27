/*
- this file is to make the webpage interactive with the other services




*/

//helper methods
function setOutput(id, html) {
    document.getElementById(id).innerHTML = html;
}

function appendOutput(id, html) {
    const el = document.getElementById(id);
    el.innerHTML += html;
    el.scrollTop = el.scrollHeight;
}

function formatJSON(obj) {
    return Object.entries(obj)
        .filter(([k]) => k !== 'done')
        .map(([k, v]) => `<div class="output-line"><span class="output-key">${k}:</span> <span class="output-val">${v}</span></div>`)
        .join('');
}

function setStatus(service, online) {
    const el = document.getElementById(`status-${service}`);
    const badge = el.querySelector('.badge');
    badge.className = `badge ${online ? 'badge-online' : 'badge-offline'}`;
    badge.textContent = online ? 'Online' : 'Offline';
}

//----- discover services --------
async function discoverServices() {
    setOutput('naming-output', '<p class="output-warn">🔍 Discovering services...</p>');

    // Set all to offline first
    setStatus('forest', false);
    setStatus('soil', false);
    setStatus('water', false);
    setStatus('naming', false);

    try {
        const res = await fetch('/services/list');
        const data = await res.json();

        if (data.error) {
            setOutput('naming-output', `<p class="output-err">Error: ${data.error}</p>`);
            return;
        }

        if (data.length === 0) {
            setOutput('naming-output', '<p class="output-warn">No services registered yet. Are your services running?</p>');
            return;
        }

        const html = data.map(s =>
            `<div class="output-line">✅ <span class="output-key">${s.name}</span> → <span class="output-val">${s.address}</span></div>`
        ).join('');

        setOutput('naming-output', html);
        setStatus('naming', true);

        // Update status badges
        data.forEach(s => {
            // Set any service not in the list to offline
            const foundNames = data.map(s => s.name);
            if (!foundNames.includes('ForestMonitor')) setStatus('forest', false);
            if (!foundNames.includes('SoilSensor')) setStatus('soil', false);
            if (!foundNames.includes('WaterMonitor')) setStatus('water', false);
            if (s.name === 'ForestMonitor') setStatus('forest', true);
            if (s.name === 'SoilSensor') setStatus('soil', true);
            if (s.name === 'WaterMonitor') setStatus('water', true);
        });

    } catch (err) {
        setOutput('naming-output', `<p class="output-err">Could not reach naming service: ${err.message}</p>`);
        setStatus('naming', false);
    }
}


