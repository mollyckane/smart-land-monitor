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
    setOutput('naming-output', '<p class="output-warn"><img src="images/search.png" class="icons-btn"> Discovering services...</p>');

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

        setStatus('naming', true);

        if (data.length === 0) {
            setOutput('naming-output', '<p class="output-warn">No services registered yet. Are your services running?</p>');
            return;
        }

        const html = data.map(s =>
            `<div class="output-line"><img src="images/green-tick.png" class="icons-btn"> <span class="output-key">${s.name}</span> → <span class="output-val">${s.address}</span></div>`
        ).join('');

        setOutput('naming-output', html);

        // Update status badges
        const foundNames = data.map(s => s.name);
        data.forEach(s => {
            // Set any service not in the list to offline
            if (!foundNames.includes('ForestMonitor')) setStatus('forest', false);
            if (!foundNames.includes('SoilSensor')) setStatus('soil', false);
            if (!foundNames.includes('WaterMonitor')) setStatus('water', false);
            if (s.name === 'ForestMonitor') setStatus('forest', true);
            if (s.name === 'SoilSensor') setStatus('soil', true);
            if (s.name === 'WaterMonitor') setStatus('water', true);
        });


    } catch (error) {
        setOutput('naming-output', `<p class="output-err">Could not reach naming service: ${error.message}</p>`);
        setStatus('naming', false);
    }
}

//-------- Forest Service ---------
// 1. Unary RPC: GetCurrentReading
async function getForestReading(){
    const location = document.getElementById('forest-location').value;

    try{
        const res = await fetch('/forest/reading/', {
            method: 'POST',
            headers: { 'Content-Type' : 'application/json'},
            body: JSON.stringify({ location })
        });

        const data = await res.json();
        setOutput('forest-output', formatJSON(data));   
    }
    catch(error){
        console.error(`Error processing reading for: ${location}`);
        setOutput('forest-output', `<p class="output-err">Error: ${error.message}</p>`)
    }
}

//2. Server Streaming RPC: StreamReadings
function streamForestReadings(){
    const location = document.getElementById('forest-location').value;
    setOutput('forest-output', '');
    let count=0;

    const source = new EventSource(`/forest/stream?location=${location}`);
    source.onmessage = (event) =>{
        try{
            const data = JSON.parse(event.data);
            if(data.done) { source.close(); return; }
            if (data.error) { 
                appendOutput('forest-output', `<p class="output-err">Error: ${data.error}</p>`);
                source.close();
                return;
            }
            count++;
            appendOutput('forest-output', `<p class="output-key">-----Reading ${count}-----</p>${formatJSON(data)}`);
        }
        catch(error){
            appendOutput('forest-output', `<p class="output-err">Error: ${error.message}</p>`);
            source.close();
        }
    };
    source.onerror = () => { source.close(); };
}

//3. Bidirectional Streaming RPC: MonitorAlertChannel
function startForestAlerts(){
    setOutput('forest-output-alerts', '');
    const source = new EventSource('/forest/alerts');

    source.onmessage = (event) => {
        try{
            const data = JSON.parse(event.data);
            if(data.error){
                appendOutput('forest-output-alerts', `<p class="output-err">Error: ${error.message}</p>`);
                source.close();
                return;
            }
            appendOutput('forest-output-alerts', formatJSON(data));
        }
        catch(error){
            appendOutput('forest-output-alerts', `<p class="output-err">Error: ${error.message}</p>`);
            source.close();
        }
    };
    source.onerror = () => { source.close(); };
}

async function sendForestAlertConfig(){
    const location = document.getElementById('forest-location-alerts').value;
    const humidity = parseFloat(document.getElementById('forest-humidity-threshold').value);
    const co2_threshold = parseFloat(document.getElementById('forest-co2-threshold').value);
    
    try{
        const res = await fetch ('/forest/alerts', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json'},
            body: JSON.stringify({ location, humidity_threshold: humidity, co2_threshold })
        });
        const data = await res.json();
        setOutput('forest-output-alerts', formatJSON(data));
    }
    catch(error){
        setOutput('forest-output-alerts', `<p class="output-err">Error: ${error.message}</p>`);
    }
}
//-------- Forest Service END ---------

//-------- Soil Service ---------
//1. Unary RPC: GetSoilStatus
async function getSoilStatus(){
    const zone_id = document.getElementById('soil-zone').value;

    try{
        const res=await fetch('/soil/status', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json'},
            body: JSON.stringify({ zone_id })
        });
        const data=await res.json();
        setOutput('soil-output', formatJSON(data));
    }
    catch(error){
        setOutput('soil-output', `<p class="output-err">Error: ${error.message}</p>`);
    }
}

//2. Server Streaming RPC: StreamZoneMonitor
function streamSoilZone(){
    const zone_id = document.getElementById('soil-zone').value;
    setOutput('soil-output', '');
    let count=0;

    const source= new EventSource(`/soil/stream?zone_id=${zone_id}`);
    source.onmessage = (event) => {
        try{
            const data = JSON.parse(event.data);
            if(data.done){ source.close(); return; }

            if(data.error){
                appendOutput('soil-output', `<p class="output-err">Error: ${data.error}</p>`);
                source.close();
                return;
            }
            count++;
            appendOutput('soil-output', `<p class="output-key">-----Reading ${count}-----</p>${formatJSON(data)}`);
        }
        catch(error){
            appendOutput('soil-output', `<p class="output-err">Error: ${error.message}</p>`);
            source.close();
        }
    };
    source.onerror = () => { source.close(); };
}

//3. Client Streaming RPC: UploadReadingHistory
async function uploadSoilHistory(){
    const zone_id = document.getElementById('soil-zone').value;

    try{
        const res=await fetch('/soil/upload', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json'},
            body: JSON.stringify({ zone_id })
        });
        const data=await res.json();
        setOutput('soil-output', formatJSON(data));
    }
    catch(error){
        setOutput('soil-output', `<p class="output-err">Error: ${error.message}</p>`)
    }
}
//-------- Soil Service END ---------

//-------- Water Service ---------
//1. Unary RPC: GetWaterQuality
async function getWaterQuality(){
    const source_id=document.getElementById('water-source').value;

    try{
        const res=await fetch('/water/quality', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json'},
            body: JSON.stringify({ source_id })
        });
        const data=await res.json();
        setOutput('water-output', formatJSON(data));
    }
    catch(error){
        setOutput('water-output', `<p class="output-err">Error: ${error.message}</p>`)
    }
}

//2. Server Streaming: StreamWaterLevels
function streamWaterLevels(){
    const source_id=document.getElementById('water-source').value;
    setOutput('water-output', '');
    let count=0;

    const source=new EventSource(`/water/stream?source_id=${source_id}`);
    
    source.onmessage = (event) => {
        try{
            const data=JSON.parse(event.data);
            if (data.done) { source.close(); return; }

            if (data.error) {
                appendOutput('water-output', `<p class="output-err">Error: ${data.error}</p>`);
                source.close();
                return;
            }
            count++;
            appendOutput('water-output', `<p class="output-key">-----Reading ${count}-----</p>${formatJSON(data)}`);
        }
        catch(error){
            appendOutput('water-output', `<p class="output-err">Error: ${error.message}</p>`);
            source.close();
        }
    };
    source.onerror = () => { source.close(); };
}

//helper methods for pollution report
const queuedReadings=[];

function addReading(){
    const source_id = document.getElementById('water-source-pollution').value;
    const pollutant_ppm = parseFloat(document.getElementById('water-pollutant').value);

    if(!source_id || isNaN(pollutant_ppm)){ return; }

    queuedReadings.push({ source_id, pollutant_ppm, timestamp: new Date().toISOString() });
    renderQueue();
}

function renderQueue(){
    const list= document.getElementById('report-queue');
    list.innerHTML = queuedReadings.map(r => `<li>${r.source_id} - ${r.pollutant_ppm}ppm`).join(''); 

    list.style.display = queuedReadings.length > 0 ? 'flex' : 'none';
}

//3. Client Streaming RPC: PollutionReport
async function reportPollutionEvent() {
    if(queuedReadings.length === 0) return;

    try {
        const res = await fetch('/water/pollution', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ readings: queuedReadings })
        });
        const data = await res.json();
        setOutput('water-output-pollution', formatJSON(data));
        queuedReadings.length=0;
        renderQueue();
    }
    catch (error) {
        setOutput('water-output-pollution', `<p class="output-err">Error: ${error.message}</p>`)
    }
}

//4. Bidirectional RPC: PollutionAlertChanel
function startWaterAlerts(){
    setOutput('water-output-alert', '');

    const source = new EventSource('/water/alerts');

    source.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            if (data.done) { source.close(); return; }

            if (data.error) {
                appendOutput('water-output-alert', `<p class="output-err">Error: ${data.error}</p>`);
                source.close();
                return;
            }
            appendOutput('water-output-alert', formatJSON(data));
        }
        catch (error) {
            appendOutput('water-output-alert', `<p class="output-err">Error: ${error.message}</p>`);
            source.close();
        }
    };
    source.onerror = () => { source.close(); };
}

async function sendWaterAlerts() {
    const source_id = document.getElementById('water-source-alert').value;
    const alert_threshold= parseFloat(document.getElementById('water-alert-threshold').value);

    try {
        const res = await fetch('/water/alerts', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ source_id, alert_threshold })
        });
        const data = await res.json();
        setOutput('water-output-alert', formatJSON(data));
    }
    catch (error) {
        setOutput('water-output-alert', `<p class="output-err">Error: ${error.message}</p>`)
    }
}