// DOM Elements
const filesContainer = document.getElementById('filesContainer');
const newBtn = document.getElementById('newFileBtn');
const createModal = document.getElementById('createModal');
const cancelBtn = document.getElementById('cancelBtn');
const submitBtn = document.getElementById('submitBtn');

// Fetch and render files
async function loadFiles() {
    try {
        const res = await fetch('/api/files');
        if (!res.ok) throw new Error("Failed to load files");
        const files = await res.json();
        renderFiles(files);
    } catch (err) {
        console.error(err);
        filesContainer.innerHTML = '<div style="padding: 16px; color: red;">Error loading files</div>';
    }
}

function getIconForFile(name) {
    if (name.endsWith('.txt') || name.endsWith('.md')) return { icon: 'bx-file', class: 'icon-doc' };
    if (name.endsWith('.png') || name.endsWith('.jpg')) return { icon: 'bx-image', class: 'icon-img' };
    if (name.endsWith('.py') || name.endsWith('.js') || name.endsWith('.html')) return { icon: 'bx-code-alt', class: 'icon-code' };
    return { icon: 'bx-file-blank', class: 'icon-doc' };
}

function renderFiles(files) {
    filesContainer.innerHTML = '';

    if (files.length === 0) {
        filesContainer.innerHTML = '<div style="padding: 24px; text-align: center; color: var(--text-inactive);">No files found in distributed system. Create one to get started!</div>';
        return;
    }

    files.forEach(file => {
        const iconData = getIconForFile(file.name);
        const fileEl = document.createElement('div');
        fileEl.className = 'file-row';
        fileEl.innerHTML = `
            <div class="file-name-col">
                <i class='bx ${iconData.icon} ${iconData.class}' style="color: var(--accent-primary);"></i>
                <span>${file.name}</span>
            </div>
            <div class="file-owner">Me</div>
            <div class="file-modified">${new Date(file.modified * 1000).toLocaleDateString()}</div>
            <div class="file-size">${(file.size / 1024).toFixed(1)} KB</div>
            <div class="file-actions">
                <button class="action-btn download-btn" title="Download" data-name="${file.name}"><i class='bx bx-download'></i></button>
                <button class="action-btn delete-btn" title="Delete" data-name="${file.name}"><i class='bx bx-trash'></i></button>
            </div>
        `;
        filesContainer.appendChild(fileEl);
    });

    // Add event listeners for download and delete
    document.querySelectorAll('.download-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            const name = e.currentTarget.dataset.name;
            window.open(`/api/download?name=${encodeURIComponent(name)}`, '_blank');
        });
    });

    document.querySelectorAll('.delete-btn').forEach(btn => {
        btn.addEventListener('click', async (e) => {
            const name = e.currentTarget.dataset.name;
            if (confirm(`Are you sure you want to delete ${name}?`)) {
                await fetch(`/api/files?name=${encodeURIComponent(name)}`, { method: 'DELETE' });
                loadFiles();
            }
        });
    });
}

// Interactivity for creation
newBtn.addEventListener('click', () => {
    createModal.classList.add('active');
});

cancelBtn.addEventListener('click', () => {
    createModal.classList.remove('active');
});

submitBtn.addEventListener('click', async () => {
    const name = document.getElementById('fileNameInput').value;
    const content = document.getElementById('fileContentInput').value;

    if (!name) return alert("Please enter a filename");

    // Show loading text
    submitBtn.innerText = "Creating...";
    submitBtn.disabled = true;

    try {
        const res = await fetch('/api/upload', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name, content })
        });
        if (res.ok) {
            createModal.classList.remove('active');
            document.getElementById('fileNameInput').value = '';
            document.getElementById('fileContentInput').value = '';
            loadFiles();
        } else {
            alert("Failed to create file");
        }
    } catch (err) {
        console.error(err);
        alert("Error creating file");
    } finally {
        submitBtn.innerText = "Create";
        submitBtn.disabled = false;
    }
});

// Sidebar active states
document.querySelectorAll('.nav-item').forEach(item => {
    item.addEventListener('click', (e) => {
        e.preventDefault();
        document.querySelectorAll('.nav-item').forEach(nav => nav.classList.remove('active'));
        item.classList.add('active');
    });
});

// Init
document.addEventListener('DOMContentLoaded', () => {
    // Check if grid/list toggle exists, avoid error if removed
    const gridViewBtn = document.getElementById('gridViewBtn');
    const listViewBtn = document.getElementById('listViewBtn');
    if (gridViewBtn) gridViewBtn.addEventListener('click', () => { gridViewBtn.classList.add('active'); listViewBtn.classList.remove('active'); });
    if (listViewBtn) listViewBtn.addEventListener('click', () => { listViewBtn.classList.add('active'); gridViewBtn.classList.remove('active'); });

    loadFiles();
});
