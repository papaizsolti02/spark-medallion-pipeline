$ErrorActionPreference = "Stop"

function Log-Info {
    param ([string]$Message)
    Write-Host "[INFO]  $Message" -ForegroundColor Cyan
}

function Log-Success {
    param ([string]$Message)
    Write-Host "[SUCCESS]  $Message" -ForegroundColor Green
}

function Log-Warn {
    param ([string]$Message)
    Write-Host "[WARN]  $Message" -ForegroundColor Yellow
}

function Log-Error {
    param ([string]$Message)
    Write-Host "[ERROR] $Message" -ForegroundColor Red
}

try {

    Log-Info "Starting environment setup..."

    if (Test-Path "venv") {
        Log-Warn "Existing virtual environment detected. Removing..."
        Remove-Item -Recurse -Force "venv"
        Log-Success "Old environment removed."
    }

    Log-Info "Creating new virtual environment..."
    python -m venv venv
    Log-Success "Virtual environment created."

    $pythonPath = ".\venv\Scripts\python.exe"
    $pipPath = ".\venv\Scripts\pip.exe"

    Log-Info "Upgrading pip..."
    & $pythonPath -m pip install --upgrade pip

    Log-Info "Installing project dependencies..."
    & $pipPath install -r requirements.txt

    Log-Success "Environment setup completed successfully."

    Write-Host ""
    Write-Host "Activate the environment using:" -ForegroundColor White
    Write-Host "    .\venv\Scripts\activate" -ForegroundColor Yellow
    Write-Host ""
}
catch {
    Log-Error "Environment setup failed."
    Log-Error $_
    exit 1
}