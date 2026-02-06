Write-Host "Blackbox Data Pro quickstart"

python -m venv .venv
& .\.venv\Scripts\Activate.ps1
python -m pip install -U pip
python -m pip install -e ".[pro]"

Write-Host "Starting Blackbox Pro (wizard)..."
blackbox-pro wizard
