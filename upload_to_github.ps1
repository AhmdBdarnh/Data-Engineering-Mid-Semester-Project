Set-StrictMode -Version Latest

$RepoUrl = "https://github.com/AhmdBdarnh/Data-Engineering-Mid-Semester-Project.git"

if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    Write-Error "Git is not installed or not available on PATH. Install Git for Windows first: https://git-scm.com/download/win"
    exit 1
}

if (-not (Test-Path ".git")) {
    git init
}

git branch -M main
git remote remove origin 2>$null
git remote add origin $RepoUrl
git add .
git commit -m "Add mid-semester data engineering demo"
git push -u origin main
