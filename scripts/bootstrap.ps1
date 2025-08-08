#Requires -Version 5
$ErrorActionPreference = "Stop"
Write-Host "🔧 Bootstrap (Windows)"

# 1) Descarga coursier si no está
if (-not (Get-Command cs -ErrorAction SilentlyContinue)) {
  Write-Host "⬇️  Descargando coursier (cs.exe)..."
  $csUrl = "https://github.com/coursier/launchers/raw/master/cs-x86_64-pc-win32.exe"
  $csExe = "$env:USERPROFILE\cs.exe"
  Invoke-WebRequest -Uri $csUrl -OutFile $csExe
  & $csExe setup -y --jvm temurin:11
  $csBin = "$env:USERPROFILE\AppData\Local\Coursier\data\bin"
  if ($env:PATH -notlike "*$csBin*") {
    [Environment]::SetEnvironmentVariable("PATH", "$env:PATH;$csBin", "User")
    $env:PATH = "$env:PATH;$csBin"
  }
}

# 2) Instala sbt vía coursier
Write-Host "☕ Instalando sbt..."
cs install sbt | Out-Null

# 3) Hadoop dummy
$hadoopHome = "C:\hadoop-dummy"
New-Item -ItemType Directory -Force -Path $hadoopHome | Out-Null
[Environment]::SetEnvironmentVariable("HADOOP_HOME", $hadoopHome, "User")

# 4) Verificaciones
Write-Host "✅ Versiones:"
sbt -version
java -version

Write-Host "🎉 Bootstrap OK. Para ejecutar usa: .\run_compare.ps1"
