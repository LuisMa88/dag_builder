# Read .env file (if exists), parse lines as KEY=VALUE, and set environment variables.
# Supports comments (#) and ignores blank lines.
$envPath = Join-Path (Get-Location) '.env'
if (-Not (Test-Path -Path $envPath -PathType Leaf)) {
    Write-Verbose "No .env file found at $envPath; skipping import."
    return
}
Get-Content $envPath | ForEach-Object {
    $line = $_.Trim()
    if ([string]::IsNullOrWhiteSpace($line) -or $line.StartsWith('#')) {
        return
    }

    if ($line -match '^([^=\s]+)\s*=\s*(.*)$') {
        $name = $matches[1].Trim()
        $value = $matches[2].Trim()

        # Remove optional surrounding single/double quotes
        if (($value.StartsWith('"') -and $value.EndsWith('"')) -or ($value.StartsWith("'") -and $value.EndsWith("'"))) {
            $value = $value.Substring(1, $value.Length - 2)
        }

        # Set env var dynamically; cannot use $env:$name directly
        Set-Item -Path ("Env:" + $name) -Value $value
    }
    else {
        Write-Verbose "Skipping invalid .env line: '$line'"
    }
}
