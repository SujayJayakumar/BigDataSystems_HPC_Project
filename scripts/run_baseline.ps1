param(
  [ValidateSet('median','drop')][string]$Strategy = 'median',
  [int]$MaxFiles = 0
)
$ErrorActionPreference = 'Stop'
$arguments = @('compose','run','--rm','--service-ports','spark','/opt/spark/bin/spark-submit','--driver-memory','8g','--master','local[*]','/workspace/src/run_baseline.py','--strategy',$Strategy)
if ($MaxFiles -gt 0) { $arguments += @('--max-files', [string]$MaxFiles) }
docker @arguments
