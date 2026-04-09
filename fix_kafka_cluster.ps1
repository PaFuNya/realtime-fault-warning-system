#!/usr/bin/env pwsh
# Kafka 集群诊断与修复 - PowerShell 版

$NODES = @{
    "master"  = "192.168.45.100"
    "slave1" = "192.168.45.101"
    "slave2" = "192.168.45.102"
}
$Password = "123456"
$SSHUser = "root"

function Get-SSH {
    param($Node, $Cmd, $Timeout = 15)
    $ip = $NODES[$Node]
    $result = ssh -o StrictHostKeyChecking=no -o ConnectTimeout=$Timeout "$SSHUser@$ip" "$Cmd" 2>&1
    return $result
}

function Get-SSHRaw {
    param($Node, $Cmd, $Timeout = 15)
    $ip = $NODES[$Node]
    $result = ssh -o StrictHostKeyChecking=no -o ConnectTimeout=$Timeout "$SSHUser@$ip" $Cmd 2>&1
    return $result
}

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Kafka Cluster Fix Tool"
Write-Host "============================================================"
Write-Host "  Master : $($NODES.master)"
Write-Host "  Slave1 : $($NODES.slave1)"
Write-Host "  Slave2 : $($NODES.slave2)"
Write-Host "============================================================"

# STEP 1: Diagnose
Write-Host "`n============================================================" -ForegroundColor Yellow
Write-Host "  STEP 1: Diagnose"
Write-Host "============================================================"

Write-Host "`n--- /etc/hosts ---"
foreach ($node in $NODES.Keys) {
    Write-Host "`n[$node]:"
    $out = Get-SSH $node "cat /etc/hosts"
    $out | ForEach-Object { Write-Host "  $_" }
}

Write-Host "`n--- Kafka process ---"
foreach ($node in $NODES.Keys) {
    $out = Get-SSH $node "jps | grep Kafka"
    $status = if ($out -match "Kafka") { "[OK] Running" } else { "[NOT RUNNING]" }
    Write-Host "  [$node] $status"
}

Write-Host "`n--- Kafka listener config ---"
foreach ($node in $NODES.Keys) {
    Write-Host "`n[$node]:"
    $out = Get-SSH $node "grep -E '^listeners|^advertised.listeners|^broker.id' /opt/module/kafka/config/server.properties 2>/dev/null || echo '(none)'"
    $out | ForEach-Object { Write-Host "  $_" }
}

# STEP 2: Fix /etc/hosts
Write-Host "`n============================================================" -ForegroundColor Yellow
Write-Host "  STEP 2: Fix /etc/hosts on all nodes"
Write-Host "============================================================"

$hostsBlock = @"
192.168.45.100  master
192.168.45.101  slave1
192.168.45.102  slave2
"@

foreach ($node in $NODES.Keys) {
    Write-Host "`n[$node] updating /etc/hosts..."
    Get-SSH $node "cp /etc/hosts /etc/hosts.bak.$(date +%Y%m%d%H%M%S)"
    Get-SSHRaw $node "cat > /tmp/hosts_fix.sh << 'SCRIPT'
HOSTS_LINE=`"192.168.45.100  master`n192.168.45.101  slave1`n192.168.45.102  slave2`n`"
grep -v '^[^#]*192\.168\.45\.' /etc/hosts > /tmp/hosts.new
echo `"`$HOSTS_LINE`" >> /tmp/hosts.new
mv /tmp/hosts.new /etc/hosts
SCRIPT
bash /tmp/hosts_fix.sh"
    $verify = Get-SSH $node "grep '192.168.45' /etc/hosts"
    if ($verify -match "192.168.45.100" -and $verify -match "192.168.45.101" -and $verify -match "192.168.45.102") {
        Write-Host "  [OK] All IPs present" -ForegroundColor Green
    } else {
        Write-Host "  [WARN] Some IPs may be missing" -ForegroundColor Yellow
    }
}

# STEP 3: Fix Kafka config
Write-Host "`n============================================================" -ForegroundColor Yellow
Write-Host "  STEP 3: Fix Kafka listener config"
Write-Host "============================================================"

foreach ($node in $NODES.Keys) {
    Write-Host "`n[$node] fixing server.properties..."
    Get-SSH $node "cp /opt/module/kafka/config/server.properties /opt/module/kafka/config/server.properties.bak.$(date +%Y%m%d%H%M%S)"

    # Fix or add listeners and advertised.listeners
    Get-SSHRaw $node @"
sed -i '/^listeners=/d' /opt/module/kafka/config/server.properties
sed -i '/^advertised.listeners=/d' /opt/module/kafka/config/server.properties
echo "listeners=PLAINTEXT://0.0.0.0:9092" >> /opt/module/kafka/config/server.properties
echo "advertised.listeners=PLAINTEXT://${node}:9092" >> /opt/module/kafka/config/server.properties
"@

    $verify = Get-SSH $node "grep -E '^listeners|^advertised.listeners' /opt/module/kafka/config/server.properties"
    Write-Host "  Config: $verify" -ForegroundColor Gray
}

# STEP 4: Stop Kafka
Write-Host "`n============================================================" -ForegroundColor Yellow
Write-Host "  STEP 4: Stop Kafka on all nodes"
Write-Host "============================================================"

foreach ($node in $NODES.Keys) {
    Write-Host "  Stopping [$node]..."
    Get-SSH $node "cd /opt/module/kafka && bin/kafka-server-stop.sh 2>/dev/null; sleep 3"
}
Start-Sleep -Seconds 5
Write-Host "  [OK] All stopped" -ForegroundColor Green

# STEP 5: Start Kafka
Write-Host "`n============================================================" -ForegroundColor Yellow
Write-Host "  STEP 5: Start Kafka on all nodes"
Write-Host "============================================================"

$jobs = @()
foreach ($node in $NODES.Keys) {
    Write-Host "  Starting [$node]..."
    $job = Start-Job -ScriptBlock {
        param($n) ssh -o StrictHostKeyChecking=no "root@$($args[0])" "cd /opt/module/kafka && nohup bin/kafka-server-start.sh config/server.properties > kafka.log 2>&1 &"
    } -ArgumentList $NODES[$node]
    $jobs += $job
    Start-Sleep -Seconds 5
}

$jobs | Wait-Job | Out-Null
Write-Host "  [OK] All start commands sent, waiting 20s..." -ForegroundColor Green
Start-Sleep -Seconds 20

# STEP 6: Verify
Write-Host "`n============================================================" -ForegroundColor Yellow
Write-Host "  STEP 6: Verify cluster"
Write-Host "============================================================"

Write-Host "`nKafka processes:"
foreach ($node in $NODES.Keys) {
    $out = Get-SSH $node "jps | grep Kafka"
    $status = if ($out -match "Kafka") { "[OK]" } else { "[FAIL]" }
    Write-Host "  [$node] $status $out"
}

Write-Host "`nPort 9092 listening:"
foreach ($node in $NODES.Keys) {
    Write -NoNewline "  [$node]: "
    $out = Get-SSH $node "netstat -tlnp 2>/dev/null | grep 9092 || ss -tlnp | grep 9092"
    if ($out) { Write-Host "[OK]" -ForegroundColor Green; Write-Host "    $out" -ForegroundColor Gray }
    else { Write-Host "[NOT LISTENING]" -ForegroundColor Red }
}

Write-Host "`nZookeeper broker registration:"
$zkOut = Get-SSH "master" "/opt/module/kafka/bin/zookeeper-shell.sh localhost:2181 ls /brokers/ids 2>/dev/null"
Write-Host "  $zkOut"

Write-Host "`nBroker connectivity:"
foreach ($node in $NODES.Keys) {
    Write -NoNewline "  [$node] ${node}:9092 -> "
    $out = Get-SSH "master" "timeout 5 /opt/module/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server ${node}:9092 2>&1 | head -1"
    if ($out -match "broker.id" -or $out -match "ApiVersions") {
        Write-Host "[OK]" -ForegroundColor Green
    } else {
        Write-Host "[FAIL] $out" -ForegroundColor Red
    }
}

# STEP 7: Rebuild topics
Write-Host "`n============================================================" -ForegroundColor Yellow
Write-Host "  STEP 7: Rebuild Kafka topics (3 partitions)"
Write-Host "============================================================"

$topics = @("device_state", "sensor_metrics", "highfreq_sensor", "sensor_raw", "log_raw", "ChangeRecord", "Feature_log", "alert_topic", "device_metrics")

Write-Host "`nDeleting old topics..."
foreach ($topic in $topics) {
    $out = Get-SSH "master" "/opt/module/kafka/bin/kafka-topics.sh --delete --topic $topic --bootstrap-server master:9092 2>&1"
    Write-Host "  $topic -> $out" -ForegroundColor Gray
}

Start-Sleep -Seconds 5

Write-Host "`nCreating new topics..."
foreach ($topic in $topics) {
    $out = Get-SSH "master" "/opt/module/kafka/bin/kafka-topics.sh --create --topic $topic --partitions 3 --replication-factor 1 --bootstrap-server master:9092 2>&1"
    if ($out -match "Created" -or $out -match "already exists") {
        Write-Host "  $topic -> [OK]" -ForegroundColor Green
    } else {
        Write-Host "  $topic -> $out" -ForegroundColor Yellow
    }
}

Write-Host "`nTopic list:"
$listOut = Get-SSH "master" "/opt/module/kafka/bin/kafka-topics.sh --list --bootstrap-server master:9092"
$listOut | ForEach-Object { Write-Host "  $_" }

Write-Host "`nTopic details:"
foreach ($topic in $topics) {
    $detail = Get-SSH "master" "/opt/module/kafka/bin/kafka-topics.sh --describe --topic $topic --bootstrap-server master:9092 2>&1"
    Write-Host "  [$topic]" -ForegroundColor Cyan
    $detail | Select-Object -First 5 | ForEach-Object { Write-Host "    $_" -ForegroundColor Gray }
}

# SUMMARY
Write-Host "`n============================================================" -ForegroundColor Cyan
Write-Host "  DONE! Next steps:" -ForegroundColor Cyan
Write-Host "============================================================"
Write-Host @"
1. Restart Flume:
   cd /opt/module/flume
   bin/flume-ng agent -n a1 -c conf -f conf/MyFlume.conf -Dflume.root.logger=INFO,console

2. Start data generation (if needed):
   cd /data_log
   bash generate_change_data.sh &

3. Restart Flink:
   cd /opt/module/flink
   bin/flink run -d /opt/module/flink/realtime-data-process-1.0-SNAPSHOT.jar

4. Test Kafka consumer:
   kafka-console-consumer.sh --topic device_state --bootstrap-server master:9092 --from-beginning --max-messages 3
"@
