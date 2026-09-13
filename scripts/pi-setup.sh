#!/bin/sh
# One-time setup on the Raspberry Pi Zero W, before a long capture.
# Idempotent: safe to re-run. Needs root.
#
#   sudo ./scripts/pi-setup.sh
#
set -e
[ "$(id -u)" = 0 ] || { echo "run with sudo"; exit 1; }

echo "== build dependencies =="
apt-get update
apt-get install -y build-essential libwebsockets-dev libcjson-dev \
                   ca-certificates chrony rt-tests

echo "== Wi-Fi power save off =="
# The #1 cause of multi-hundred-ms latency spikes and overnight dropouts
# on brcmfmac. Applies now and on every boot.
iw wlan0 set power_save off || true
cat > /etc/systemd/system/wifi-powersave-off.service <<'UNIT'
[Unit]
Description=Disable Wi-Fi power save
After=network.target
[Service]
Type=oneshot
ExecStart=/sbin/iw wlan0 set power_save off
RemainAfterExit=yes
[Install]
WantedBy=multi-user.target
UNIT
systemctl enable wifi-powersave-off.service

echo "== clock: slew, do not step =="
# The Pi Zero W has NO hardware RTC. The clock is wrong until NTP syncs,
# and the monitor sleeps on absolute CLOCK_REALTIME deadlines, so a step
# mid-run shows up as skipped seconds. chrony is allowed to step only in
# the first few updates after boot, then must slew.
if ! grep -q '^makestep 1.0 3' /etc/chrony/chrony.conf 2>/dev/null; then
    sed -i 's/^makestep.*/makestep 1.0 3/' /etc/chrony/chrony.conf || \
        echo 'makestep 1.0 3' >> /etc/chrony/chrony.conf
fi
systemctl enable --now chrony
systemctl disable --now systemd-timesyncd 2>/dev/null || true

echo "== rtprio + memlock limits (so it runs without sudo) =="
cat > /etc/security/limits.d/99-telemetry.conf <<'LIM'
@telemetry  -  rtprio  60
@telemetry  -  memlock unlimited
LIM
groupadd -f telemetry
usermod -aG telemetry "${SUDO_USER:-pi}"

echo "== CPU governor: performance =="
# Single core. Frequency ramping is jitter you do not need.
for g in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    [ -w "$g" ] && echo performance > "$g"
done

echo "== quiet the machine down =="
systemctl disable --now unattended-upgrades  2>/dev/null || true
systemctl disable --now apt-daily.timer      2>/dev/null || true
systemctl disable --now apt-daily-upgrade.timer 2>/dev/null || true
systemctl disable --now man-db.timer         2>/dev/null || true
systemctl disable --now triggerhappy         2>/dev/null || true
systemctl disable --now bluetooth            2>/dev/null || true

echo
echo "== health check =="
printf 'throttled : %s  (0x0 = no undervoltage/throttling)\n' "$(vcgencmd get_throttled)"
printf 'temp      : %s\n' "$(vcgencmd measure_temp)"
printf 'governor  : %s\n' "$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo n/a)"
printf 'powersave : %s\n' "$(iw wlan0 get power_save 2>/dev/null || echo n/a)"
printf 'clock     : %s\n' "$(timedatectl show -p NTPSynchronized --value) synchronised"
echo
echo "Log out and back in for the group limits to take effect."
