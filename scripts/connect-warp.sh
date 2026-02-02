#!/bin/bash
set -e

# Don't wait for background jobs to complete
set +m

# Exponential backoff helper
backoff() {
  local max_attempts=10
  local max_delay=4
  local attempt=1
  local delay=1
  
  while [ $attempt -le $max_attempts ]; do
    if "$@"; then
      return 0
    fi
    
    if [ $attempt -lt $max_attempts ]; then
      if [ $delay -gt $max_delay ]; then
        delay=$max_delay
      fi
      sleep $delay
      delay=$((delay * 2))
    fi
    
    attempt=$((attempt + 1))
  done
  
  return 1
}

# Convert config object to XML
json_to_xml() {
  local org=$1
  local auth_id=$2
  local auth_secret=$3
  local unique_id=$4
  
  local xml=""
  
  [ -n "$org" ] && xml="${xml}<key>organization</key>\n<string>${org}</string>\n"
  [ -n "$auth_id" ] && xml="${xml}<key>auth_client_id</key>\n<string>${auth_id}</string>\n"
  [ -n "$auth_secret" ] && xml="${xml}<key>auth_client_secret</key>\n<string>${auth_secret}</string>\n"
  [ -n "$unique_id" ] && xml="${xml}<key>unique_client_id</key>\n<string>${unique_id}</string>\n"
  
  echo -e "$xml"
}

# Write Linux configuration
write_linux_config() {
  local org=$1
  local auth_id=$2
  local auth_secret=$3
  local unique_id=$4
  
  local xml_content=$(json_to_xml "$org" "$auth_id" "$auth_secret" "$unique_id")
  local config="<dict>\n${xml_content}\n</dict>"
  
  sudo mkdir -p /var/lib/cloudflare-warp/
  echo -e "$config" | sudo tee /var/lib/cloudflare-warp/mdm.xml
}

# Check WARP registration
check_warp_registration() {
  local org=$1
  warp-cli --accept-tos settings 2>&1 | grep -q "Organization: ${org}"
}

# Check WARP connection
check_warp_connected() {
  local output
  output=$(warp-cli --accept-tos status 2>&1 || true)
  if echo "$output" | grep -q "Reason: Registration Missing"; then
    warp-cli --accept-tos connect || true
    sleep 2
    output=$(warp-cli --accept-tos status 2>&1 || true)
  fi
  # Check for various "Connected" status formats
  echo "$output" | grep -qiE "(Status update: Connected|Status: Connected|Connected)"
}

# Main execution
if [ -z "$CLOUDFLARE_WARP_AUTH_CLIENT_ID" ] || [ -z "$CLOUDFLARE_WARP_AUTH_CLIENT_SECRET" ]; then
  echo "CLOUDFLARE_WARP_AUTH_CLIENT_ID or CLOUDFLARE_WARP_AUTH_CLIENT_SECRET not set. Skipping WARP connection."
  exit 0
fi

ORGANIZATION=${CLOUDFLARE_WARP_ORGANIZATION:-jfp}
UNIQUE_CLIENT_ID=${CLOUDFLARE_WARP_UNIQUE_CLIENT_ID:-}

# Start WARP daemon if not running
if ! pgrep -x warp-svc > /dev/null; then
  # In containers, start directly with sudo to avoid systemd overhead
  # Use nohup to properly daemonize and prevent hanging
  nohup sudo warp-svc </dev/null &
  sleep 2
fi

# Clean up any existing invalidated registration
echo "Cleaning up any existing registration..."
sudo warp-cli --accept-tos registration delete || true
sudo rm -f /var/lib/cloudflare-warp/mdm.xml || true
sleep 1

# Write configuration
echo "Writing WARP configuration..."
write_linux_config "$ORGANIZATION" "$CLOUDFLARE_WARP_AUTH_CLIENT_ID" "$CLOUDFLARE_WARP_AUTH_CLIENT_SECRET" "$UNIQUE_CLIENT_ID"

# Verify config file exists
if [ ! -f /var/lib/cloudflare-warp/mdm.xml ]; then
  echo "Error: Config file was not created"
  exit 1
fi

# Restart daemon to pick up new config
echo "Restarting WARP daemon..."
if pgrep -x warp-svc > /dev/null; then
  sudo pkill -x warp-svc || true
  sleep 2
fi
nohup sudo warp-svc </dev/null &
sleep 5

# Wait for registration with exponential backoff
echo "Checking WARP registration..."
if ! backoff check_warp_registration "$ORGANIZATION"; then
  echo "Failed to register WARP after retries"
  echo "Debug: warp-cli settings output:"
  warp-cli --accept-tos settings
  exit 1
fi

# Connect
echo "Connecting to WARP..."
warp-cli --accept-tos connect || true

# Wait for connection with exponential backoff
echo "Checking WARP connection..."
if ! backoff check_warp_connected; then
  echo "Failed to connect WARP after retries"
  echo "Debug: warp-cli status output:"
  warp-cli --accept-tos status
  exit 1
fi

echo "Cloudflare WARP connected"
exit 0
