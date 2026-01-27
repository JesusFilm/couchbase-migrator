if [ -n "$CLOUDFLARE_WARP_REGISTRATION_TOKEN" ]; then
  warp-cli register "$CLOUDFLARE_WARP_REGISTRATION_TOKEN" || true
  warp-cli set-mode warp
  warp-cli connect
  echo "Cloudflare WARP connected"
else
  echo "CLOUDFLARE_WARP_REGISTRATION_TOKEN not set. Skipping WARP connection."
fi