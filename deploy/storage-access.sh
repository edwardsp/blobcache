#!/usr/bin/env bash
# Toggle public network access on the storage account so vnet rules apply.
# Default action is already Deny + vnet rule allows the AKS subnet, so flipping
# publicAccess between Enabled and Disabled is the entire dance.
#
# Reads STORAGE_ACCOUNT and RESOURCE_GROUP from .env (or environment).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"
[[ -f "$REPO_ROOT/.env" ]] && set -a && . "$REPO_ROOT/.env" && set +a

ACCOUNT="${STORAGE_ACCOUNT:?set STORAGE_ACCOUNT (in .env or env) to your storage account name}"
RG="${RESOURCE_GROUP:?set RESOURCE_GROUP (in .env or env) to the storage account resource group}"

case "${1:-}" in
  on)
    echo "[$(date +%T)] enabling public network access (vnet rules apply)"
    az storage account update -n "$ACCOUNT" -g "$RG" \
      --public-network-access Enabled --default-action Deny >/dev/null
    az storage account show -n "$ACCOUNT" -g "$RG" \
      --query '{publicAccess:publicNetworkAccess,default:networkRuleSet.defaultAction}' -o json
    ;;
  off)
    echo "[$(date +%T)] disabling public network access (locked down)"
    az storage account update -n "$ACCOUNT" -g "$RG" \
      --public-network-access Disabled >/dev/null
    az storage account show -n "$ACCOUNT" -g "$RG" \
      --query '{publicAccess:publicNetworkAccess,default:networkRuleSet.defaultAction}' -o json
    ;;
  status)
    az storage account show -n "$ACCOUNT" -g "$RG" \
      --query '{publicAccess:publicNetworkAccess,default:networkRuleSet.defaultAction,vnet:networkRuleSet.virtualNetworkRules}' -o json
    ;;
  *)
    echo "usage: $0 {on|off|status}"
    exit 1
    ;;
esac
