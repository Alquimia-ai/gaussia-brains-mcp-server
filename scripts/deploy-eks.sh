#!/usr/bin/env bash
# Deploy gaussia-brains-mcp-server to an EKS cluster.
#
# This script applies the Kubernetes manifests under k8s/base through a temporary
# Kustomize overlay that pins the namespace (alquimia-platform) and the image you
# point it at. Secrets are NOT created here -- you will add them yourself.
#
# Usage:
#   # Just apply manifests using an already-built image:
#   AWS_REGION=us-east-1 IMAGE_REPO=123456789012.dkr.ecr.us-east-1.amazonaws.com/gaussia-brains-mcp-server IMAGE_TAG=0.1.0 ./scripts/deploy-eks.sh
#
#   # Build the Docker image from ../Dockerfile, push it, then deploy:
#   AWS_REGION=us-east-1 IMAGE_REPO=123456789012.dkr.ecr.us-east-1.amazonaws.com/gaussia-brains-mcp-server ./scripts/deploy-eks.sh --build
#
# Required environment variables (or edit defaults below):
#   AWS_REGION      - EKS/ECR region
#   IMAGE_REPO      - image repository (e.g. ECR URI)
#   IMAGE_TAG       - image tag (defaults to short git SHA, or latest with --build)
#
# Optional:
#   NAMESPACE       - defaults to alquimia-platform
#   EKS_CLUSTER     - cluster name; if set, the script runs 'aws eks update-kubeconfig'
#   SKIP_ROLLOUT    - set to any value to skip waiting for pods

set -euo pipefail

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

NAMESPACE="${NAMESPACE:-alquimia-platform}"
AWS_REGION="${AWS_REGION:-}"
EKS_CLUSTER="${EKS_CLUSTER:-}"
IMAGE_REPO="${IMAGE_REPO:-}"
IMAGE_TAG="${IMAGE_TAG:-$(git rev-parse --short HEAD 2>/dev/null || echo latest)}"
SKIP_ROLLOUT="${SKIP_ROLLOUT:-}"

BUILD=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build)
      BUILD=true
      IMAGE_TAG="${IMAGE_TAG:-latest}"
      shift
      ;;
    --tag)
      IMAGE_TAG="$2"
      shift 2
      ;;
    --repo)
      IMAGE_REPO="$2"
      shift 2
      ;;
    --cluster)
      EKS_CLUSTER="$2"
      shift 2
      ;;
    --region)
      AWS_REGION="$2"
      shift 2
      ;;
    -h|--help)
      sed -n '2,24p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "$AWS_REGION" ]]; then
  echo "ERROR: AWS_REGION is required." >&2
  exit 1
fi

if [[ -z "$IMAGE_REPO" ]]; then
  echo "ERROR: IMAGE_REPO is required. Set it to your ECR repository URI." >&2
  exit 1
fi

if [[ "$IMAGE_REPO" != */* ]]; then
  echo "ERROR: IMAGE_REPO must contain a '/' (e.g. <account>.dkr.ecr.<region>.amazonaws.com/gaussia-brains-mcp-server)." >&2
  exit 1
fi

FULL_IMAGE="${IMAGE_REPO}:${IMAGE_TAG}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OVERLAY_DIR="$(mktemp -d /tmp/gbms-eks-overlay.XXXXXX)"
trap 'rm -rf "$OVERLAY_DIR"' EXIT

# --------------------------------------------------------------------------- #
# Tool checks
# --------------------------------------------------------------------------- #

for tool in kubectl aws; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "ERROR: '$tool' not found in PATH." >&2
    exit 1
  fi
done

if "$BUILD"; then
  if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: 'docker' not found in PATH (needed with --build)." >&2
    exit 1
  fi
fi

# --------------------------------------------------------------------------- #
# EKS kubeconfig
# --------------------------------------------------------------------------- #

if [[ -n "$EKS_CLUSTER" ]]; then
  echo "==> Updating kubeconfig for cluster '$EKS_CLUSTER' in $AWS_REGION"
  aws eks update-kubeconfig --region "$AWS_REGION" --name "$EKS_CLUSTER"
fi

if ! kubectl auth can-i create deployments >/dev/null 2>&1; then
  echo "ERROR: kubectl cannot reach the cluster or current context lacks permission." >&2
  exit 1
fi

# --------------------------------------------------------------------------- #
# Build and push image
# --------------------------------------------------------------------------- #

if "$BUILD"; then
  echo "==> Logging in to ECR"
  aws ecr get-login-password --region "$AWS_REGION" \
    | docker login --username AWS --password-stdin "${IMAGE_REPO%/*}"

  echo "==> Building image $FULL_IMAGE"
  docker build --platform linux/amd64 -t "$FULL_IMAGE" "$REPO_ROOT"

  echo "==> Pushing image $FULL_IMAGE"
  docker push "$FULL_IMAGE"
fi

# --------------------------------------------------------------------------- #
# Create namespace and temporary overlay
# --------------------------------------------------------------------------- #

echo "==> Ensuring namespace '$NAMESPACE' exists"
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

cat > "$OVERLAY_DIR/kustomization.yaml" <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: $NAMESPACE

resources:
  - ${REPO_ROOT}/k8s/base

images:
  - name: gaussia-brains-mcp-server
    newName: ${IMAGE_REPO}
    newTag: "${IMAGE_TAG}"
EOF

# --------------------------------------------------------------------------- #
# Apply manifests
# --------------------------------------------------------------------------- #

echo "==> Applying manifests to namespace '$NAMESPACE' with image $FULL_IMAGE"
kubectl apply -k "$OVERLAY_DIR"

# --------------------------------------------------------------------------- #
# Wait for rollout
# --------------------------------------------------------------------------- #

if [[ -z "$SKIP_ROLLOUT" ]]; then
  echo "==> Waiting for writer deployment rollout"
  kubectl rollout status deployment/gaussia-brains-mcp-server -n "$NAMESPACE" --timeout=300s

  echo "==> Waiting for read replica deployment rollout"
  kubectl rollout status deployment/gaussia-brains-mcp-server-read -n "$NAMESPACE" --timeout=300s
fi

# --------------------------------------------------------------------------- #
# Secrets checklist
# --------------------------------------------------------------------------- #

echo ""
echo "==========================================================================="
echo "Deployment applied to namespace: $NAMESPACE"
echo "Image: $FULL_IMAGE"
echo "==========================================================================="
echo ""
echo "NEXT STEPS: create the secrets yourself before the service can fully start."
echo ""
echo "1. Registry credentials (required if the registry is private or the writer"
echo "   needs to push brains). Create or update the Secret:"
echo ""
echo "   kubectl -n $NAMESPACE create secret generic gaussia-brains-mcp-server \\"
echo "     --from-literal=GBMS_REGISTRY_USERNAME=<username> \\"
echo "     --from-literal=GBMS_REGISTRY_PASSWORD=<password>"
echo ""
echo "2. Embeddings API key (only if GBMS_EMBEDDINGS_URL is set in the ConfigMap):"
echo ""
echo "   kubectl -n $NAMESPACE patch secret gaussia-brains-mcp-server --type merge -p \\"
echo "     '{\"stringData\":{\"GBMS_EMBEDDINGS_API_KEY\":\"<key>\"}}'"
echo ""
echo "3. Auth issuers (REQUIRED unless GBMS_ALLOW_INSECURE_DEV=true). Edit the"
echo "   ConfigMap to set GBMS_AUTH_ISSUERS, e.g.:"
echo ""
echo "   kubectl -n $NAMESPACE edit configmap gaussia-brains-mcp-server"
echo ""
echo "   Set GBMS_AUTH_ISSUERS to a JSON list of trusted issuers/role mappings."
echo ""
echo "4. Optional cosign public key for signature verification:"
echo ""
echo "   kubectl -n $NAMESPACE create secret generic gaussia-brains-mcp-server-cosign \\"
echo "     --from-file=cosign.pub=/path/to/cosign.pub"
echo ""
echo "   Then uncomment GBMS_COSIGN_KEY in the ConfigMap."
echo ""
echo "Useful commands:"
echo "  kubectl -n $NAMESPACE get pods"
echo "  kubectl -n $NAMESPACE logs -l app.kubernetes.io/name=gaussia-brains-mcp-server --tail=100"
echo ""
