#!/bin/bash

# DocGen Frontend - Build and Deploy Script
# This script handles development, production builds, and AWS deployment

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FRONTEND_DIR="$SCRIPT_DIR/frontend"
PROJECT_ROOT="$SCRIPT_DIR"

# Functions
print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
    exit 1
}

print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

# Check prerequisites
check_prerequisites() {
    print_header "Checking Prerequisites"

    # Check Node.js
    if ! command -v node &> /dev/null; then
        print_error "Node.js is not installed"
    fi
    NODE_VERSION=$(node -v)
    print_success "Node.js $NODE_VERSION found"

    # Check npm
    if ! command -v npm &> /dev/null; then
        print_error "npm is not installed"
    fi
    NPM_VERSION=$(npm -v)
    print_success "npm $NPM_VERSION found"

    # Check if frontend directory exists
    if [ ! -d "$FRONTEND_DIR" ]; then
        print_error "Frontend directory not found at $FRONTEND_DIR"
    fi
    print_success "Frontend directory found"
}

# Install dependencies
install_dependencies() {
    print_header "Installing Dependencies"

    cd "$FRONTEND_DIR"
    npm install
    print_success "Dependencies installed"
}

# Development server
dev_server() {
    print_header "Starting Development Server"
    print_info "Application will be available at http://localhost:3000"
    print_info "Press Ctrl+C to stop"

    cd "$FRONTEND_DIR"
    npm run dev
}

# Production build
build_production() {
    print_header "Building for Production"

    cd "$FRONTEND_DIR"
    npm run build
    print_success "Production build complete"
    print_info "Build artifacts in: $FRONTEND_DIR/out"
}

# Type checking
type_check() {
    print_header "Running Type Check"

    cd "$FRONTEND_DIR"
    npm run type-check
    print_success "Type check passed"
}

# Linting
lint_code() {
    print_header "Linting Code"

    cd "$FRONTEND_DIR"
    npm run lint || print_info "Fix linting issues before deployment"
}

# Full build pipeline
full_build() {
    check_prerequisites
    install_dependencies
    lint_code
    type_check
    build_production
}

# AWS deployment
aws_deploy() {
    local bucket_name="$1"
    local distribution_id="$2"

    if [ -z "$bucket_name" ] || [ -z "$distribution_id" ]; then
        print_error "Usage: $0 aws-deploy <bucket-name> <cloudfront-distribution-id>"
    fi

    print_header "Deploying to AWS"

    if [ ! -d "$FRONTEND_DIR/out" ]; then
        print_error "Build artifacts not found. Run 'npm run build' first"
    fi

    # Check AWS CLI
    if ! command -v aws &> /dev/null; then
        print_error "AWS CLI is not installed"
    fi

    print_info "S3 Bucket: $bucket_name"
    print_info "CloudFront Distribution: $distribution_id"

    # Upload to S3
    print_info "Uploading to S3..."
    cd "$FRONTEND_DIR/out"
    aws s3 sync . "s3://$bucket_name/" \
        --delete \
        --cache-control "public, max-age=0, must-revalidate" \
        --exclude "_next/*" \
        --exclude "static/*"

    # Handle _next with longer cache
    print_info "Uploading assets with long cache..."
    aws s3 sync "_next" "s3://$bucket_name/_next" \
        --cache-control "public, max-age=31536000, immutable"

    print_success "Files uploaded to S3"

    # Invalidate CloudFront
    print_info "Invalidating CloudFront cache..."
    aws cloudfront create-invalidation \
        --distribution-id "$distribution_id" \
        --paths "/*"

    print_success "CloudFront invalidated"
    print_success "Deployment complete!"
}

# Main
case "${1:-}" in
    "dev")
        check_prerequisites
        install_dependencies
        dev_server
        ;;
    "build")
        full_build
        ;;
    "install")
        check_prerequisites
        install_dependencies
        ;;
    "type-check")
        check_prerequisites
        cd "$FRONTEND_DIR"
        npm run type-check
        ;;
    "lint")
        check_prerequisites
        lint_code
        ;;
    "aws-deploy")
        build_production
        aws_deploy "$2" "$3"
        ;;
    "help")
        cat << EOF
DocGen Frontend - Build and Deploy Script

Usage: $0 <command> [options]

Commands:
  dev              Start development server (with hot reload)
  build            Full production build pipeline
  install          Install dependencies only
  type-check       Run TypeScript type checking
  lint             Run ESLint
  aws-deploy       Build and deploy to AWS S3 + CloudFront
                   Usage: $0 aws-deploy <bucket-name> <distribution-id>
  help             Show this help message

Examples:
  $0 dev
  $0 build
  $0 aws-deploy my-bucket-name d1234abcd

Environment Variables:
  FRONTEND_DIR     Override frontend directory path
  NODE_ENV         Set to 'production' for production builds

For more information, see README.md and AWS_DEPLOYMENT_GUIDE.md
EOF
        ;;
    *)
        print_error "Unknown command: '${1:-}'. Run '$0 help' for usage."
        ;;
esac
