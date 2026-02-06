/** @type {import('next').NextConfig} */
const nextConfig = {
  // Enable static export for AWS S3 + CloudFront
  output: 'export',
  
  // Image optimization for static export
  images: {
    unoptimized: true,
  },
  
  // Environment variables
  env: {
    NEXT_PUBLIC_API_BASE_URL: process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:8000',
  },
};

module.exports = nextConfig;
