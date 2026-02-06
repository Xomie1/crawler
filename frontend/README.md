# DocGen Frontend - Next.js Application

Professional document generation interface built with Next.js, React, TypeScript, and Tailwind CSS.

## Overview

The DocGen frontend is a modern Single Page Application (SPA) that provides:

- ✅ Document generation form with validation
- ✅ Real-time progress tracking with status polling
- ✅ Document history and download management
- ✅ Responsive design for mobile/tablet/desktop
- ✅ AWS S3 + CloudFront deployment ready
- ✅ Type-safe API integration with TypeScript
- ✅ User session management with localStorage

## Tech Stack

| Layer | Technology | Version |
|-------|-----------|---------|
| Framework | Next.js | 14.x |
| UI Library | React | 18.x |
| Language | TypeScript | 5.x |
| Styling | Tailwind CSS | 3.x |
| HTTP Client | Axios | 1.x |
| Icons | Lucide React | latest |
| Package Manager | npm | 10.x |

## Project Structure

```
frontend/
├── public/                 # Static assets
├── src/
│   ├── app/               # Next.js app router
│   │   ├── layout.tsx     # Root layout with navigation
│   │   ├── page.tsx       # Home/dashboard page
│   │   ├── globals.css    # Global Tailwind styles
│   │   ├── generate/
│   │   │   └── page.tsx   # Document generation page
│   │   └── history/
│   │       └── page.tsx   # Document history page
│   ├── components/        # Reusable components
│   │   ├── ui.tsx         # UI component library
│   │   ├── DocumentForm.tsx      # Form for document generation
│   │   └── DocumentList.tsx      # List view of documents
│   ├── context/          # React context providers
│   │   └── UserContext.tsx       # User session management
│   ├── hooks/            # Custom React hooks
│   │   └── useDocuments.ts       # API operation hooks
│   └── lib/              # Utility functions and API client
│       └── api.ts        # TypeScript API client
├── package.json          # Dependencies
├── next.config.js        # Next.js configuration
├── tailwind.config.js    # Tailwind CSS configuration
├── postcss.config.js     # PostCSS configuration
├── tsconfig.json         # TypeScript configuration
└── .env.example          # Environment variables template
```

## Installation

### Prerequisites

- Node.js 18.x or higher
- npm 10.x or higher
- FastAPI backend running on `http://localhost:8000`

### Setup Instructions

1. **Install Dependencies**

```bash
cd frontend
npm install
```

2. **Configure Environment Variables**

```bash
# Copy example to .env.local
cp .env.example .env.local

# Edit .env.local with your backend URL
NEXT_PUBLIC_API_BASE_URL=http://localhost:8000
```

3. **Start Development Server**

```bash
npm run dev
```

Application will be available at `http://localhost:3000`

## Available Scripts

```bash
# Development
npm run dev           # Start dev server with hot reload

# Production
npm run build         # Create optimized production build
npm run start         # Start production server

# Analysis
npm run lint          # Run ESLint for code quality
npm run type-check    # Run TypeScript type checking

# Deployment
npm run export        # Export to static HTML (for S3)
```

## Building for Production

### Local Build

```bash
# Create optimized build
npm run build

# Test production build locally
npm run start

# Visit http://localhost:3000
```

### AWS Deployment

For S3 + CloudFront deployment:

```bash
# Build static files (outputs to 'out/' directory)
npm run build

# Upload to S3
aws s3 sync out/ s3://your-bucket-name/ --delete

# Invalidate CloudFront cache
aws cloudfront create-invalidation --distribution-id YOUR_DIST_ID --paths "/*"
```

See [AWS_DEPLOYMENT_GUIDE.md](../AWS_DEPLOYMENT_GUIDE.md) for detailed instructions.

## Key Components

### 1. **DocumentForm Component**

Handles document generation form with:
- Document type selection (Prenuptial/Divorce)
- Party A & B information collection
- Options checkboxes (Property Separation, Alimony, Children)
- Form validation
- Error handling

**Usage:**
```tsx
<DocumentForm onSubmit={(jobId) => console.log('Created:', jobId)} />
```

### 2. **DocumentList Component**

Displays document history with:
- Status indicators (completed/processing/failed)
- Progress bars for processing documents
- Download buttons for completed documents
- Auto-refresh every 5 seconds

**Usage:**
```tsx
<DocumentList maxItems={10} onDownload={(jobId) => {}} />
```

### 3. **UI Component Library** (`src/components/ui.tsx`)

Pre-built Tailwind-styled components:

- **Alert** - Informational banners with 4 variants
- **Button** - Buttons with loading state and variants
- **Input** - Text inputs with labels and error states
- **Select** - Dropdowns with options
- **Card** - Container component
- **LoadingSpinner** - Progress indicator
- **EmptyState** - No-data placeholder

### 4. **Custom Hooks** (`src/hooks/useDocuments.ts`)

React hooks for API operations:

```tsx
// Generate new document
const { generateDocument, status } = useDocumentGeneration();
await generateDocument(formData);

// Poll document status
const { status, progress } = useDocumentStatus(jobId);

// List user documents
const { documents, loading, error } = useDocumentList(userId);

// Send document via email
const { sendEmail, status } = useSendEmail();
await sendEmail(jobId, email);
```

### 5. **API Client** (`src/lib/api.ts`)

Type-safe API communication:

```typescript
const api = new DocumentAPI('http://localhost:8000');

// Generate document
const { job_id } = await api.generateDocument(request);

// Get status
const doc = await api.getStatus(job_id);

// Download
const url = api.getDownloadUrl(job_id);

// List documents
const { documents, total } = await api.listDocuments(userId);

// Send email
await api.sendEmail(job_id, email);
```

### 6. **User Context** (`src/context/UserContext.tsx`)

Manages user identification:

```tsx
// In component
const { user } = useUser();
console.log(user.user_id);  // Auto-generated UUID

// In provider
<UserProvider>
  <App />
</UserProvider>
```

## API Integration

The frontend communicates with the FastAPI backend at `/api/documents`:

### Endpoints

```
POST   /api/documents/generate      - Create document
GET    /api/documents/status/:id    - Get document status
GET    /api/documents/download/:id  - Download document
GET    /api/documents/list          - List user documents
POST   /api/documents/send-email    - Send document via email
GET    /api/documents/health        - Health check
```

### Authentication

Currently uses localStorage-based `user_id`:

```typescript
// Auto-generated on first visit
const userId = localStorage.getItem('userId');

// Sent with all requests
{
  metadata: {
    user_id: userId
  }
}
```

## Styling

### Tailwind CSS Configuration

Custom colors defined in `tailwind.config.js`:

```javascript
colors: {
  primary: {
    50: '#f0f9ff',
    600: '#0ea5e9',
    700: '#0284c7',
  },
  success: { /* Green */ },
  error: { /* Red */ },
  warning: { /* Amber */ },
}
```

### Global Styles

Global styles in `src/app/globals.css`:
- Custom scrollbar styling
- Selection colors
- Focus states
- Animations
- Utility classes

### Responsive Design

Mobile-first approach with breakpoints:
- `sm`: 640px
- `md`: 768px
- `lg`: 1024px
- `xl`: 1280px

## Deployment

### Development Environment

```bash
# Start dev server
npm run dev

# Accessible at http://localhost:3000
# Hot reload enabled for code changes
```

### AWS S3 + CloudFront

See [AWS_DEPLOYMENT_GUIDE.md](../AWS_DEPLOYMENT_GUIDE.md) for:
- S3 bucket setup
- CloudFront distribution configuration
- Custom domain setup
- CI/CD automation with GitHub Actions

### Environment Configuration

For AWS deployment, update `.env.production.local`:

```env
NEXT_PUBLIC_API_BASE_URL=https://api.yourdomain.com
NEXT_PUBLIC_AWS_REGION=us-east-1
```

## Performance Optimizations

1. **Code Splitting** - Next.js automatic route-based splitting
2. **Image Optimization** - Next.js Image component (when needed)
3. **CSS Optimization** - Tailwind purges unused styles
4. **Bundle Size** - Tree-shaking with ES modules
5. **Caching** - CloudFront + S3 versioning
6. **Compression** - Gzip enabled in CloudFront

## Browser Support

- Chrome/Edge 90+
- Firefox 88+
- Safari 14+
- Mobile browsers (iOS Safari 14+, Chrome Android)

## Troubleshooting

### Issue: API calls return 404

**Solution**: Ensure FastAPI backend is running
```bash
# Check backend
curl http://localhost:8000/api/documents/health
```

### Issue: CORS errors

**Solution**: Verify CORS is enabled in backend
```python
# In FastAPI main.py
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
)
```

### Issue: Form validation not working

**Solution**: Check browser console for validation errors
- Inspect Network tab for API errors
- Verify form data structure matches backend

### Issue: Build fails with TypeScript errors

**Solution**: Run type checking
```bash
npm run type-check
```

### Issue: Slow build or dev server

**Solution**: Clear cache and reinstall
```bash
rm -rf .next node_modules package-lock.json
npm install
npm run build
```

## Development Workflow

1. **Feature Development**
   ```bash
   npm run dev          # Start dev server
   npm run type-check   # Check types
   npm run lint         # Lint code
   ```

2. **Before Commit**
   ```bash
   npm run lint         # Fix linting issues
   npm run type-check   # Verify types
   npm run build        # Test production build
   ```

3. **Deployment**
   ```bash
   npm run build        # Create production build
   aws s3 sync out/ s3://bucket/  # Upload to S3
   ```

## Code Organization Best Practices

### Components

```tsx
// ✅ Good: Functional with proper typing
export function DocumentForm({ onSubmit }: Props) {
  // Implementation
}

// ✅ Good: Use 'use client' for interactivity
'use client';
```

### Hooks

```tsx
// ✅ Good: Named exports
export function useDocuments() {
  // Implementation
}

// ✅ Good: Return proper types
return {
  documents: DocumentInfo[],
  loading: boolean,
  error: string | null,
};
```

### API Client

```tsx
// ✅ Good: Type-safe with interfaces
const response: DocumentResponse = await api.generate(request);

// ✅ Good: Error handling
try {
  await api.generate(request);
} catch (error) {
  // Handle error
}
```

## Security Considerations

1. **No Sensitive Data in localStorage** - Only user_id stored
2. **HTTPS Recommended** - Use CloudFront for HTTPS
3. **API Validation** - Backend validates all inputs
4. **CORS Properly Configured** - Only allow trusted origins
5. **Environment Variables** - Never commit .env files
6. **Content Security Policy** - Can be added to CloudFront

## Performance Monitoring

### Development

```bash
# Analyze bundle size
npm run build
npm install -g next-bundle-analyzer
```

### Production

Monitor with CloudWatch:
- CloudFront requests/bandwidth
- S3 object count and size
- Error rates and latency

## Contributing

When adding new features:

1. Create feature branch: `git checkout -b feature/name`
2. Develop with `npm run dev`
3. Test with `npm run build`
4. Check types: `npm run type-check`
5. Lint code: `npm run lint`
6. Commit with descriptive messages
7. Create pull request

## Related Documentation

- [PHASE5_API_SCHEMA.md](../PHASE5_API_SCHEMA.md) - Backend API specification
- [PHASE5_README.md](../PHASE5_README.md) - Backend setup guide
- [AWS_DEPLOYMENT_GUIDE.md](../AWS_DEPLOYMENT_GUIDE.md) - AWS deployment instructions
- [Next.js Documentation](https://nextjs.org/docs)
- [Tailwind CSS Documentation](https://tailwindcss.com/docs)

## License

This project is part of the DocGen system and is proprietary.

## Support

For issues or questions:
1. Check documentation files listed above
2. Review error logs in browser console
3. Check backend logs: `tail -f logs/phase5.log`
4. Test with curl: `curl http://localhost:8000/api/documents/health`

---

**Last Updated**: 2024
**Frontend Version**: 1.0.0
**Node Version**: 18.x
**Next.js Version**: 14.x
