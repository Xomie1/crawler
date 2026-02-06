'use client';

import React, { useEffect } from 'react';
import { Button, Card, LoadingSpinner, EmptyState } from './ui';
import { useDocumentList } from '@/hooks/useDocuments';
import { useUser } from '@/context/UserContext';
import { DocumentInfo } from '@/lib/api';
import { Download, CheckCircle, AlertCircle, Clock } from 'lucide-react';

interface DocumentListProps {
  maxItems?: number;
  onDownload?: (jobId: string) => void;
}

function DocumentListItem({ doc }: { doc: DocumentInfo }) {
  const [isRefreshing, setIsRefreshing] = React.useState(false);
  const status = doc.status || 'queued';

  const handleRefresh = async () => {
    setIsRefreshing(true);
    setTimeout(() => setIsRefreshing(false), 1000);
  };

  const getStatusIcon = (st: string) => {
    switch (st) {
      case 'completed':
        return <CheckCircle className="w-5 h-5 text-success-600" />;
      case 'failed':
        return <AlertCircle className="w-5 h-5 text-error-600" />;
      case 'processing':
        return <Clock className="w-5 h-5 text-warning-600" />;
      default:
        return <Clock className="w-5 h-5 text-primary-600" />;
    }
  };

  const getStatusColor = (st: string) => {
    switch (st) {
      case 'completed':
        return 'bg-success-50 border-success-200';
      case 'failed':
        return 'bg-error-50 border-error-200';
      case 'processing':
        return 'bg-warning-50 border-warning-200';
      default:
        return 'bg-primary-50 border-primary-200';
    }
  };

  return (
    <div className={`border rounded-lg p-4 ${getStatusColor(status)}`}>
      <div className="flex items-start justify-between gap-4">
        <div className="flex-1">
          <div className="flex items-center gap-3 mb-2">
            {getStatusIcon(status)}
            <div>
              <h3 className="font-semibold text-gray-900">
                Document
              </h3>
              <p className="text-sm text-gray-600">
                Job ID: {doc.job_id}
              </p>
            </div>
          </div>

          <div className="flex gap-2 flex-wrap mb-3">
            <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-800 capitalize">
              {status}
            </span>
            <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-800">
              {new Date(doc.created_at).toLocaleDateString()}
            </span>
          </div>

          <div className="text-sm text-gray-600">
            <p>Status: <span className="font-medium capitalize">{status}</span></p>
          </div>
        </div>

        <div className="flex gap-2">
          {status === 'completed' && (
            <Button variant="primary" size="sm" className="flex items-center gap-2">
              <Download className="w-4 h-4" />
              Download
            </Button>
          )}
          {status === 'processing' && (
            <Button variant="secondary" size="sm" onClick={handleRefresh} disabled={isRefreshing}>
              Refresh
            </Button>
          )}
        </div>
      </div>
    </div>
  );
}

export function DocumentList({ maxItems = 10 }: DocumentListProps) {
  const { userId } = useUser();
  const { documents, loading, error, refetch } = useDocumentList(userId);

  useEffect(() => {
    const interval = setInterval(() => {
      refetch();
    }, 5000); // Refresh every 5 seconds

    return () => clearInterval(interval);
  }, [refetch]);

  if (loading && documents.length === 0) {
    return (
      <div className="flex justify-center py-8">
        <LoadingSpinner />
      </div>
    );
  }

  if (error) {
    return (
      <Card title="Error Loading Documents">
        <p className="text-error-600">{error}</p>
        <Button onClick={refetch} variant="secondary" size="sm" className="mt-4">
          Try Again
        </Button>
      </Card>
    );
  }

  if (documents.length === 0) {
    return (
      <EmptyState
        icon={<Download className="w-12 h-12" />}
        title="No Documents Yet"
      />
    );
  }

  const displayDocs = documents.slice(0, maxItems);

  return (
    <div className="space-y-3">
      <div className="flex justify-between items-center">
        <h2 className="text-lg font-semibold text-gray-900">Document History</h2>
        <span className="text-sm text-gray-600">{documents.length} total</span>
      </div>
      <div className="space-y-3">
        {displayDocs.map((doc) => (
          <DocumentListItem key={doc.job_id} doc={doc} />
        ))}
      </div>
      {documents.length > maxItems && (
        <button className="text-primary-600 hover:text-primary-700 text-sm font-medium">
          View All {documents.length} Documents
        </button>
      )}
    </div>
  );
}
