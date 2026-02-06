'use client';

import { useState } from 'react';
import Link from 'next/link';
import { Button, Card, Input } from '@/components/ui';
import { useDocumentList } from '@/hooks/useDocuments';
import { useUser } from '@/context/UserContext';
import { ArrowLeft, RefreshCw, Download, Eye } from 'lucide-react';

export default function HistoryPage() {
  const { userId } = useUser();
  const { documents, loading, error, refetch } = useDocumentList(userId);
  const [searchTerm, setSearchTerm] = useState('');
  const [filterType, setFilterType] = useState<'all' | 'prenuptial' | 'divorce'>('all');
  const [filterStatus, setFilterStatus] = useState<'all' | 'processing' | 'completed' | 'failed'>('all');

  const filteredDocuments = documents.filter((doc) => {
    const matchesSearch = 
      doc.metadata?.parties?.[0]?.name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      doc.metadata?.parties?.[1]?.name?.toLowerCase().includes(searchTerm.toLowerCase());

    const matchesType =
      filterType === 'all' || doc.metadata?.document_type === filterType;

    const matchesStatus = filterStatus === 'all' || doc.status === filterStatus;

    return matchesSearch && matchesType && matchesStatus;
  });

  const stats = {
    total: documents.length,
    completed: documents.filter((d) => d.status === 'completed').length,
    processing: documents.filter((d) => d.status === 'processing').length,
    failed: documents.filter((d) => d.status === 'failed').length,
  };

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Document History</h1>
          <p className="text-gray-600 mt-1">View and manage all your generated documents</p>
        </div>
        <Link href="/generate">
          <Button variant="secondary" className="flex items-center gap-2">
            <ArrowLeft className="w-4 h-4" />
            Back to Generate
          </Button>
        </Link>
      </div>

      {/* Stats Section */}
      <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-4 gap-4">
        {[
          { label: 'Total Documents', value: stats.total, color: 'primary' },
          { label: 'Completed', value: stats.completed, color: 'success' },
          { label: 'Processing', value: stats.processing, color: 'warning' },
          { label: 'Failed', value: stats.failed, color: 'error' },
        ].map((stat, idx) => (
          <Card key={idx} className={`border-t-4 border-${stat.color}-600`}>
            <div className="text-center">
              <p className="text-gray-600 text-sm mb-2">{stat.label}</p>
              <p className={`text-3xl font-bold text-${stat.color}-600`}>{stat.value}</p>
            </div>
          </Card>
        ))}
      </div>

      {/* Filters */}
      <Card className="bg-gray-50 border-gray-200">
        <div className="space-y-4">
          <h3 className="font-semibold text-gray-900">Filters</h3>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
            <Input
              label="Search by name"
              placeholder="Search party names..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
            />

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Document Type</label>
              <select
                value={filterType}
                onChange={(e) => setFilterType(e.target.value as any)}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500"
              >
                <option value="all">All Types</option>
                <option value="prenuptial">Prenuptial Agreement</option>
                <option value="divorce">Divorce Settlement</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Status</label>
              <select
                value={filterStatus}
                onChange={(e) => setFilterStatus(e.target.value as any)}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500"
              >
                <option value="all">All Status</option>
                <option value="processing">Processing</option>
                <option value="completed">Completed</option>
                <option value="failed">Failed</option>
              </select>
            </div>
          </div>

          <div className="flex gap-2">
            <Button
              variant="secondary"
              size="sm"
              onClick={() => {
                setSearchTerm('');
                setFilterType('all');
                setFilterStatus('all');
              }}
            >
              Clear Filters
            </Button>
            <Button
              variant="secondary"
              size="sm"
              onClick={() => refetch()}
              className="flex items-center gap-2"
              disabled={loading}
            >
              <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
              Refresh
            </Button>
          </div>
        </div>
      </Card>

      {/* Results */}
      <div className="space-y-4">
        <div className="flex justify-between items-center">
          <h2 className="text-lg font-semibold text-gray-900">
            {filteredDocuments.length} Document{filteredDocuments.length !== 1 ? 's' : ''} Found
          </h2>
        </div>

        {error && (
          <Card className="border-l-4 border-error-600">
            <p className="text-error-600">{error}</p>
            <Button onClick={() => refetch()} variant="secondary" size="sm" className="mt-4">
              Try Again
            </Button>
          </Card>
        )}

        {loading && documents.length === 0 ? (
          <div className="text-center py-12">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary-600 mx-auto mb-4" />
            <p className="text-gray-600">Loading documents...</p>
          </div>
        ) : filteredDocuments.length === 0 ? (
          <Card className="text-center py-12">
            <div className="text-gray-400 mb-4">
              <Download className="w-12 h-12 mx-auto" />
            </div>
            <h3 className="font-semibold text-gray-900 mb-2">No documents found</h3>
            <p className="text-gray-600 mb-4">
              {searchTerm || filterType !== 'all' || filterStatus !== 'all'
                ? 'Try adjusting your filters'
                : 'Generate your first document to get started'}
            </p>
            <Link href="/generate">
              <Button variant="primary">Generate Document</Button>
            </Link>
          </Card>
        ) : (
          <div className="space-y-3">
            {filteredDocuments.map((doc) => (
              <div key={doc.job_id} className="bg-white border border-gray-200 rounded-lg p-4 hover:border-primary-300 transition">
                <div className="flex items-start justify-between gap-4">
                  <div className="flex-1">
                    <div className="mb-2">
                      <h3 className="font-semibold text-gray-900">
                        {doc.metadata?.document_type === 'prenuptial' ? 'Prenuptial Agreement' : 'Divorce Settlement'}
                      </h3>
                      <p className="text-sm text-gray-600">
                        {doc.metadata?.parties?.[0]?.name} & {doc.metadata?.parties?.[1]?.name}
                      </p>
                    </div>

                    <div className="flex gap-2 flex-wrap mb-3">
                      <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-800">
                        {doc.metadata?.document_type}
                      </span>
                      <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-800">
                        {new Date(doc.created_at).toLocaleDateString()}
                      </span>
                      <span
                        className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                          doc.status === 'completed'
                            ? 'bg-success-100 text-success-800'
                            : doc.status === 'processing'
                            ? 'bg-warning-100 text-warning-800'
                            : 'bg-error-100 text-error-800'
                        }`}
                      >
                        {doc.status}
                      </span>
                    </div>

                    <p className="text-xs text-gray-500 font-mono">{doc.job_id}</p>
                  </div>

                  <div className="flex gap-2">
                    {doc.status === 'completed' && doc.result?.pdf_url && (
                      <a href={doc.result.pdf_url} download className="inline-block">
                        <Button variant="primary" size="sm" className="flex items-center gap-2">
                          <Download className="w-4 h-4" />
                          Download
                        </Button>
                      </a>
                    )}
                    <Button variant="secondary" size="sm" className="flex items-center gap-2">
                      <Eye className="w-4 h-4" />
                      View
                    </Button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Pagination info */}
      {documents.length > filteredDocuments.length && (
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 text-center text-sm text-blue-700">
          Showing {filteredDocuments.length} of {documents.length} documents
        </div>
      )}
    </div>
  );
}
