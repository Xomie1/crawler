'use client';

import { useState } from 'react';
import { DocumentForm } from '@/components/DocumentForm';
import { DocumentList } from '@/components/DocumentList';
import { Button, Card, Alert } from '@/components/ui';
import { useDocumentStatus } from '@/hooks/useDocuments';
import { Download, ArrowLeft } from 'lucide-react';
import Link from 'next/link';

export default function GeneratePage() {
  const [currentJobId, setCurrentJobId] = useState<string | null>(null);
  const docStatus = useDocumentStatus(currentJobId || undefined);

  const handleFormSubmit = (jobId: string) => {
    setCurrentJobId(jobId);
  };

  const progress = docStatus.progress || 0;

  const getDownloadUrl = (jobId: string) => {
    const apiUrl = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:8000';
    return `${apiUrl}/api/documents/download/${jobId}`;
  };

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Generate Document</h1>
          <p className="text-gray-600 mt-1">Create a new legal document in minutes</p>
        </div>
        <Link href="/">
          <Button variant="secondary" className="flex items-center gap-2">
            <ArrowLeft className="w-4 h-4" />
            Back to Home
          </Button>
        </Link>
      </div>

      {currentJobId && docStatus.status !== 'idle' && (
        <div className="space-y-4">
          {/* Status Alert */}
          {docStatus.data?.status === 'completed' && (
            <Alert
              variant="success"
              title="Document Ready!"
              message="Your document has been generated successfully. Download it now."
              onClose={() => {}}
            />
          )}

          {docStatus.data?.status === 'failed' && (
            <Alert
              variant="error"
              title="Generation Failed"
              message="There was an error generating your document. Please try again."
              onClose={() => setCurrentJobId(null)}
            />
          )}

          {docStatus.data?.status === 'processing' && (
            <Alert
              variant="info"
              title="Generating Document"
              message={`Processing: ${progress}% complete`}
              onClose={() => {}}
            />
          )}

          {/* Status Card */}
          <Card className="border-l-4 border-primary-600">
            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <h3 className="text-lg font-semibold text-gray-900">Generation Status</h3>
                <span className="px-3 py-1 rounded-full text-sm font-medium bg-primary-100 text-primary-700 capitalize">
                  {docStatus.data?.status || docStatus.status}
                </span>
              </div>

              <div className="space-y-3">
                <div>
                  <div className="flex justify-between items-center mb-2">
                    <span className="text-sm font-medium text-gray-700">Progress</span>
                    <span className="text-sm font-medium text-gray-900">{progress}%</span>
                  </div>
                  <div className="w-full bg-gray-200 rounded-full h-3 overflow-hidden">
                    <div
                      className="bg-gradient-to-r from-primary-600 to-blue-600 h-3 transition-all duration-500"
                      style={{ width: `${progress}%` }}
                    />
                  </div>
                </div>

                {docStatus.data?.status === 'completed' && (
                  <div className="flex gap-2">
                    <a href={getDownloadUrl(currentJobId)} download className="flex-1">
                      <Button variant="primary" size="lg" className="w-full flex items-center justify-center gap-2">
                        <Download className="w-5 h-5" />
                        Download PDF
                      </Button>
                    </a>
                    <Button
                      variant="secondary"
                      size="lg"
                      onClick={() => setCurrentJobId(null)}
                      className="flex items-center justify-center gap-2"
                    >
                      Create Another
                    </Button>
                  </div>
                )}

                {docStatus.data?.status === 'failed' && (
                  <Button
                    variant="primary"
                    size="lg"
                    onClick={() => setCurrentJobId(null)}
                    className="w-full"
                  >
                    Try Again
                  </Button>
                )}

                {docStatus.data?.status === 'processing' && (
                  <p className="text-sm text-gray-600 text-center">
                    Your document is being generated. This may take a minute or two.
                  </p>
                )}
              </div>

              <div className="bg-gray-50 p-4 rounded border border-gray-200">
                <p className="text-xs text-gray-500 mb-1">Job ID</p>
                <p className="text-sm font-mono text-gray-900">{currentJobId}</p>
              </div>
            </div>
          </Card>
        </div>
      )}

      {!currentJobId || docStatus.status === 'idle' ? (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
          <div className="lg:col-span-2">
            <Card className="border-t-4 border-primary-600">
              <DocumentForm onSubmit={handleFormSubmit} />
            </Card>
          </div>

          <div className="space-y-6">
            {/* Info Card */}
            <Card title="Need Help?" className="bg-blue-50 border-blue-200">
              <div className="space-y-3 text-sm">
                <div>
                  <h4 className="font-semibold text-gray-900 mb-1">Document Types</h4>
                  <ul className="space-y-1 text-gray-600">
                    <li>• Prenuptial Agreement</li>
                    <li>• Divorce Settlement</li>
                  </ul>
                </div>
                <div>
                  <h4 className="font-semibold text-gray-900 mb-1">Tips</h4>
                  <ul className="space-y-1 text-gray-600">
                    <li>• Be accurate with addresses</li>
                    <li>• Review generated documents carefully</li>
                    <li>• Consult an attorney before signing</li>
                  </ul>
                </div>
              </div>
            </Card>

            {/* Recent Activity */}
            <Card title="Recent Activity">
              <DocumentList maxItems={3} />
            </Card>
          </div>
        </div>
      ) : null}

      {/* FAQ Section */}
      <section className="bg-white rounded-lg border border-gray-200 p-8">
        <h2 className="text-2xl font-bold text-gray-900 mb-6">Frequently Asked Questions</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
          {[
            {
              q: 'How long does it take to generate a document?',
              a: 'Most documents are generated within 1-3 minutes. Complex documents with many customizations may take longer.',
            },
            {
              q: 'What formats can I download?',
              a: 'You can download documents as PDF or DOCX, allowing you to edit them with Microsoft Word or Google Docs.',
            },
            {
              q: 'Are these documents legally binding?',
              a: 'Our documents are templates that should be reviewed by a licensed attorney before signing. They are not a substitute for legal advice.',
            },
            {
              q: 'Can I modify the generated document?',
              a: 'Yes! Download the DOCX version to edit the document with your preferred word processor.',
            },
            {
              q: 'Is my information secure?',
              a: 'Yes, all your data is encrypted and stored securely. We never share your information with third parties.',
            },
            {
              q: 'What if generation fails?',
              a: 'If generation fails, you can try again. Contact support if the issue persists.',
            },
          ].map((item, idx) => (
            <div key={idx}>
              <h3 className="font-semibold text-gray-900 mb-2">{item.q}</h3>
              <p className="text-gray-600 text-sm">{item.a}</p>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}
