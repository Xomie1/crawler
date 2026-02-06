'use client';

import { useState, useEffect } from 'react';
import { useUser } from '@/context/UserContext';
import { Button, Card, Input } from '@/components/ui';
import axios from 'axios';
import { Mail, Check, Send, ChevronDown, ChevronUp, Upload } from 'lucide-react';

// Configure axios with base URL
const axiosInstance = axios.create({
  baseURL: process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:8000',
});

interface Recipient {
  email: string;
  name?: string;
  company_name?: string;
}

interface CampaignPreview {
  status: string;
  job_id: string;
  campaign_name: string;
  total_recipients: number;
  emails_sent: number;
  emails_failed: number;
  progress_percentage: number;
  dry_run: boolean;
}

interface EmailLog {
  recipient_email: string;
  recipient_name?: string;
  company_name?: string;
  send_status: string;
  error_message?: string;
  opened: boolean;
  clicked: boolean;
  bounced: boolean;
  sent_at?: string;
}

export default function EmailCampaignPage() {
  const { userId } = useUser();
  const [campaignName, setCampaignName] = useState('');
  const [senderEmail, setSenderEmail] = useState('sales@company.com');
  const [senderName, setSenderName] = useState('Sales Team');
  const [subjectTemplate, setSubjectTemplate] = useState('Hi {{name}}, exploring opportunities with {{company_name}}');
  const [messageTemplate, setMessageTemplate] = useState(
    'Dear {{name}},\n\nWe at Company would love to explore potential collaborations with {{company_name}}.\n\nBest regards,\nSales Team'
  );
  const [replyTo, setReplyTo] = useState('');
  const [recipients, setRecipients] = useState<Recipient[]>([
    { email: 'contact@example1.com', name: 'John Doe', company_name: 'Example Corp' },
    { email: 'info@example2.jp', name: '田中太郎', company_name: 'Example Japan' },
  ]);
  const [skipDuplicates, setSkipDuplicates] = useState(true);
  const [rateLimit, setRateLimit] = useState(10);
  const [dryRun, setDryRun] = useState(false);
  const [loading, setLoading] = useState(false);
  const [currentJobId, setCurrentJobId] = useState<string | null>(null);
  const [campaignStatus, setCampaignStatus] = useState<CampaignPreview | null>(null);
  const [emailLogs, setEmailLogs] = useState<EmailLog[]>([]);
  const [showLogs, setShowLogs] = useState(false);
  const [campaigns, setCampaigns] = useState<any[]>([]);
  const [showHistory, setShowHistory] = useState(false);
  const [csvFile, setCsvFile] = useState<File | null>(null);
  const [csvLoading, setCsvLoading] = useState(false);

  // Fetch campaigns on mount
  useEffect(() => {
    if (userId) {
      fetchCampaigns();
    }
  }, [userId]);

  // Poll for campaign status
  useEffect(() => {
    if (!currentJobId) return;

    const interval = setInterval(async () => {
      try {
        const response = await axiosInstance.get(`/api/email/status/${currentJobId}`);
        setCampaignStatus(response.data);

        // If completed, fetch logs
        if (response.data.status === 'completed') {
          fetchEmailLogs(currentJobId);
          clearInterval(interval);
        }
      } catch (error) {
        console.error('Error fetching campaign status:', error);
      }
    }, 2000); // Poll every 2 seconds

    return () => clearInterval(interval);
  }, [currentJobId]);

  const fetchCampaigns = async () => {
    try {
      const response = await axiosInstance.get('/api/email/list', {
        params: { user_id: userId, limit: 10 },
      });
      setCampaigns(response.data.campaigns || []);
    } catch (error) {
      console.error('Error fetching campaigns:', error);
    }
  };

  const fetchEmailLogs = async (jobId: string) => {
    try {
      const response = await axiosInstance.get(`/api/email/send-logs/${jobId}`);
      setEmailLogs(response.data.logs || []);
    } catch (error) {
      console.error('Error fetching email logs:', error);
    }
  };

  const handleAddRecipient = () => {
    setRecipients([...recipients, { email: '', name: '', company_name: '' }]);
  };

  const handleRemoveRecipient = (index: number) => {
    setRecipients(recipients.filter((_, i) => i !== index));
  };

  const handleSubmitCampaign = async () => {
    if (!campaignName.trim()) {
      alert('Please enter a campaign name');
      return;
    }

    if (recipients.length === 0 || !recipients[0].email) {
      alert('Please add at least one recipient');
      return;
    }

    setLoading(true);
    try {
      const response = await axiosInstance.post('/api/email/submit', {
        campaign_name: campaignName,
        campaign_type: 'custom',
        recipients,
        sender_email: senderEmail,
        sender_name: senderName,
        subject_template: subjectTemplate,
        message_template: messageTemplate,
        reply_to_email: replyTo || undefined,
        skip_duplicates: skipDuplicates,
        rate_limit_per_hour: rateLimit,
        dry_run: dryRun,
      }, {
        headers: {
          'user-id': userId,
        }
      });

      setCurrentJobId(response.data.job_id);
      setCampaignStatus({
        status: 'queued',
        job_id: response.data.job_id,
        campaign_name: campaignName,
        total_recipients: recipients.length,
        emails_sent: 0,
        emails_failed: 0,
        progress_percentage: 0,
        dry_run: dryRun,
      });

      // Refresh campaigns list
      fetchCampaigns();
    } catch (error: any) {
      alert(`Error: ${error.response?.data?.detail || error.message}`);
    } finally {
      setLoading(false);
    }
  };

  const handleCsvUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    setCsvLoading(true);
    try {
      const text = await file.text();
      const lines = text.trim().split('\n');
      const headers = lines[0].toLowerCase().split(',').map(h => h.trim());
      
      const emailIndex = headers.findIndex(h => h.includes('email'));
      const nameIndex = headers.findIndex(h => h.includes('name'));
      const companyIndex = headers.findIndex(h => h.includes('company'));

      if (emailIndex === -1) {
        alert('CSV must contain an "email" column');
        return;
      }

      const parsedRecipients: Recipient[] = [];
      for (let i = 1; i < lines.length; i++) {
        const values = lines[i].split(',').map(v => v.trim());
        if (values[emailIndex]) {
          parsedRecipients.push({
            email: values[emailIndex],
            name: nameIndex >= 0 ? values[nameIndex] : undefined,
            company_name: companyIndex >= 0 ? values[companyIndex] : undefined,
          });
        }
      }

      setRecipients(parsedRecipients);
      alert(`Loaded ${parsedRecipients.length} recipients from CSV`);
    } catch (error) {
      alert(`Error parsing CSV: ${error}`);
    } finally {
      setCsvLoading(false);
      setCsvFile(null);
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'completed':
        return 'bg-success-50 border-success-200';
      case 'failed':
        return 'bg-error-50 border-error-200';
      case 'in-progress':
        return 'bg-warning-50 border-warning-200';
      default:
        return 'bg-primary-50 border-primary-200';
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 py-8">
      <div className="max-w-6xl mx-auto px-4">
        {/* Header */}
        <div className="mb-8">
          <div className="flex items-center gap-3 mb-2">
            <Mail className="w-8 h-8 text-primary-600" />
            <h1 className="text-3xl font-bold text-gray-900">Email Campaigns</h1>
          </div>
          <p className="text-gray-600">Create and manage bulk email campaigns with template variables</p>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Left: Campaign Form */}
          <div className="lg:col-span-2">
            <Card title="Create Campaign">
              <div className="space-y-4">
                {/* Campaign Name */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Campaign Name *
                  </label>
                  <Input
                    type="text"
                    placeholder="e.g., Q1 2025 Outreach"
                    value={campaignName}
                    onChange={(e) => setCampaignName(e.target.value)}
                  />
                </div>

                {/* Sender Info */}
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      From Email *
                    </label>
                    <Input
                      type="email"
                      value={senderEmail}
                      onChange={(e) => setSenderEmail(e.target.value)}
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      From Name
                    </label>
                    <Input
                      type="text"
                      value={senderName}
                      onChange={(e) => setSenderName(e.target.value)}
                    />
                  </div>
                </div>

                {/* Subject Template */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Subject Template (use variables)
                  </label>
                  <Input
                    type="text"
                    placeholder="Hi {{name}}, message for {{company_name}}"
                    value={subjectTemplate}
                    onChange={(e) => setSubjectTemplate(e.target.value)}
                  />
                  <p className="text-xs text-gray-500 mt-1">Available: name, company_name</p>
                </div>

                {/* Message Template */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Message Template
                  </label>
                  <textarea
                    placeholder="Dear {{name}}, we would like to..."
                    value={messageTemplate}
                    onChange={(e) => setMessageTemplate(e.target.value)}
                    className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-600 focus:border-transparent"
                    rows={6}
                  />
                  <p className="text-xs text-gray-500 mt-1">Available: name, company_name, email</p>
                </div>

                {/* Reply To */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Reply To Email (optional)
                  </label>
                  <Input
                    type="email"
                    placeholder="replies@company.com"
                    value={replyTo}
                    onChange={(e) => setReplyTo(e.target.value)}
                  />
                </div>

                {/* Campaign Settings */}
                <div className="bg-gray-50 p-4 rounded-lg space-y-3">
                  <h3 className="font-semibold text-gray-900">Campaign Settings</h3>

                  <div className="flex items-center justify-between">
                    <label className="text-sm text-gray-700">Skip Duplicate Recipients</label>
                    <input
                      type="checkbox"
                      checked={skipDuplicates}
                      onChange={(e) => setSkipDuplicates(e.target.checked)}
                      className="rounded border-gray-300"
                    />
                  </div>

                  <div>
                    <label className="block text-sm text-gray-700 mb-1">
                      Rate Limit (emails/hour): {rateLimit}
                    </label>
                    <input
                      type="range"
                      min="1"
                      max="50"
                      value={rateLimit}
                      onChange={(e) => setRateLimit(Number(e.target.value))}
                      className="w-full"
                    />
                    <p className="text-xs text-gray-500 mt-1">~{(3600 / rateLimit).toFixed(1)}s per email</p>
                  </div>

                  <div className="flex items-center justify-between bg-yellow-50 p-3 rounded border border-yellow-200">
                    <label className="text-sm text-gray-700">Dry Run (test without sending)</label>
                    <input
                      type="checkbox"
                      checked={dryRun}
                      onChange={(e) => setDryRun(e.target.checked)}
                      className="rounded border-yellow-300"
                    />
                  </div>
                </div>
              </div>
            </Card>
          </div>

          {/* Right: Recipients & Progress */}
          <div className="space-y-6">
            {/* CSV Upload */}
            <Card title="Import Recipients">
              <div className="space-y-3">
                <p className="text-xs text-gray-600">Upload a CSV file with columns: email, name, company_name</p>
                <div className="border-2 border-dashed border-gray-300 rounded-lg p-4">
                  <label className="flex items-center justify-center gap-2 cursor-pointer">
                    <Upload className="w-4 h-4 text-primary-600" />
                    <span className="text-sm font-medium text-primary-600">
                      {csvLoading ? 'Processing...' : 'Choose CSV File'}
                    </span>
                    <input
                      type="file"
                      accept=".csv"
                      onChange={handleCsvUpload}
                      disabled={csvLoading}
                      className="hidden"
                    />
                  </label>
                </div>
              </div>
            </Card>

            {/* Recipients */}
            <Card title="Recipients">
              <div className="space-y-2 max-h-96 overflow-y-auto">
                {recipients.length === 0 ? (
                  <p className="text-sm text-gray-500">No recipients added</p>
                ) : (
                  recipients.map((recipient, index) => (
                    <div
                      key={index}
                      className="flex items-center justify-between text-sm p-2 bg-gray-50 rounded"
                    >
                      <div className="flex-1 min-w-0">
                        <p className="font-medium text-gray-900 truncate">{recipient.email}</p>
                        <p className="text-xs text-gray-500">{recipient.company_name}</p>
                      </div>
                      <button
                        onClick={() => handleRemoveRecipient(index)}
                        className="ml-2 text-error-600 hover:text-error-700"
                      >
                        ×
                      </button>
                    </div>
                  ))
                )}
              </div>
              <Button
                onClick={handleAddRecipient}
                variant="secondary"
                size="sm"
                className="w-full mt-4"
              >
                Add Recipient
              </Button>
            </Card>

            {/* Progress */}
            {campaignStatus && (
              <Card
                title={`Campaign Progress`}
              >
                <div className={`border rounded-lg p-4 ${getStatusColor(campaignStatus.status)}`}>
                  <p className="text-sm font-medium mb-2 capitalize">{campaignStatus.status}</p>
                  <div className="space-y-2">
                    <p className="text-sm">
                      Sent: <span className="font-semibold text-success-600">{campaignStatus.emails_sent}</span>
                    </p>
                    <p className="text-sm">
                      Failed: <span className="font-semibold text-error-600">{campaignStatus.emails_failed}</span>
                    </p>
                    <p className="text-sm">
                      Total: {campaignStatus.total_recipients}
                    </p>
                    <div className="w-full bg-gray-200 rounded-full h-2 mt-3">
                      <div
                        className="bg-primary-600 h-2 rounded-full transition-all duration-500"
                        style={{ width: `${campaignStatus.progress_percentage}%` }}
                      />
                    </div>
                    <p className="text-xs text-gray-600 text-right">
                      {campaignStatus.progress_percentage.toFixed(1)}%
                    </p>
                  </div>
                </div>
              </Card>
            )}

            {/* Action Buttons */}
            <Button
              onClick={handleSubmitCampaign}
              loading={loading}
              className="w-full"
            >
              <Send className="w-4 h-4 mr-2" />
              {dryRun ? 'Start Dry Run' : 'Send Campaign'}
            </Button>
          </div>
        </div>

        {/* Email Logs */}
        {campaignStatus && (
          <div className="mt-8">
            <button
              onClick={() => setShowLogs(!showLogs)}
              className="flex items-center gap-2 text-lg font-semibold text-gray-900 mb-4"
            >
              {showLogs ? <ChevronUp className="w-5 h-5" /> : <ChevronDown className="w-5 h-5" />}
              Detailed Email Logs ({emailLogs.length})
            </button>

            {showLogs && emailLogs.length > 0 && (
              <Card>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead className="bg-gray-50 border-b">
                      <tr>
                        <th className="px-4 py-2 text-left font-medium text-gray-900">Email</th>
                        <th className="px-4 py-2 text-left font-medium text-gray-900">Name</th>
                        <th className="px-4 py-2 text-left font-medium text-gray-900">Status</th>
                        <th className="px-4 py-2 text-left font-medium text-gray-900">Opened</th>
                        <th className="px-4 py-2 text-left font-medium text-gray-900">Clicked</th>
                      </tr>
                    </thead>
                    <tbody>
                      {emailLogs.map((log, idx) => (
                        <tr key={idx} className="border-b hover:bg-gray-50">
                          <td className="px-4 py-3">{log.recipient_email}</td>
                          <td className="px-4 py-3">{log.recipient_name || '—'}</td>
                          <td className="px-4 py-3">
                            <span
                              className={`px-2 py-1 rounded text-xs font-medium ${
                                log.send_status === 'sent'
                                  ? 'bg-success-100 text-success-800'
                                  : 'bg-error-100 text-error-800'
                              }`}
                            >
                              {log.send_status}
                            </span>
                          </td>
                          <td className="px-4 py-3">
                            {log.opened ? (
                              <Check className="w-4 h-4 text-success-600" />
                            ) : (
                              <span className="text-gray-400">—</span>
                            )}
                          </td>
                          <td className="px-4 py-3">
                            {log.clicked ? (
                              <Check className="w-4 h-4 text-success-600" />
                            ) : (
                              <span className="text-gray-400">—</span>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </Card>
            )}
          </div>
        )}

        {/* Campaign History */}
        <div className="mt-8">
          <button
            onClick={() => setShowHistory(!showHistory)}
            className="flex items-center gap-2 text-lg font-semibold text-gray-900 mb-4"
          >
            {showHistory ? <ChevronUp className="w-5 h-5" /> : <ChevronDown className="w-5 h-5" />}
            Campaign History ({campaigns.length})
          </button>

          {showHistory && campaigns.length > 0 && (
            <Card>
              <div className="space-y-3">
                {campaigns.map((campaign) => (
                  <div
                    key={campaign.job_id}
                    className="flex items-center justify-between p-4 bg-gray-50 rounded-lg border"
                  >
                    <div className="flex-1">
                      <p className="font-medium text-gray-900">{campaign.campaign_name}</p>
                      <p className="text-sm text-gray-600">
                        {campaign.emails_sent} sent • {campaign.emails_failed} failed
                      </p>
                    </div>
                    <span
                      className={`px-3 py-1 rounded-full text-xs font-medium ${
                        campaign.status === 'completed'
                          ? 'bg-success-100 text-success-800'
                          : 'bg-primary-100 text-primary-800'
                      }`}
                    >
                      {campaign.status}
                    </span>
                  </div>
                ))}
              </div>
            </Card>
          )}
        </div>
      </div>
    </div>
  );
}
