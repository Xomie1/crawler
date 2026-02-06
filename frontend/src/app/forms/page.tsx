'use client';

import { useState, useEffect } from 'react';
import { useUser } from '@/context/UserContext';
import { Button, Card, Input } from '@/components/ui';
import axios from 'axios';
import { FormInput, Check, Send, ChevronDown, ChevronUp, AlertCircle, Upload } from 'lucide-react';

// Configure axios with base URL
const axiosInstance = axios.create({
  baseURL: process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:8000',
});

interface FormSubmissionJob {
  job_id: string;
  job_name: string;
  status: string;
  total_urls: number;
  successful_submissions: number;
  failed_submissions: number;
  captcha_detected: number;
  progress_percentage?: number;
  error_message?: string;
  created_at?: string;
  completed_at?: string;
  processing_time_seconds?: number;
}

interface FormSubmissionLog {
  id: number;
  url: string;
  form_type?: string;
  submission_status: string;
  status_code?: number;
  error_message?: string;
  captcha_detected: boolean;
  captcha_type?: string;
  detected_fields?: string[];
  submitted_at?: string;
}

export default function FormsPage() {
  const { userId } = useUser();
  const [jobName, setJobName] = useState('Form Submission Campaign');
  const [targetUrls, setTargetUrls] = useState<string[]>([
    'https://example.com/contact',
    'https://example2.com/inquiry',
  ]);
  const [email, setEmail] = useState('contact@company.com');
  const [name, setName] = useState('John Doe');
  const [phone, setPhone] = useState('+1-555-0123');
  const [company, setCompany] = useState('Company Inc');
  const [message, setMessage] = useState('Hello, I would like to inquire about your services.');
  const [submitMethod, setSubmitMethod] = useState('auto');
  const [usePlaywright, setUsePlaywright] = useState(false);
  const [ignoreCaptcha, setIgnoreCaptcha] = useState(false);
  const [timeout, setTimeout_] = useState(30);
  const [delay, setDelay] = useState(2);
  const [loading, setLoading] = useState(false);
  const [currentJobId, setCurrentJobId] = useState<string | null>(null);
  const [jobStatus, setJobStatus] = useState<FormSubmissionJob | null>(null);
  const [submissionLogs, setSubmissionLogs] = useState<FormSubmissionLog[]>([]);
  const [showLogs, setShowLogs] = useState(false);
  const [jobs, setJobs] = useState<FormSubmissionJob[]>([]);
  const [showHistory, setShowHistory] = useState(false);
  const [urlInput, setUrlInput] = useState('');
  const [csvFile, setCsvFile] = useState<File | null>(null);
  const [csvLoading, setCsvLoading] = useState(false);

  // Fetch jobs on mount
  useEffect(() => {
    if (userId) {
      fetchJobs();
    }
  }, [userId]);

  // Poll for job status
  useEffect(() => {
    if (!currentJobId) return;

    const interval = setInterval(async () => {
      try {
        const response = await axios.get(`/api/forms/status/${currentJobId}`);
        setJobStatus(response.data);

        // If completed, fetch logs
        if (response.data.status === 'completed') {
          fetchSubmissionLogs(currentJobId);
          clearInterval(interval);
        }
      } catch (error) {
        console.error('Error fetching job status:', error);
      }
    }, 2000); // Poll every 2 seconds

    return () => clearInterval(interval);
  }, [currentJobId]);

  const fetchJobs = async () => {
    try {
      const response = await axios.get('/api/forms/list', {
        params: { user_id: userId, limit: 10 },
      });
      setJobs(response.data.jobs || []);
    } catch (error) {
      console.error('Error fetching jobs:', error);
    }
  };

  const fetchSubmissionLogs = async (jobId: string) => {
    try {
      const response = await axios.get(`/api/forms/logs/${jobId}`, {
        params: { limit: 100 },
      });
      setSubmissionLogs(response.data.logs || []);
    } catch (error) {
      console.error('Error fetching submission logs:', error);
    }
  };

  const handleAddUrl = () => {
    if (urlInput.trim() && !targetUrls.includes(urlInput.trim())) {
      setTargetUrls([...targetUrls, urlInput.trim()]);
      setUrlInput('');
    }
  };

  const handleRemoveUrl = (index: number) => {
    setTargetUrls(targetUrls.filter((_, i) => i !== index));
  };

  const handleCsvUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    setCsvLoading(true);
    try {
      const text = await file.text();
      const lines = text.trim().split('\n');
      const urls: string[] = [];
      
      for (let i = 1; i < lines.length; i++) {
        const url = lines[i].trim();
        if (url && (url.startsWith('http://') || url.startsWith('https://'))) {
          urls.push(url);
        }
      }

      setTargetUrls(urls);
      alert(`Loaded ${urls.length} URLs from CSV`);
    } catch (error) {
      alert(`Error parsing CSV: ${error}`);
    } finally {
      setCsvLoading(false);
      setCsvFile(null);
    }
  };

  const handleSubmitForms = async () => {
    if (!jobName.trim()) {
      alert('Please enter a job name');
      return;
    }

    if (targetUrls.length === 0) {
      alert('Please add at least one target URL');
      return;
    }

    if (!email.trim()) {
      alert('Please enter an email address');
      return;
    }

    setLoading(true);
    try {
      const response = await axiosInstance.post('/api/forms/submit', {
        job_name: jobName,
        target_urls: targetUrls,
        email,
        name,
        phone: phone || undefined,
        company: company || undefined,
        message: message || undefined,
        submit_method: submitMethod,
        use_playwright: usePlaywright,
        ignore_captcha: ignoreCaptcha,
        timeout,
        delay,
      }, {
        headers: {
          'user-id': userId,
        }
      });

      setCurrentJobId(response.data.job_id);
      setJobStatus({
        job_id: response.data.job_id,
        job_name: jobName,
        status: 'queued',
        total_urls: targetUrls.length,
        successful_submissions: 0,
        failed_submissions: 0,
        captcha_detected: 0,
        progress_percentage: 0,
      });

      // Refresh jobs list
      fetchJobs();
    } catch (error: any) {
      alert(`Error: ${error.response?.data?.detail || error.message}`);
    } finally {
      setLoading(false);
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

  const getSubmissionStatusColor = (status: string) => {
    switch (status) {
      case 'success':
        return 'text-success-600 bg-success-50';
      case 'failed':
        return 'text-error-600 bg-error-50';
      case 'captcha':
        return 'text-warning-600 bg-warning-50';
      case 'not_found':
        return 'text-gray-600 bg-gray-50';
      default:
        return 'text-gray-600 bg-gray-50';
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 py-8">
      <div className="max-w-6xl mx-auto px-4">
        {/* Header */}
        <div className="mb-8">
          <div className="flex items-center gap-3 mb-2">
            <FormInput className="w-8 h-8 text-primary-600" />
            <h1 className="text-3xl font-bold text-gray-900">Form Submission</h1>
          </div>
          <p className="text-gray-600">Automate form submissions on discovered websites</p>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Left: Form Submission Form */}
          <div className="lg:col-span-2">
            <Card title="Create Submission Job">
              <div className="space-y-4">
                {/* Job Name */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Job Name
                  </label>
                  <Input
                    value={jobName}
                    onChange={(e) => setJobName(e.target.value)}
                    placeholder="e.g., Form Submission Campaign #1"
                  />
                </div>

                {/* Target URLs */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Target URLs
                  </label>
                  
                  {/* CSV Upload */}
                  <div className="mb-3 border-2 border-dashed border-gray-300 rounded-lg p-3">
                    <label className="flex items-center justify-center gap-2 cursor-pointer">
                      <Upload className="w-4 h-4 text-primary-600" />
                      <span className="text-sm font-medium text-primary-600">
                        {csvLoading ? 'Processing...' : 'Upload CSV'}
                      </span>
                      <input
                        type="file"
                        accept=".csv"
                        onChange={handleCsvUpload}
                        disabled={csvLoading}
                        className="hidden"
                      />
                    </label>
                    <p className="text-xs text-gray-500 text-center mt-1">CSV with URL column</p>
                  </div>

                  <div className="flex gap-2 mb-2">
                    <Input
                      value={urlInput}
                      onChange={(e) => setUrlInput(e.target.value)}
                      onKeyPress={(e) => e.key === 'Enter' && handleAddUrl()}
                      placeholder="https://example.com/contact"
                    />
                    <Button onClick={handleAddUrl} variant="outline">
                      Add
                    </Button>
                  </div>
                  <div className="space-y-2">
                    {targetUrls.map((url, idx) => (
                      <div
                        key={idx}
                        className="flex items-center justify-between bg-gray-50 p-2 rounded border border-gray-200"
                      >
                        <span className="text-sm text-gray-700 truncate flex-1">{url}</span>
                        <button
                          onClick={() => handleRemoveUrl(idx)}
                          className="text-error-600 hover:text-error-700 text-sm font-medium ml-2"
                        >
                          Remove
                        </button>
                      </div>
                    ))}
                  </div>
                  <p className="text-xs text-gray-500 mt-1">
                    Total URLs: <span className="font-semibold">{targetUrls.length}</span>
                  </p>
                </div>

                {/* Form Data Section */}
                <div className="border-t pt-4 mt-4">
                  <h3 className="font-semibold text-gray-900 mb-3">Form Data</h3>

                  <div className="grid grid-cols-2 gap-3 mb-3">
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">
                        Email *
                      </label>
                      <Input
                        type="email"
                        value={email}
                        onChange={(e) => setEmail(e.target.value)}
                        placeholder="contact@company.com"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">
                        Name
                      </label>
                      <Input
                        value={name}
                        onChange={(e) => setName(e.target.value)}
                        placeholder="John Doe"
                      />
                    </div>
                  </div>

                  <div className="grid grid-cols-2 gap-3 mb-3">
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">
                        Phone
                      </label>
                      <Input
                        value={phone}
                        onChange={(e) => setPhone(e.target.value)}
                        placeholder="+1-555-0123"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">
                        Company
                      </label>
                      <Input
                        value={company}
                        onChange={(e) => setCompany(e.target.value)}
                        placeholder="Company Inc"
                      />
                    </div>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Message
                    </label>
                    <textarea
                      value={message}
                      onChange={(e) => setMessage(e.target.value)}
                      placeholder="Your message here..."
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500"
                      rows={3}
                    />
                  </div>
                </div>

                {/* Advanced Options */}
                <div className="border-t pt-4 mt-4">
                  <h3 className="font-semibold text-gray-900 mb-3">Advanced Options</h3>

                  <div className="grid grid-cols-2 gap-3 mb-3">
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">
                        Submit Method
                      </label>
                      <select
                        value={submitMethod}
                        onChange={(e) => setSubmitMethod(e.target.value)}
                        className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500"
                      >
                        <option value="auto">Auto Detect</option>
                        <option value="direct">Direct POST</option>
                        <option value="ajax">AJAX</option>
                      </select>
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">
                        Timeout (seconds)
                      </label>
                      <Input
                        type="number"
                        value={timeout}
                        onChange={(e) => setTimeout_(parseInt(e.target.value))}
                        min="5"
                        max="120"
                      />
                    </div>
                  </div>

                  <div className="grid grid-cols-2 gap-3 mb-3">
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">
                        Delay Between Submissions (seconds)
                      </label>
                      <Input
                        type="number"
                        value={delay}
                        onChange={(e) => setDelay(parseFloat(e.target.value))}
                        min="0.5"
                        step="0.5"
                      />
                    </div>
                  </div>

                  <div className="space-y-2">
                    <label className="flex items-center gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={usePlaywright}
                        onChange={(e) => setUsePlaywright(e.target.checked)}
                        className="rounded border-gray-300"
                      />
                      <span className="text-sm text-gray-700">Use Playwright for JavaScript rendering</span>
                    </label>
                    <label className="flex items-center gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={ignoreCaptcha}
                        onChange={(e) => setIgnoreCaptcha(e.target.checked)}
                        className="rounded border-gray-300"
                      />
                      <span className="text-sm text-gray-700">Ignore CAPTCHA detection (manual handling)</span>
                    </label>
                  </div>
                </div>

                {/* Submit Button */}
                <div className="border-t pt-4 mt-4">
                  <Button
                    onClick={handleSubmitForms}
                    disabled={loading}
                    className="w-full"
                  >
                    {loading ? 'Submitting...' : 'Submit Forms'}
                    {!loading && <Send className="w-4 h-4 ml-2" />}
                  </Button>
                </div>
              </div>
            </Card>
          </div>

          {/* Right: Status and History */}
          <div className="lg:col-span-1">
            {/* Current Job Status */}
            {jobStatus && (
              <Card
                title="Current Job"
                className={`border-2 ${getStatusColor(jobStatus.status)}`}
              >
                <div className="space-y-3">
                  <div>
                    <p className="text-xs text-gray-600 uppercase font-semibold">Status</p>
                    <p className="text-lg font-bold text-gray-900 capitalize">
                      {jobStatus.status}
                    </p>
                  </div>

                  <div>
                    <p className="text-xs text-gray-600 uppercase font-semibold">Progress</p>
                    <div className="w-full bg-gray-200 rounded-full h-2">
                      <div
                        className="bg-primary-600 h-2 rounded-full transition-all"
                        style={{
                          width: `${jobStatus.progress_percentage || 0}%`,
                        }}
                      />
                    </div>
                    <p className="text-xs text-gray-600 mt-1">
                      {jobStatus.progress_percentage?.toFixed(1) || 0}%
                    </p>
                  </div>

                  <div className="grid grid-cols-3 gap-2 text-center">
                    <div className="bg-success-50 p-2 rounded">
                      <p className="text-2xl font-bold text-success-600">
                        {jobStatus.successful_submissions}
                      </p>
                      <p className="text-xs text-gray-600">Success</p>
                    </div>
                    <div className="bg-error-50 p-2 rounded">
                      <p className="text-2xl font-bold text-error-600">
                        {jobStatus.failed_submissions}
                      </p>
                      <p className="text-xs text-gray-600">Failed</p>
                    </div>
                    <div className="bg-warning-50 p-2 rounded">
                      <p className="text-2xl font-bold text-warning-600">
                        {jobStatus.captcha_detected}
                      </p>
                      <p className="text-xs text-gray-600">CAPTCHA</p>
                    </div>
                  </div>

                  {jobStatus.error_message && (
                    <div className="bg-error-50 border border-error-200 rounded p-2">
                      <p className="text-xs text-error-600 font-semibold flex items-center gap-1">
                        <AlertCircle className="w-3 h-3" />
                        {jobStatus.error_message}
                      </p>
                    </div>
                  )}

                  {jobStatus.status === 'completed' && (
                    <Button
                      onClick={() => setShowLogs(!showLogs)}
                      variant="outline"
                      className="w-full justify-between"
                    >
                      <span>View Logs ({submissionLogs.length})</span>
                      {showLogs ? (
                        <ChevronUp className="w-4 h-4" />
                      ) : (
                        <ChevronDown className="w-4 h-4" />
                      )}
                    </Button>
                  )}
                </div>
              </Card>
            )}

            {/* Submission Logs */}
            {showLogs && submissionLogs.length > 0 && (
              <Card title="Submission Logs" className="mt-4">
                <div className="space-y-2 max-h-96 overflow-y-auto">
                  {submissionLogs.map((log) => (
                    <div
                      key={log.id}
                      className={`p-2 rounded border text-xs ${getSubmissionStatusColor(
                        log.submission_status
                      )}`}
                    >
                      <p className="font-semibold truncate">{log.url}</p>
                      <div className="flex items-center justify-between mt-1">
                        <span className="capitalize font-medium">{log.submission_status}</span>
                        {log.captcha_detected && (
                          <span className="bg-warning-100 text-warning-700 px-2 py-0.5 rounded text-xs">
                            {log.captcha_type || 'CAPTCHA'}
                          </span>
                        )}
                      </div>
                      {log.error_message && (
                        <p className="text-gray-700 mt-1 line-clamp-2">{log.error_message}</p>
                      )}
                    </div>
                  ))}
                </div>
              </Card>
            )}

            {/* Job History */}
            <div className="mt-4">
              <Button
                onClick={() => setShowHistory(!showHistory)}
                variant="outline"
                className="w-full justify-between"
              >
                <span>Job History ({jobs.length})</span>
                {showHistory ? (
                  <ChevronUp className="w-4 h-4" />
                ) : (
                  <ChevronDown className="w-4 h-4" />
                )}
              </Button>

              {showHistory && (
                <Card className="mt-2">
                  <div className="space-y-2 max-h-96 overflow-y-auto">
                    {jobs.map((job) => (
                      <div
                        key={job.job_id}
                        className={`p-2 rounded border-l-4 cursor-pointer hover:bg-gray-50 transition ${
                          job.status === 'completed'
                            ? 'border-l-success-500 bg-success-50'
                            : job.status === 'failed'
                            ? 'border-l-error-500 bg-error-50'
                            : 'border-l-primary-500 bg-primary-50'
                        }`}
                        onClick={() => {
                          setCurrentJobId(job.job_id);
                          setShowLogs(false);
                        }}
                      >
                        <p className="font-semibold text-sm text-gray-900 truncate">
                          {job.job_name}
                        </p>
                        <div className="flex justify-between text-xs text-gray-600 mt-1">
                          <span className="capitalize font-medium">{job.status}</span>
                          <span>{job.total_urls} URLs</span>
                        </div>
                        <div className="text-xs text-gray-600">
                          ✓ {job.successful_submissions} | ✗ {job.failed_submissions} | ⚠ {job.captcha_detected}
                        </div>
                      </div>
                    ))}
                  </div>
                </Card>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
