'use client';

import { useState, useEffect } from 'react';
import { Upload, Play, Download, AlertCircle, CheckCircle, Clock } from 'lucide-react';
import { useUser } from '@/context/UserContext';
import axios from 'axios';

// Configure axios with base URL
const axiosInstance = axios.create({
  baseURL: process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:8000',
});

interface CrawlJob {
  status: 'queued' | 'processing' | 'completed' | 'failed';
  job_id: string;
  urls_crawled: number;
  total_urls: number;
  percentage: number;
  estimated_time_remaining_seconds?: number;
  error?: { type: string; message: string };
  results_excel_url?: string;
  results_jsonl_url?: string;
  processing_time_seconds?: number;
}

export default function CrawlPage() {
  const { userId } = useUser();
  const [file, setFile] = useState<File | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [crawlJob, setCrawlJob] = useState<CrawlJob | null>(null);
  const [isPolling, setIsPolling] = useState(false);
  const [configs, setConfigs] = useState({
    timeout: 30,
    robots_policy: 'respect',
    use_playwright: true,
    delay: 10,
  });

  // Poll for status updates
  useEffect(() => {
    if (!crawlJob || !isPolling || crawlJob.status === 'completed' || crawlJob.status === 'failed') {
      setIsPolling(false);
      return;
    }

    const interval = setInterval(async () => {
      try {
        const res = await axiosInstance.get(`/api/crawler/status/${crawlJob.job_id}`);
        setCrawlJob(res.data);
        if (res.data.status === 'completed' || res.data.status === 'failed') {
          setIsPolling(false);
        }
      } catch (error) {
        console.error('Error polling status:', error);
      }
    }, 2000);

    return () => clearInterval(interval);
  }, [crawlJob, isPolling]);

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files?.[0]) {
      const selectedFile = e.target.files[0];
      if (['.xlsx', '.xls', '.csv'].some(ext => selectedFile.name.endsWith(ext))) {
        setFile(selectedFile);
      } else {
        alert('Please upload an Excel (.xlsx, .xls) or CSV file');
      }
    }
  };

  const handleSubmit = async () => {
    if (!file) {
      alert('Please select a file');
      return;
    }

    setIsSubmitting(true);
    const formData = new FormData();
    formData.append('file', file);
    formData.append('user_id', userId);
    formData.append('timeout', configs.timeout.toString());
    formData.append('robots_policy', configs.robots_policy);
    formData.append('use_playwright', configs.use_playwright.toString());
    formData.append('delay', configs.delay.toString());

    try {
      const res = await axiosInstance.post('/api/crawler/upload-excel', formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
      });

      setCrawlJob({
        status: 'queued',
        job_id: res.data.job_id,
        urls_crawled: 0,
        total_urls: res.data.total_urls,
        percentage: 0,
      });
      setIsPolling(true);
      setFile(null);
    } catch (error: any) {
      console.error('Error submitting crawl job:', error);
      const errorMsg = error.response?.data?.detail || error.message || 'Failed to submit crawl job';
      alert(`Error: ${errorMsg}`);
    } finally {
      setIsSubmitting(false);
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'completed': return 'bg-success-100 text-success-800';
      case 'processing': return 'bg-warning-100 text-warning-800';
      case 'failed': return 'bg-error-100 text-error-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'completed': return <CheckCircle className="w-5 h-5" />;
      case 'processing': return <Clock className="w-5 h-5 animate-spin" />;
      case 'failed': return <AlertCircle className="w-5 h-5" />;
      default: return <Clock className="w-5 h-5" />;
    }
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-4xl font-bold text-gray-900 mb-2">Web Crawler</h1>
        <p className="text-gray-600">
          Upload a list of URLs and crawl them automatically. Supports .xlsx, .xls, and .csv files.
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Upload Section */}
        {!crawlJob && (
          <div className="bg-white rounded-lg border border-gray-200 p-6">
            <h2 className="text-lg font-semibold mb-4">Upload URLs</h2>

            {/* File Upload */}
            <div className="border-2 border-dashed border-gray-300 rounded-lg p-8 text-center mb-6 hover:border-primary-400 transition">
              <Upload className="w-8 h-8 text-gray-400 mx-auto mb-2" />
              <p className="text-gray-600 mb-2">
                Drag and drop your Excel file here, or click to select
              </p>
              <input
                type="file"
                onChange={handleFileChange}
                accept=".xlsx,.xls,.csv"
                className="hidden"
                id="file-input"
              />
              <label
                htmlFor="file-input"
                className="text-primary-600 hover:text-primary-700 cursor-pointer font-medium"
              >
                Select file
              </label>

              {file && (
                <p className="text-gray-900 font-medium mt-3">
                  ✓ {file.name}
                </p>
              )}
            </div>

            {/* Configuration */}
            <div className="space-y-4 mb-6 p-4 bg-gray-50 rounded-lg">
              <h3 className="font-semibold text-gray-900">Configuration</h3>

              <div>
                <label className="text-sm text-gray-600 block mb-1">
                  Timeout (seconds)
                </label>
                <input
                  type="number"
                  min="5"
                  max="300"
                  value={configs.timeout}
                  onChange={(e) => setConfigs({ ...configs, timeout: parseInt(e.target.value) })}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg"
                />
              </div>

              <div>
                <label className="text-sm text-gray-600 block mb-1">
                  Robots.txt Policy
                </label>
                <select
                  value={configs.robots_policy}
                  onChange={(e) => setConfigs({ ...configs, robots_policy: e.target.value })}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg"
                >
                  <option value="respect">Respect robots.txt</option>
                  <option value="ignore">Ignore robots.txt</option>
                </select>
              </div>

              <div>
                <label className="text-sm text-gray-600 block mb-1">
                  Delay between requests (seconds)
                </label>
                <input
                  type="number"
                  min="0"
                  max="60"
                  value={configs.delay}
                  onChange={(e) => setConfigs({ ...configs, delay: parseInt(e.target.value) })}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg"
                />
              </div>

              <label className="flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={configs.use_playwright}
                  onChange={(e) => setConfigs({ ...configs, use_playwright: e.target.checked })}
                  className="rounded"
                />
                <span className="text-sm text-gray-600">Use Playwright (browser automation)</span>
              </label>
            </div>

            {/* Submit Button */}
            <button
              onClick={handleSubmit}
              disabled={isSubmitting || !file}
              className="w-full bg-primary-600 hover:bg-primary-700 disabled:bg-gray-400 text-white font-semibold py-3 rounded-lg transition flex items-center justify-center gap-2"
            >
              <Play className="w-4 h-4" />
              {isSubmitting ? 'Starting Crawl...' : 'Start Crawl'}
            </button>
          </div>
        )}

        {/* Status Section */}
        {crawlJob && (
          <div className="bg-white rounded-lg border border-gray-200 p-6">
            <div className="flex items-center gap-3 mb-6">
              <div className={`p-2 rounded-lg ${getStatusColor(crawlJob.status)}`}>
                {getStatusIcon(crawlJob.status)}
              </div>
              <div>
                <h2 className="text-lg font-semibold text-gray-900 capitalize">
                  {crawlJob.status}
                </h2>
                <p className="text-sm text-gray-600">
                  Job ID: {crawlJob.job_id.substring(0, 8)}...
                </p>
              </div>
            </div>

            {/* Progress Bar */}
            <div className="mb-6">
              <div className="flex justify-between items-center mb-2">
                <span className="text-sm font-medium text-gray-700">Progress</span>
                <span className="text-sm font-semibold text-gray-900">
                  {crawlJob.percentage}%
                </span>
              </div>
              <div className="w-full bg-gray-200 rounded-full h-3 overflow-hidden">
                <div
                  className="bg-primary-600 h-3 rounded-full transition-all duration-300"
                  style={{ width: `${crawlJob.percentage}%` }}
                />
              </div>
            </div>

            {/* Stats */}
            <div className="grid grid-cols-2 gap-4 mb-6 p-4 bg-gray-50 rounded-lg">
              <div>
                <p className="text-sm text-gray-600">URLs Crawled</p>
                <p className="text-2xl font-bold text-gray-900">
                  {crawlJob.urls_crawled}
                </p>
              </div>
              <div>
                <p className="text-sm text-gray-600">Total URLs</p>
                <p className="text-2xl font-bold text-gray-900">
                  {crawlJob.total_urls}
                </p>
              </div>
              {crawlJob.status === 'processing' && crawlJob.estimated_time_remaining_seconds && (
                <div className="col-span-2">
                  <p className="text-sm text-gray-600">Estimated Time Remaining</p>
                  <p className="text-xl font-semibold text-gray-900">
                    ~{Math.ceil(crawlJob.estimated_time_remaining_seconds / 60)} minutes
                  </p>
                </div>
              )}
              {crawlJob.status === 'completed' && crawlJob.processing_time_seconds && (
                <div className="col-span-2">
                  <p className="text-sm text-gray-600">Processing Time</p>
                  <p className="text-xl font-semibold text-gray-900">
                    {Math.ceil(crawlJob.processing_time_seconds / 60)} minutes
                  </p>
                </div>
              )}
            </div>

            {/* Error Display */}
            {crawlJob.error && (
              <div className="mb-6 p-4 bg-error-50 border border-error-200 rounded-lg">
                <p className="text-sm font-semibold text-error-800 mb-1">
                  {crawlJob.error.type}
                </p>
                <p className="text-sm text-error-700">
                  {crawlJob.error.message}
                </p>
              </div>
            )}

            {/* Download Buttons */}
            {crawlJob.status === 'completed' && (
              <div className="space-y-2">
                {crawlJob.results_excel_url && (
                  <a
                    href={crawlJob.results_excel_url}
                    download
                    className="block w-full bg-success-600 hover:bg-success-700 text-white font-semibold py-2 px-4 rounded-lg transition text-center flex items-center justify-center gap-2"
                  >
                    <Download className="w-4 h-4" />
                    Download Excel Results
                  </a>
                )}
                {crawlJob.results_jsonl_url && (
                  <a
                    href={crawlJob.results_jsonl_url}
                    download
                    className="block w-full bg-primary-600 hover:bg-primary-700 text-white font-semibold py-2 px-4 rounded-lg transition text-center flex items-center justify-center gap-2"
                  >
                    <Download className="w-4 h-4" />
                    Download JSONL Results
                  </a>
                )}
                <button
                  onClick={() => setCrawlJob(null)}
                  className="w-full mt-4 border border-gray-300 text-gray-700 font-semibold py-2 px-4 rounded-lg hover:bg-gray-50 transition"
                >
                  Start New Crawl
                </button>
              </div>
            )}

            {(crawlJob.status === 'processing' || crawlJob.status === 'queued') && (
              <button
                onClick={() => setCrawlJob(null)}
                className="w-full border border-gray-300 text-gray-700 font-semibold py-2 px-4 rounded-lg hover:bg-gray-50 transition"
              >
                Reset
              </button>
            )}

            {crawlJob.status === 'failed' && (
              <button
                onClick={() => setCrawlJob(null)}
                className="w-full bg-primary-600 hover:bg-primary-700 text-white font-semibold py-2 px-4 rounded-lg transition"
              >
                Try Again
              </button>
            )}
          </div>
        )}

        {/* Help Section */}
        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <h2 className="text-lg font-semibold mb-4">File Format</h2>
          <div className="space-y-4 text-sm text-gray-600">
            <div>
              <p className="font-medium text-gray-900 mb-1">Excel Format (.xlsx, .xls)</p>
              <p>Column A: URLs (required)</p>
              <p>Column B: Company Names (optional)</p>
            </div>

            <div>
              <p className="font-medium text-gray-900 mb-1">CSV Format</p>
              <p>Similar structure: first column for URLs, second for company names</p>
            </div>

            <div>
              <p className="font-medium text-gray-900 mb-1">Example:</p>
              <pre className="bg-gray-100 p-2 rounded text-xs overflow-auto">
{`URL,Company
https://example.com,Example Corp
https://test.com,Test Inc
https://sample.org,Sample Org`}
              </pre>
            </div>

            <div className="pt-4 border-t border-gray-200">
              <p className="font-medium text-gray-900 mb-2">Output Files</p>
              <p>Results are generated in both Excel and JSONL formats for easy integration with other tools.</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
