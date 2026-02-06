const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:8000';

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...(options && options.headers),
    },
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API ${res.status}: ${text}`);
  }
  return res.json() as Promise<T>;
}

export const api = {
  enqueueCrawls(urls: string[]) {
    return request<{ queued: number; job_ids: string[] }>('/api/phase1/enqueue', {
      method: 'POST',
      body: JSON.stringify({ urls }),
    });
  },

  async enqueueUpload(file: File) {
    const formData = new FormData();
    formData.append('file', file);
    const res = await fetch(`${API_BASE}/api/phase1/enqueue_upload`, {
      method: 'POST',
      body: formData,
    });
    if (!res.ok) throw new Error(`Upload failed: ${res.status}`);
    return res.json();
  },

  getCrawls() {
    return request<{ results: any[] }>('/api/phase1/crawls?limit=50');
  },

  runEmailCampaign(crawlFile: string, dryRun: boolean) {
    const form = new FormData();
    form.append('crawl_results_file', crawlFile);
    form.append('dry_run', String(dryRun));
    return fetch(`${API_BASE}/api/phase2/email_campaign`, {
      method: 'POST',
      body: form,
    }).then(res => res.json());
  },

  async emailFromFile(file: File, subjectTemplate: string, messageTemplate: string, dryRun: boolean, testRecipient?: string) {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('subject_template', subjectTemplate);
    formData.append('message_template', messageTemplate);
    formData.append('dry_run', String(dryRun));
    if (testRecipient) formData.append('test_recipient', testRecipient);
    const res = await fetch(`${API_BASE}/api/phase2/email_from_file`, {
      method: 'POST',
      body: formData,
    });
    if (!res.ok) throw new Error(`Email campaign failed: ${res.status}`);
    return res.json();
  },

  submitForms(limit: number) {
    const form = new FormData();
    form.append('limit', String(limit));
    return fetch(`${API_BASE}/api/phase3/submit_forms`, {
      method: 'POST',
      body: form,
    }).then(res => res.json());
  },

  async formsFromFile(file: File, messageChoice: string, customMessage?: string) {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('message_choice', messageChoice);
    if (customMessage) formData.append('custom_message', customMessage);
    const res = await fetch(`${API_BASE}/api/phase3/forms_from_file`, {
      method: 'POST',
      body: formData,
    });
    if (!res.ok) throw new Error(`Forms submission failed: ${res.status}`);
    return res.json();
  },

  generatePdf(documentInput: any) {
    return request<{ job_id: string }>('/api/phase4/generate_pdf', {
      method: 'POST',
      body: JSON.stringify(documentInput),
    });
  },

  getQueueMetrics() {
    return request<any>('/api/phase5/metrics');
  },

  getEmailMetrics() {
    return request<any>('/api/phase2/metrics');
  },

  getFormMetrics() {
    return request<any>('/api/phase3/metrics');
  },

  async getCampaigns(limit: number = 10) {
    return request<any[]>(`/api/campaigns?limit=${limit}`);
  },

  async getCampaignDetails(campaignId: number) {
    return request<any>(`/api/campaigns/${campaignId}`);
  },

  async createCampaign(name: string, notes?: string) {
    return request<any>('/api/campaigns', {
      method: 'POST',
      body: JSON.stringify({ name, notes }),
    });
  },

  async getErrors(phase?: string, limit: number = 20) {
    const params = new URLSearchParams();
    if (phase) params.append('phase', phase);
    params.append('limit', String(limit));
    return request<any[]>(`/api/errors?${params.toString()}`);
  },

  async exportErrors(phase?: string) {
    const params = new URLSearchParams();
    if (phase) params.append('phase', phase);
    return fetch(`${API_BASE}/api/errors/export?${params.toString()}`, {
      method: 'POST',
      body: JSON.stringify({}),
    }).then(res => res.blob());
  },

  getQueueMetrics() {
    return request<any>('/api/metrics/queues');
  },

  getEmailMetrics() {
    return request<any>('/api/metrics/email');
  },

  getFormMetrics() {
    return request<any>('/api/metrics/forms');
  },

  // Export functions
  async exportCrawlResults() {
    const res = await fetch(`${API_BASE}/api/phase1/export`, { method: 'GET' });
    if (!res.ok) throw new Error('Export failed');
    return res.blob();
  },

  async exportCampaignResults() {
    const res = await fetch(`${API_BASE}/api/campaigns/export`, { method: 'GET' });
    if (!res.ok) throw new Error('Export failed');
    return res.blob();
  },

  async exportResults(phase?: string) {
    const url = phase ? `/api/results/export?phase=${phase}` : '/api/results/export';
    const res = await fetch(`${API_BASE}${url}`, { method: 'GET' });
    if (!res.ok) throw new Error('Export failed');
    return res.blob();
  },
};

