import axios, { AxiosInstance } from 'axios';

export interface Party {
  name: string;
  address: string;
  role: 'party_a' | 'party_b';
}

export interface DocumentOptions {
  property_separation?: boolean;
  alimony?: boolean;
  children?: boolean;
  custom_date?: string;
}

export interface GenerateRequest {
  document_type: 'prenuptial' | 'divorce';
  parties: Party[];
  options?: DocumentOptions;
  custom_values?: Record<string, string>;
  metadata?: Record<string, any>;
}

export interface DocumentResult {
  pdf_path: string;
  pdf_url: string;
  docx_path: string;
  docx_url: string;
  generation_time_seconds: number;
  file_sizes: {
    pdf_bytes: number;
    docx_bytes: number;
  };
}

export interface DocumentCompletedResponse {
  status: 'completed';
  job_id: string;
  document_type: string;
  result: DocumentResult;
  created_at: string;
  completed_at: string;
}

export interface DocumentProgressResponse {
  status: 'processing' | 'queued';
  job_id: string;
  progress?: {
    stage: string;
    percentage: number;
  };
  queue_position?: number;
  estimated_time_remaining_seconds?: number;
}

export interface DocumentFailedResponse {
  status: 'failed';
  job_id: string;
  error: {
    type: string;
    message: string;
  };
  failed_at: string;
}

export interface DocumentListItem {
  job_id: string;
  document_type: string;
  status: string;
  party_names: string[];
  created_at: string;
  completed_at?: string;
  file_urls?: {
    pdf: string;
    docx: string;
  };
}

// Alias for compatibility
export type DocumentInfo = DocumentListItem;

export interface DocumentListResponse {
  status: string;
  total_count: number;
  limit: number;
  offset: number;
  documents: DocumentListItem[];
}

export interface SendEmailRequest {
  recipient_email: string;
  include_pdf?: boolean;
  include_docx?: boolean;
  message?: string;
  sender_name?: string;
}

class DocumentAPI {
  private client: AxiosInstance;
  private baseURL: string;

  constructor(baseURL: string = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:8000') {
    this.baseURL = baseURL;
    this.client = axios.create({
      baseURL: `${baseURL}/api/documents`,
      headers: {
        'Content-Type': 'application/json',
      },
    });
  }

  async generateDocument(data: GenerateRequest): Promise<{ job_id: string; status: string }> {
    try {
      const response = await this.client.post('/generate', data);
      return response.data;
    } catch (error) {
      throw this.handleError(error);
    }
  }

  async getStatus(
    jobId: string
  ): Promise<DocumentCompletedResponse | DocumentProgressResponse | DocumentFailedResponse> {
    try {
      const response = await this.client.get(`/status/${jobId}`);
      return response.data;
    } catch (error) {
      throw this.handleError(error);
    }
  }

  async listDocuments(
    userId: string,
    filters?: {
      document_type?: string;
      status?: string;
      limit?: number;
      offset?: number;
    }
  ): Promise<DocumentListResponse> {
    try {
      const params = {
        user_id: userId,
        ...filters,
      };
      const response = await this.client.get('/list', { params });
      return response.data;
    } catch (error) {
      throw this.handleError(error);
    }
  }

  async sendEmail(jobId: string, data: SendEmailRequest): Promise<{ email_job_id: string }> {
    try {
      const response = await this.client.post(`/${jobId}/send-email`, data);
      return response.data;
    } catch (error) {
      throw this.handleError(error);
    }
  }

  getDownloadUrl(filename: string): string {
    return `${this.baseURL}/api/documents/download/${filename}`;
  }

  async healthCheck(): Promise<boolean> {
    try {
      const response = await this.client.get('/health');
      return response.data.status === 'healthy';
    } catch {
      return false;
    }
  }

  private handleError(error: any): Error {
    if (axios.isAxiosError(error)) {
      const message = error.response?.data?.message || error.message;
      const errorType = error.response?.data?.error_type || 'unknown_error';
      return new Error(`[${errorType}] ${message}`);
    }
    return error;
  }
}

export const documentAPI = new DocumentAPI();
