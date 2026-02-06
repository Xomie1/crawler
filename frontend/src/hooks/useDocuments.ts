import { useState, useCallback, useEffect, useRef } from 'react';
import { documentAPI, GenerateRequest, DocumentCompletedResponse, DocumentProgressResponse, DocumentFailedResponse } from '@/lib/api';

export interface DocumentStatus {
  status: 'idle' | 'loading' | 'success' | 'error' | 'polling';
  jobId?: string;
  data?: DocumentCompletedResponse | DocumentProgressResponse | DocumentFailedResponse;
  error?: string;
  progress?: number;
}

export function useDocumentGeneration() {
  const [state, setState] = useState<DocumentStatus>({ status: 'idle' });

  const generateDocument = useCallback(async (data: GenerateRequest) => {
    setState({ status: 'loading' });
    try {
      const result = await documentAPI.generateDocument(data);
      setState({
        status: 'polling',
        jobId: result.job_id,
      });
      return result.job_id;
    } catch (error) {
      setState({
        status: 'error',
        error: error instanceof Error ? error.message : 'Failed to generate document',
      });
      throw error;
    }
  }, []);

  return { status: state, generateDocument };
}

export function useDocumentStatus(jobId?: string, pollInterval: number = 2000) {
  const [state, setState] = useState<DocumentStatus>({ status: 'idle' });
  const pollTimeoutRef = useRef<NodeJS.Timeout>();

  const pollStatus = useCallback(async () => {
    if (!jobId) return;

    try {
      const response = await documentAPI.getStatus(jobId);

      setState({
        status: 'polling',
        jobId,
        data: response,
        progress: response.status === 'processing' ? response.progress?.percentage : undefined,
      });

      if (response.status === 'completed') {
        setState({
          status: 'success',
          jobId,
          data: response,
        });
      } else if (response.status === 'failed') {
        setState({
          status: 'error',
          jobId,
          data: response,
          error: response.error.message,
        });
      } else {
        // Continue polling
        pollTimeoutRef.current = setTimeout(pollStatus, pollInterval);
      }
    } catch (error) {
      setState({
        status: 'error',
        jobId,
        error: error instanceof Error ? error.message : 'Failed to fetch status',
      });
    }
  }, [jobId, pollInterval]);

  useEffect(() => {
    if (jobId) {
      // Initial poll
      pollStatus();
    }

    return () => {
      if (pollTimeoutRef.current) {
        clearTimeout(pollTimeoutRef.current);
      }
    };
  }, [jobId, pollStatus]);

  return state;
}

export function useDocumentList(userId: string, enabled: boolean = true) {
  const [documents, setDocuments] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>();

  const fetchDocuments = useCallback(async () => {
    if (!enabled || !userId) return;

    setLoading(true);
    try {
      const response = await documentAPI.listDocuments(userId);
      setDocuments(response.documents);
      setError(undefined);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch documents');
    } finally {
      setLoading(false);
    }
  }, [userId, enabled]);

  useEffect(() => {
    fetchDocuments();
  }, [fetchDocuments]);

  return { documents, loading, error, refetch: fetchDocuments };
}

export function useSendEmail() {
  const [state, setState] = useState<{ status: 'idle' | 'loading' | 'success' | 'error'; error?: string }>({
    status: 'idle',
  });

  const sendEmail = useCallback(async (jobId: string, email: string, includePdf: boolean = true, includeDocx: boolean = true) => {
    setState({ status: 'loading' });
    try {
      const result = await documentAPI.sendEmail(jobId, {
        recipient_email: email,
        include_pdf: includePdf,
        include_docx: includeDocx,
      });
      setState({ status: 'success' });
      return result;
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : 'Failed to send email';
      setState({ status: 'error', error: errorMsg });
      throw error;
    }
  }, []);

  return { ...state, sendEmail };
}
