'use client';

import React from 'react';
import { AlertCircle, CheckCircle, AlertTriangle, FileText } from 'lucide-react';
import clsx from 'clsx';

interface AlertProps {
  variant: 'success' | 'error' | 'warning' | 'info';
  title: string;
  message?: string;
  onClose?: () => void;
}

export function Alert({ variant, title, message, onClose }: AlertProps) {
  const styles = {
    success: {
      container: 'bg-success-50 border-l-4 border-success-500',
      icon: 'text-success-500',
      title: 'text-success-800',
      message: 'text-success-700',
    },
    error: {
      container: 'bg-error-50 border-l-4 border-error-500',
      icon: 'text-error-500',
      title: 'text-error-800',
      message: 'text-error-700',
    },
    warning: {
      container: 'bg-warning-50 border-l-4 border-warning-500',
      icon: 'text-warning-500',
      title: 'text-warning-800',
      message: 'text-warning-700',
    },
    info: {
      container: 'bg-primary-50 border-l-4 border-primary-500',
      icon: 'text-primary-500',
      title: 'text-primary-800',
      message: 'text-primary-700',
    },
  };

  const style = styles[variant];
  const Icon =
    variant === 'success'
      ? CheckCircle
      : variant === 'error'
        ? AlertCircle
        : variant === 'warning'
          ? AlertTriangle
          : AlertCircle;

  return (
    <div className={clsx('p-4 rounded-lg', style.container)}>
      <div className="flex items-start gap-3">
        <Icon className={clsx('w-5 h-5 flex-shrink-0 mt-0.5', style.icon)} />
        <div className="flex-1 min-w-0">
          <h3 className={clsx('font-semibold text-sm', style.title)}>{title}</h3>
          {message && <p className={clsx('text-sm mt-1', style.message)}>{message}</p>}
        </div>
        {onClose && (
          <button
            onClick={onClose}
            className={clsx('text-sm font-medium hover:underline flex-shrink-0', style.title)}
          >
            Dismiss
          </button>
        )}
      </div>
    </div>
  );
}

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: 'primary' | 'secondary' | 'danger';
  size?: 'sm' | 'md' | 'lg';
  loading?: boolean;
}

export function Button({
  variant = 'primary',
  size = 'md',
  loading = false,
  children,
  disabled,
  className,
  ...props
}: ButtonProps) {
  const baseStyles = 'font-semibold rounded-lg transition-colors focus:outline-none focus:ring-2 focus:ring-offset-2';

  const variantStyles = {
    primary: 'bg-primary-600 text-white hover:bg-primary-700 focus:ring-primary-500 disabled:bg-gray-400',
    secondary: 'bg-gray-200 text-gray-800 hover:bg-gray-300 focus:ring-gray-500 disabled:bg-gray-200',
    danger: 'bg-error-600 text-white hover:bg-error-700 focus:ring-error-500 disabled:bg-gray-400',
  };

  const sizeStyles = {
    sm: 'px-3 py-2 text-sm',
    md: 'px-4 py-2 text-base',
    lg: 'px-6 py-3 text-lg',
  };

  return (
    <button
      disabled={disabled || loading}
      className={clsx(baseStyles, variantStyles[variant], sizeStyles[size], className, {
        'opacity-70 cursor-not-allowed': disabled || loading,
      })}
      {...props}
    >
      {loading ? (
        <span className="flex items-center gap-2">
          <svg className="animate-spin h-4 w-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
            <path
              className="opacity-75"
              fill="currentColor"
              d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
            ></path>
          </svg>
          Loading...
        </span>
      ) : (
        children
      )}
    </button>
  );
}

interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  label?: string;
  error?: string;
}

export function Input({ label, error, ...props }: InputProps) {
  return (
    <div>
      {label && <label className="block text-sm font-medium text-gray-700 mb-1">{label}</label>}
      <input
        className={clsx(
          'w-full px-3 py-2 border rounded-lg focus:outline-none focus:ring-2',
          error ? 'border-error-500 focus:ring-error-500' : 'border-gray-300 focus:ring-primary-500'
        )}
        {...props}
      />
      {error && <p className="text-error-600 text-sm mt-1">{error}</p>}
    </div>
  );
}

interface SelectProps extends React.SelectHTMLAttributes<HTMLSelectElement> {
  label?: string;
  error?: string;
  options: Array<{ value: string; label: string }>;
}

export function Select({ label, error, options, ...props }: SelectProps) {
  return (
    <div>
      {label && <label className="block text-sm font-medium text-gray-700 mb-1">{label}</label>}
      <select
        className={clsx(
          'w-full px-3 py-2 border rounded-lg focus:outline-none focus:ring-2',
          error ? 'border-error-500 focus:ring-error-500' : 'border-gray-300 focus:ring-primary-500'
        )}
        {...props}
      >
        <option value="">-- Select --</option>
        {options.map((opt) => (
          <option key={opt.value} value={opt.value}>
            {opt.label}
          </option>
        ))}
      </select>
      {error && <p className="text-error-600 text-sm mt-1">{error}</p>}
    </div>
  );
}

interface CardProps {
  title?: string;
  children: React.ReactNode;
  className?: string;
}

export function Card({ title, children, className }: CardProps) {
  return (
    <div className={clsx('bg-white rounded-lg shadow-md border border-gray-200 p-6', className)}>
      {title && <h2 className="text-xl font-bold text-gray-900 mb-4">{title}</h2>}
      {children}
    </div>
  );
}

interface LoadingSpinnerProps {
  message?: string;
  progress?: number;
}

export function LoadingSpinner({ message = 'Loading...', progress }: LoadingSpinnerProps) {
  return (
    <div className="flex flex-col items-center justify-center py-12">
      <svg className="animate-spin h-12 w-12 text-primary-600 mb-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
        <path
          className="opacity-75"
          fill="currentColor"
          d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
        ></path>
      </svg>
      <p className="text-gray-600 font-medium">{message}</p>
      {progress !== undefined && (
        <div className="w-32 h-2 bg-gray-200 rounded-full mt-2 overflow-hidden">
          <div className="h-full bg-primary-600 transition-all" style={{ width: `${progress}%` }} />
        </div>
      )}
    </div>
  );
}

interface EmptyStateProps {
  icon?: React.ReactNode;
  title: string;
  message?: string;
  action?: React.ReactNode;
}

export function EmptyState({ icon = <FileText className="w-16 h-16 text-gray-300" />, title, message, action }: EmptyStateProps) {
  return (
    <div className="flex flex-col items-center justify-center py-12 text-center">
      {icon}
      <h3 className="mt-4 text-lg font-semibold text-gray-900">{title}</h3>
      {message && <p className="mt-2 text-gray-600 text-sm">{message}</p>}
      {action && <div className="mt-4">{action}</div>}
    </div>
  );
}
