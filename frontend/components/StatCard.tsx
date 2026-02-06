import { useEffect, useState } from 'react';

interface StatCardProps {
  title: string;
  value: number;
  label?: string;
  icon?: string;
  trend?: number;
  color?: 'primary' | 'success' | 'warn' | 'error' | 'info';
  delay?: number;
}

export default function StatCard({
  title,
  value,
  label,
  icon,
  trend,
  color = 'primary',
  delay = 0,
}: StatCardProps) {
  const [displayValue, setDisplayValue] = useState(0);
  const [isVisible, setIsVisible] = useState(false);
  
  useEffect(() => {
    // Delay animation
    const delayTimer = setTimeout(() => {
      setIsVisible(true);
      
      // Animate counter
      let start = 0;
      const end = value;
      const duration = 1000;
      const increment = end / (duration / 16);
      
      const timer = setInterval(() => {
        start += increment;
        if (start >= end) {
          setDisplayValue(end);
          clearInterval(timer);
        } else {
          setDisplayValue(Math.floor(start));
        }
      }, 16);
      
      return () => clearInterval(timer);
    }, delay);
    
    return () => clearTimeout(delayTimer);
  }, [value, delay]);
  
  const colorMap = {
    primary: 'var(--color-primary)',
    success: 'var(--color-success)',
    warn: 'var(--color-warn)',
    error: 'var(--color-error)',
    info: 'var(--color-info)',
  };
  
  const iconColor = colorMap[color];
  const trendColor = trend !== undefined ? (trend > 0 ? 'var(--color-success)' : 'var(--color-error)') : undefined;
  const trendIcon = trend !== undefined ? (trend > 0 ? '↗' : '↘') : null;
  
  return (
    <div
      className="metric-card"
      style={{
        opacity: isVisible ? 1 : 0,
        transform: isVisible ? 'translateY(0)' : 'translateY(20px)',
        transition: 'all 0.5s cubic-bezier(0.4, 0, 0.2, 1)',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '16px' }}>
        {icon && (
          <div
            style={{
              fontSize: '32px',
              filter: `drop-shadow(0 0 8px ${iconColor}40)`,
              animation: isVisible ? 'bounce 0.6s ease-in-out' : 'none',
            }}
          >
            {icon}
          </div>
        )}
        <h3 className="metric-card-title" style={{ margin: 0 }}>{title}</h3>
      </div>
      
      <p
        className="metric-card-value count-up"
        style={{
          background: `linear-gradient(135deg, ${iconColor}, var(--color-text))`,
          WebkitBackgroundClip: 'text',
          WebkitTextFillColor: 'transparent',
          backgroundClip: 'text',
        }}
      >
        {displayValue.toLocaleString()}
      </p>
      
      {label && (
        <p className="metric-card-label">{label}</p>
      )}
      
      {trend !== undefined && (
        <div
          style={{
            marginTop: '12px',
            padding: '6px 12px',
            borderRadius: '8px',
            background: trend > 0 ? 'rgba(34, 197, 94, 0.1)' : 'rgba(244, 63, 94, 0.1)',
            border: `1px solid ${trend > 0 ? 'rgba(34, 197, 94, 0.3)' : 'rgba(244, 63, 94, 0.3)'}`,
            display: 'inline-flex',
            alignItems: 'center',
            gap: '6px',
          }}
        >
          <span style={{ fontSize: '14px', color: trendColor }}>{trendIcon}</span>
          <span style={{ fontSize: '12px', fontWeight: 600, color: trendColor }}>
            {Math.abs(trend)}%
          </span>
          <span style={{ fontSize: '11px', color: 'var(--color-text-secondary)' }}>
            vs last period
          </span>
        </div>
      )}
    </div>
  );
}