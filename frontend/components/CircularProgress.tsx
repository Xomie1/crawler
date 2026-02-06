import { useEffect, useState } from 'react';

interface CircularProgressProps {
  percentage: number;
  size?: number;
  strokeWidth?: number;
  color?: 'primary' | 'success' | 'warn' | 'error' | 'info' | 'pending';
  label?: string;
  showPercentage?: boolean;
  animated?: boolean;
}

export default function CircularProgress({
  percentage,
  size = 120,
  strokeWidth = 8,
  color = 'primary',
  label,
  showPercentage = true,
  animated = true,
}: CircularProgressProps) {
  const [displayPercentage, setDisplayPercentage] = useState(0);
  
  useEffect(() => {
    if (animated) {
      let start = 0;
      const end = percentage;
      const duration = 1000;
      const increment = end / (duration / 16);
      
      const timer = setInterval(() => {
        start += increment;
        if (start >= end) {
          setDisplayPercentage(end);
          clearInterval(timer);
        } else {
          setDisplayPercentage(Math.floor(start));
        }
      }, 16);
      
      return () => clearInterval(timer);
    } else {
      setDisplayPercentage(percentage);
    }
  }, [percentage, animated]);
  
  const radius = (size - strokeWidth) / 2;
  const circumference = 2 * Math.PI * radius;
  const offset = circumference - (displayPercentage / 100) * circumference;
  
  const colorMap = {
    primary: '#6366f1',
    success: '#22c55e',
    warn: '#f59e0b',
    error: '#f43f5e',
    info: '#06b6d4',
    pending: '#a855f7',
  };
  
  const glowMap = {
    primary: 'rgba(99, 102, 241, 0.4)',
    success: 'rgba(34, 197, 94, 0.3)',
    warn: 'rgba(245, 158, 11, 0.3)',
    error: 'rgba(244, 63, 94, 0.3)',
    info: 'rgba(6, 182, 212, 0.3)',
    pending: 'rgba(168, 85, 247, 0.3)',
  };
  
  const strokeColor = colorMap[color];
  const glowColor = glowMap[color];
  
  return (
    <div
      style={{
        display: 'inline-flex',
        flexDirection: 'column',
        alignItems: 'center',
        gap: '12px',
      }}
    >
      <div style={{ position: 'relative', width: size, height: size }}>
        <svg
          width={size}
          height={size}
          style={{
            transform: 'rotate(-90deg)',
            filter: `drop-shadow(0 0 8px ${glowColor})`,
          }}
        >
          {/* Background Circle */}
          <circle
            cx={size / 2}
            cy={size / 2}
            r={radius}
            fill="none"
            stroke="var(--color-surface-alt)"
            strokeWidth={strokeWidth}
            opacity={0.3}
          />
          
          {/* Progress Circle */}
          <circle
            cx={size / 2}
            cy={size / 2}
            r={radius}
            fill="none"
            stroke={strokeColor}
            strokeWidth={strokeWidth}
            strokeDasharray={circumference}
            strokeDashoffset={offset}
            strokeLinecap="round"
            style={{
              transition: animated ? 'stroke-dashoffset 0.5s ease' : 'none',
            }}
          />
        </svg>
        
        {/* Center Text */}
        {showPercentage && (
          <div
            style={{
              position: 'absolute',
              top: '50%',
              left: '50%',
              transform: 'translate(-50%, -50%)',
              textAlign: 'center',
            }}
          >
            <div
              style={{
                fontSize: size * 0.25,
                fontWeight: 700,
                color: strokeColor,
                lineHeight: 1,
              }}
            >
              {displayPercentage}%
            </div>
          </div>
        )}
      </div>
      
      {label && (
        <div
          style={{
            fontSize: '13px',
            fontWeight: 600,
            color: 'var(--color-text-secondary)',
            textAlign: 'center',
          }}
        >
          {label}
        </div>
      )}
    </div>
  );
}