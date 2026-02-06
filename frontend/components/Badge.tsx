interface BadgeProps {
  status: 'success' | 'error' | 'pending' | 'warn';
  text: string;
  icon?: string;
  className?: string;
}

export default function Badge({ status, text, icon, className = '' }: BadgeProps) {
  const statusClass = `badge badge-${status}`;
  
  return (
    <span className={`${statusClass} ${className}`}>
      {icon && <span>{icon}</span>}
      {text}
    </span>
  );
}
