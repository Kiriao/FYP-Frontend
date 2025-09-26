// components/ResponsiveWrapper.tsx
'use client';

import { ReactNode, useEffect, useState } from 'react';

interface ResponsiveWrapperProps {
  children: ReactNode;
  className?: string;
}

export default function ResponsiveWrapper({ children, className = '' }: ResponsiveWrapperProps) {
  const [isMobile, setIsMobile] = useState(false);

  useEffect(() => {
    const checkMobile = () => {
      setIsMobile(window.innerWidth < 768);
    };

    checkMobile();
    window.addEventListener('resize', checkMobile);
    return () => window.removeEventListener('resize', checkMobile);
  }, []);

  return (
    <div className={`responsive-wrapper ${isMobile ? 'mobile' : 'desktop'} ${className}`}>
      <style jsx>{`
        .responsive-wrapper {
          width: 100%;
        }

        /* Mobile styles */
        .mobile {
          padding: 0.75rem;
        }

        .mobile h1 { font-size: 1.5rem !important; }
        .mobile h2 { font-size: 1.25rem !important; }
        .mobile h3 { font-size: 1.125rem !important; }

        .mobile .grid {
          grid-template-columns: repeat(2, 1fr) !important;
          gap: 0.5rem !important;
        }

        .mobile button {
          min-height: 44px !important;
          font-size: 0.875rem !important;
          padding: 0.5rem 0.75rem !important;
        }

        .mobile input, 
        .mobile textarea {
          font-size: 16px !important;
          padding: 0.75rem !important;
        }

        .mobile .modal {
          padding: 0.5rem !important;
          align-items: flex-start !important;
        }

        .mobile .flex {
          flex-direction: column !important;
          gap: 0.5rem !important;
        }

        .mobile .flex.keep-row {
          flex-direction: row !important;
        }



        .desktop .grid {
          gap: 1rem;
        }
      `}</style>
      {children}
    </div>
  );
}