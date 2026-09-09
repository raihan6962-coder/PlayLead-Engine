import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'PlayLead Engine',
  description: 'Google Play Store lead generation and email outreach automation',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className="dark">
      <body className="min-h-screen bg-bg text-txt font-sans antialiased">
        {children}
      </body>
    </html>
  );
}
