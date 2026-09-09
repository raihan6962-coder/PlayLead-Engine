import type { Config } from 'tailwindcss';

const config: Config = {
  content: [
    './src/pages/**/*.{js,ts,jsx,tsx,mdx}',
    './src/components/**/*.{js,ts,jsx,tsx,mdx}',
    './src/app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        bg: { DEFAULT: '#05060a', 2: '#0a0c14' },
        card: { DEFAULT: '#0e1018', 2: '#12141e' },
        surface: '#161824',
        border: { DEFAULT: '#1c1e2e', 2: '#252840' },
        accent: { DEFAULT: '#6c5ce7', light: '#a29bfe', lighter: '#ddd6fe' },
        cyan: { DEFAULT: '#22d3ee', light: '#67e8f9' },
        brand: {
          green: '#34d399',
          red: '#f87171',
          amber: '#fbbf24',
          blue: '#60a5fa',
          pink: '#f472b6',
        },
        txt: { DEFAULT: '#e8eaf0', 2: '#9ca3b8', 3: '#5b6078' },
      },
      fontFamily: {
        sans: ['Inter', '-apple-system', 'BlinkMacSystemFont', 'sans-serif'],
        mono: ['JetBrains Mono', 'monospace'],
      },
      borderRadius: {
        xl: '14px',
        '2xl': '20px',
      },
      animation: {
        'fade-up': 'fadeUp 0.3s ease',
        'pulse-dot': 'pulseDot 2s ease-in-out infinite',
      },
      keyframes: {
        fadeUp: {
          '0%': { opacity: '0', transform: 'translateY(8px)' },
          '100%': { opacity: '1', transform: 'translateY(0)' },
        },
        pulseDot: {
          '0%, 100%': { opacity: '1', transform: 'scale(1)' },
          '50%': { opacity: '0.5', transform: 'scale(0.85)' },
        },
      },
    },
  },
  plugins: [],
};

export default config;
