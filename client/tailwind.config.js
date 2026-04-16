/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        brand: {
          50:  '#eef3fb',
          100: '#d4e1f5',
          200: '#a9c3eb',
          300: '#7ea5e1',
          400: '#5387d7',
          500: '#2869cd',
          600: '#1f54a4',
          700: '#173f7b',
          800: '#0f2a52',
          900: '#081529',
        },
        gold: {
          400: '#d4a82d',
          500: '#c9a227',
          600: '#a6851f',
        },
      },
    },
  },
  plugins: [require('@tailwindcss/typography')],
}
