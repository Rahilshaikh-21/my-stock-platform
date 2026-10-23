/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        slate: {
          950: '#030712',
          900: '#0b1120',
          850: '#111827',
          800: '#1f2937',
        }
      }
    },
  },
  plugins: [],
}
