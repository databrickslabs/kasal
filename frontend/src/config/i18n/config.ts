import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';
import LanguageDetector from 'i18next-browser-languagedetector';

import enTranslations from './locales/en.json';
import koTranslations from './locales/ko.json';
import frTranslations from './locales/fr.json';
import deTranslations from './locales/de.json';
import esTranslations from './locales/es.json';
import ptTranslations from './locales/pt.json';
import arTranslations from './locales/ar.json';
import hiTranslations from './locales/hi.json';
import jaTranslations from './locales/ja.json';
import zhTranslations from './locales/zh.json';
import trTranslations from './locales/tr.json';

export const LANGUAGES = {
  en: { nativeName: 'English' },
  ko: { nativeName: '한국어' },
  fr: { nativeName: 'Français' },
  de: { nativeName: 'Deutsch' },
  es: { nativeName: 'Español' },
  pt: { nativeName: 'Português' },
  ar: { nativeName: 'العربية' },
  hi: { nativeName: 'हिन्दी' },
  ja: { nativeName: '日本語' },
  zh: { nativeName: '中文' },
  tr: { nativeName: 'Türkçe' }
};

i18n
  .use(LanguageDetector)
  .use(initReactI18next)
  .init({
    resources: {
      en: { translation: enTranslations },
      ko: { translation: koTranslations },
      fr: { translation: frTranslations },
      de: { translation: deTranslations },
      es: { translation: esTranslations },
      pt: { translation: ptTranslations },
      ar: { translation: arTranslations },
      hi: { translation: hiTranslations },
      ja: { translation: jaTranslations },
      zh: { translation: zhTranslations },
      tr: { translation: trTranslations }
    },
    fallbackLng: 'en',
    interpolation: {
      escapeValue: false
    }
  });

export default i18n; 