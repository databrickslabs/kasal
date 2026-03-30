import i18n from '../config/i18n/config';

export class LanguageService {
  private static instance: LanguageService;
  private readonly LANGUAGE_KEY = 'APP_LANGUAGE';

  // Private constructor to prevent direct instantiation
  private constructor() {
    // Singleton instance initialization
  }

  public static getInstance(): LanguageService {
    if (!LanguageService.instance) {
      LanguageService.instance = new LanguageService();
    }
    return LanguageService.instance;
  }

  public async getCurrentLanguage(): Promise<string> {
    const fullLang = i18n.language || 'en';
    
    // If the language contains a region code (e.g., 'en-GB'), use only the language part
    const baseLang = fullLang.split('-')[0];
    
    // Return the base language if it's in our supported languages
    return baseLang || 'en';
  }

  public async setLanguage(language: string): Promise<void> {
    // Extract the base language code (e.g., 'en' from 'en-GB')
    const baseLang = language.split('-')[0];
    
    await i18n.changeLanguage(baseLang);
    localStorage.setItem(this.LANGUAGE_KEY, baseLang);
  }

  public async initializeLanguage(): Promise<void> {
    const savedLanguage = localStorage.getItem(this.LANGUAGE_KEY);
    if (savedLanguage) {
      await this.setLanguage(savedLanguage);
    }
  }
} 